package dag

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"math/rand" // ...new import for jitter...

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

// TaskError is used by node processors to indicate whether an error is recoverable.
type TaskError struct {
	Err         error
	Recoverable bool
}

func (te TaskError) Error() string {
	return te.Err.Error()
}

// TaskState holds state and intermediate results for a given task (identified by a node ID).
type TaskState struct {
	UpdatedAt     time.Time
	targetResults storage.IMap[string, mq.Result]
	NodeID        string
	Status        mq.Status
	Result        mq.Result
}

func newTaskState(nodeID string) *TaskState {
	return &TaskState{
		NodeID:        nodeID,
		Status:        mq.Pending,
		UpdatedAt:     time.Now(),
		targetResults: memory.New[string, mq.Result](),
	}
}

type nodeResult struct {
	ctx    context.Context
	nodeID string
	status mq.Status
	result mq.Result
}

type task struct {
	ctx     context.Context
	taskID  string
	nodeID  string
	payload json.RawMessage
}

func newTask(ctx context.Context, taskID, nodeID string, payload json.RawMessage) *task {
	return &task{
		ctx:     ctx,
		taskID:  taskID,
		nodeID:  nodeID,
		payload: payload,
	}
}

type TaskManagerConfig struct {
	MaxRetries      int
	BaseBackoff     time.Duration
	RecoveryHandler func(ctx context.Context, result mq.Result) error
}

type TaskManager struct {
	createdAt          time.Time
	taskStates         storage.IMap[string, *TaskState]
	parentNodes        storage.IMap[string, string]
	childNodes         storage.IMap[string, int]
	deferredTasks      storage.IMap[string, *task]
	iteratorNodes      storage.IMap[string, []Edge]
	currentNodePayload storage.IMap[string, json.RawMessage]
	currentNodeResult  storage.IMap[string, mq.Result]
	taskQueue          chan *task
	result             *mq.Result
	resultQueue        chan nodeResult
	resultCh           chan mq.Result
	stopCh             chan struct{}
	taskID             string
	dag                *DAG
	maxRetries         int
	baseBackoff        time.Duration
	recoveryHandler    func(ctx context.Context, result mq.Result) error
	pauseMu            sync.Mutex
	pauseCh            chan struct{}
	wg                 sync.WaitGroup
}

func NewTaskManager(dag *DAG, taskID string, resultCh chan mq.Result, iteratorNodes storage.IMap[string, []Edge]) *TaskManager {
	config := TaskManagerConfig{
		MaxRetries:  3,
		BaseBackoff: time.Second,
	}
	tm := &TaskManager{
		createdAt:          time.Now(),
		taskStates:         memory.New[string, *TaskState](),
		parentNodes:        memory.New[string, string](),
		childNodes:         memory.New[string, int](),
		deferredTasks:      memory.New[string, *task](),
		currentNodePayload: memory.New[string, json.RawMessage](),
		currentNodeResult:  memory.New[string, mq.Result](),
		taskQueue:          make(chan *task, DefaultChannelSize),
		resultQueue:        make(chan nodeResult, DefaultChannelSize),
		resultCh:           resultCh,
		stopCh:             make(chan struct{}),
		taskID:             taskID,
		dag:                dag,
		maxRetries:         config.MaxRetries,
		baseBackoff:        config.BaseBackoff,
		recoveryHandler:    config.RecoveryHandler,
		iteratorNodes:      iteratorNodes,
	}

	tm.wg.Add(3)
	go tm.run()
	go tm.waitForResult()
	go tm.retryDeferredTasks()

	return tm
}

func (tm *TaskManager) ProcessTask(ctx context.Context, startNode string, payload json.RawMessage) {
	tm.enqueueTask(ctx, startNode, tm.taskID, payload)
}

func (tm *TaskManager) enqueueTask(ctx context.Context, startNode, taskID string, payload json.RawMessage) {
	if index, ok := ctx.Value(ContextIndex).(string); ok {
		base := strings.Split(startNode, Delimiter)[0]
		startNode = fmt.Sprintf("%s%s%s", base, Delimiter, index)
	}
	if _, exists := tm.taskStates.Get(startNode); !exists {
		tm.taskStates.Set(startNode, newTaskState(startNode))
	}
	t := newTask(ctx, taskID, startNode, payload)
	select {
	case tm.taskQueue <- t:
	default:
		log.Println("Task queue is full, deferring task")
		tm.deferredTasks.Set(taskID, t)
	}
}

func (tm *TaskManager) run() {
	defer tm.wg.Done()
	for {
		select {
		case <-tm.stopCh:
			log.Println("Stopping TaskManager run loop")
			return
		default:
			tm.pauseMu.Lock()
			pch := tm.pauseCh
			tm.pauseMu.Unlock()
			if pch != nil {
				<-pch
			}
			select {
			case <-tm.stopCh:
				log.Println("Stopping TaskManager run loop")
				return
			case tsk := <-tm.taskQueue:
				tm.processNode(tsk)
			}
		}
	}
}

// waitForResult listens for node results on resultQueue and processes them.
func (tm *TaskManager) waitForResult() {
	defer tm.wg.Done()
	for {
		select {
		case <-tm.stopCh:
			log.Println("Stopping TaskManager result listener")
			return
		case nr := <-tm.resultQueue:
			tm.onNodeCompleted(nr)
		}
	}
}

func (tm *TaskManager) processNode(exec *task) {
	startTime := time.Now()
	pureNodeID := strings.Split(exec.nodeID, Delimiter)[0]
	node, exists := tm.dag.nodes.Get(pureNodeID)
	if !exists {
		tm.dag.Logger().Error("Node not found", logger.Field{Key: "nodeID", Value: pureNodeID})
		return
	}
	// Wrap context with timeout if node.Timeout is configured.
	if node.Timeout > 0 {
		var cancel context.CancelFunc
		exec.ctx, cancel = context.WithTimeout(exec.ctx, node.Timeout)
		defer cancel()
	}
	// Invoke PreProcessHook if available.
	if tm.dag.PreProcessHook != nil {
		exec.ctx = tm.dag.PreProcessHook(exec.ctx, node, exec.taskID, exec.payload)
	}
	state, _ := tm.taskStates.Get(exec.nodeID)
	if state == nil {
		tm.dag.Logger().Warn("State not found; creating new state", logger.Field{Key: "nodeID", Value: exec.nodeID})
		state = newTaskState(exec.nodeID)
		tm.taskStates.Set(exec.nodeID, state)
	}
	state.Status = mq.Processing
	state.UpdatedAt = time.Now()
	tm.currentNodePayload.Clear()
	tm.currentNodeResult.Clear()
	tm.currentNodePayload.Set(exec.nodeID, exec.payload)

	var result mq.Result
	attempts := 0
	for {
		// log.Printf("Tracing: Start processing node %s (attempt %d) on flow %s", exec.nodeID, attempts+1, tm.dag.key)
		result = node.processor.ProcessTask(exec.ctx, mq.NewTask(exec.taskID, exec.payload, exec.nodeID, mq.WithDAG(tm.dag)))
		if result.Error != nil {
			if te, ok := result.Error.(TaskError); ok && te.Recoverable {
				if attempts < tm.maxRetries {
					attempts++
					backoff := tm.baseBackoff * time.Duration(1<<attempts)
					// add jitter to avoid thundering herd
					jitter := time.Duration(rand.Int63n(int64(tm.baseBackoff)))
					backoff += jitter
					log.Printf("Recoverable error on node %s, retrying in %s: %v", exec.nodeID, backoff, result.Error)
					select {
					case <-time.After(backoff):
					case <-exec.ctx.Done():
						log.Printf("Context cancelled for node %s", exec.nodeID)
						return
					}
					continue
				} else if tm.recoveryHandler != nil {
					if err := tm.recoveryHandler(exec.ctx, result); err == nil {
						result.Error = nil
						result.Status = mq.Completed
					}
				}
			}
		}
		break
	}
	// log.Printf("Tracing: End processing node %s on flow %s", exec.nodeID, tm.dag.key)
	nodeLatency := time.Since(startTime)

	// Invoke PostProcessHook if available.
	if tm.dag.PostProcessHook != nil {
		tm.dag.PostProcessHook(exec.ctx, node, exec.taskID, result)
	}

	if result.Error != nil {
		result.Status = mq.Failed
		state.Status = mq.Failed
		state.Result.Status = mq.Failed
		state.Result.Latency = nodeLatency.String()
		tm.result = &result
		tm.resultCh <- result
		tm.processFinalResult(state)
		return
	}
	result.Status = mq.Completed
	state.Result = result
	state.Result.Status = mq.Completed
	state.Result.Latency = nodeLatency.String()
	result.Topic = node.ID
	tm.updateTimestamps(&result)

	isLast, err := tm.dag.IsLastNode(pureNodeID)
	if err != nil {
		tm.dag.Logger().Error("Error checking if node is last", logger.Field{Key: "nodeID", Value: pureNodeID}, logger.Field{Key: "error", Value: err.Error()})
	} else if isLast {
		result.Last = true
	}
	tm.currentNodeResult.Set(exec.nodeID, result)
	tm.logNodeExecution(exec, pureNodeID, result, nodeLatency)

	if result.Error != nil {
		tm.result = &result
		tm.resultCh <- result
		tm.processFinalResult(state)
		return
	}
	if result.Last || node.NodeType == Page {
		tm.result = &result
		tm.resultCh <- result
		if result.Last {
			tm.processFinalResult(state)
		}
		return
	}

	tm.handleNext(exec.ctx, node, state, result)
}

// logNodeExecution logs node execution details
func (tm *TaskManager) logNodeExecution(exec *task, pureNodeID string, result mq.Result, latency time.Duration) {
	success := result.Error == nil

	// Log to DAG activity logger if available
	if tm.dag.activityLogger != nil {
		ctx := context.WithValue(exec.ctx, "task_id", exec.taskID)
		ctx = context.WithValue(ctx, "node_id", pureNodeID)
		ctx = context.WithValue(ctx, "duration", latency)
		if result.Error != nil {
			ctx = context.WithValue(ctx, "error", result.Error)
		}

		tm.dag.activityLogger.LogNodeExecution(ctx, exec.taskID, pureNodeID, result, latency)
	}

	// Update monitoring metrics
	if tm.dag.monitor != nil {
		tm.dag.monitor.metrics.RecordNodeExecution(pureNodeID, latency, success)
	}

	// Log to standard logger
	fields := []logger.Field{
		{Key: "nodeID", Value: pureNodeID},
		{Key: "taskID", Value: exec.taskID},
		{Key: "duration", Value: latency.String()},
		{Key: "success", Value: success},
	}

	if result.Error != nil {
		fields = append(fields, logger.Field{Key: "error", Value: result.Error.Error()})
		tm.dag.Logger().Error("Node execution failed", fields...)
	} else {
		tm.dag.Logger().Info("Node execution completed", fields...)
	}
}

func (tm *TaskManager) updateTimestamps(rs *mq.Result) {
	rs.CreatedAt = tm.createdAt
	rs.ProcessedAt = time.Now()
	rs.Latency = time.Since(rs.CreatedAt).String()
}

func (tm *TaskManager) handlePrevious(ctx context.Context, state *TaskState, result mq.Result, childNode string, dispatchFinal bool) {
	state.targetResults.Set(childNode, result)
	state.targetResults.Del(state.NodeID)
	targetsCount, _ := tm.childNodes.Get(state.NodeID)
	size := state.targetResults.Size()
	if size == targetsCount {
		if size > 1 {
			aggregated := make([]json.RawMessage, size)
			i := 0
			state.targetResults.ForEach(func(_ string, res mq.Result) bool {
				aggregated[i] = res.Payload
				i++
				return true
			})
			aggregatedPayload, err := json.Marshal(aggregated)
			if err != nil {
				panic(err)
			}
			state.Result = mq.Result{Payload: aggregatedPayload, Status: mq.Completed, Ctx: ctx, Topic: state.NodeID}
		} else if size == 1 {
			state.Result = state.targetResults.Values()[0]
		}
		state.Status = result.Status
		state.Result.Status = result.Status
	}

	if state.Result.Payload == nil {
		state.Result.Payload = result.Payload
	}
	state.UpdatedAt = time.Now()
	if result.Ctx == nil {
		result.Ctx = ctx
	}
	if result.Error != nil {
		state.Status = mq.Failed
	}
	if parentKey, ok := tm.parentNodes.Get(state.NodeID); ok {
		parts := strings.Split(state.NodeID, Delimiter)
		if edges, exists := tm.iteratorNodes.Get(parts[0]); exists && state.Status == mq.Completed {
			state.Status = mq.Processing
			tm.iteratorNodes.Del(parts[0])
			state.targetResults.Clear()
			if len(parts) == 2 {
				ctx = context.WithValue(ctx, ContextIndex, parts[1])
			}
			toProcess := nodeResult{
				ctx:    ctx,
				nodeID: state.NodeID,
				status: state.Status,
				result: state.Result,
			}
			tm.handleEdges(toProcess, edges)
		} else if size == targetsCount {
			if parentState, _ := tm.taskStates.Get(parentKey); parentState != nil {
				state.Result.Topic = state.NodeID
				tm.handlePrevious(ctx, parentState, state.Result, state.NodeID, dispatchFinal)
			}
		}
	} else {
		tm.updateTimestamps(&state.Result)
		tm.result = &state.Result
		state.Result.Topic = strings.Split(state.NodeID, Delimiter)[0]
		tm.resultCh <- state.Result
		tm.processFinalResult(state)
	}
}

func (tm *TaskManager) handleNext(ctx context.Context, node *Node, state *TaskState, result mq.Result) {
	state.UpdatedAt = time.Now()
	if result.Ctx == nil {
		result.Ctx = ctx
	}
	if result.Error != nil {
		state.Status = mq.Failed
	} else {
		edges := tm.getConditionalEdges(node, result)
		if len(edges) == 0 {
			state.Status = mq.Completed
		}
	}
	if result.Status == "" {
		result.Status = state.Status
	}
	tm.enqueueResult(nodeResult{
		ctx:    ctx,
		nodeID: state.NodeID,
		status: state.Status,
		result: result,
	})
}

func (tm *TaskManager) enqueueResult(nr nodeResult) {
	select {
	case tm.resultQueue <- nr:
	default:
		log.Println("Result queue is full, dropping result")
	}
}

func (tm *TaskManager) onNodeCompleted(nr nodeResult) {
	nodeID := strings.Split(nr.nodeID, Delimiter)[0]
	node, ok := tm.dag.nodes.Get(nodeID)
	if !ok {
		return
	}
	edges := tm.getConditionalEdges(node, nr.result)
	if nr.result.Error != nil || len(edges) == 0 {

		if index, ok := nr.ctx.Value(ContextIndex).(string); ok {
			childNode := fmt.Sprintf("%s%s%s", node.ID, Delimiter, index)
			if parentKey, exists := tm.parentNodes.Get(childNode); exists {
				if parentState, _ := tm.taskStates.Get(parentKey); parentState != nil {
					tm.handlePrevious(nr.ctx, parentState, nr.result, nr.nodeID, true)
					return
				}
			}
		}
		tm.updateTimestamps(&nr.result)
		tm.resultCh <- nr.result
		if state, ok := tm.taskStates.Get(nr.nodeID); ok {
			tm.processFinalResult(state)
		}
		return
	}
	tm.handleEdges(nr, edges)
}

func (tm *TaskManager) getConditionalEdges(node *Node, result mq.Result) []Edge {
	edges := make([]Edge, len(node.Edges))
	copy(edges, node.Edges)
	if result.ConditionStatus != "" {
		if conditions, ok := tm.dag.conditions[result.Topic]; ok {
			if targetKey, exists := conditions[result.ConditionStatus]; exists {
				if targetNode, found := tm.dag.nodes.Get(targetKey); found {
					edges = append(edges, Edge{From: node, To: targetNode})
				}
			} else if targetKey, exists := conditions["default"]; exists {
				if targetNode, found := tm.dag.nodes.Get(targetKey); found {
					edges = append(edges, Edge{From: node, To: targetNode})
				}
			}
		}
	}
	return edges
}

func (tm *TaskManager) handleEdges(currentResult nodeResult, edges []Edge) {
	if len(edges) > 1 {
		var wg sync.WaitGroup
		for _, edge := range edges {
			wg.Add(1)
			go func(edge Edge) {
				defer wg.Done()
				tm.processSingleEdge(currentResult, edge)
			}(edge)
		}
		wg.Wait()
	} else {
		for _, edge := range edges {
			tm.processSingleEdge(currentResult, edge)
		}
	}
}

func (tm *TaskManager) processSingleEdge(currentResult nodeResult, edge Edge) {
	index, ok := currentResult.ctx.Value(ContextIndex).(string)
	if !ok {
		index = "0"
	}
	parentNode := fmt.Sprintf("%s%s%s", edge.From.ID, Delimiter, index)
	switch edge.Type {
	case Simple:
		if _, exists := tm.iteratorNodes.Get(edge.From.ID); exists {
			return
		}
		fallthrough
	case Iterator:
		if edge.Type == Iterator {
			var items []json.RawMessage
			if err := json.Unmarshal(currentResult.result.Payload, &items); err != nil {
				log.Printf("Error unmarshalling payload for node %s: %v", edge.To.ID, err)
				tm.enqueueResult(nodeResult{
					ctx:    currentResult.ctx,
					nodeID: edge.To.ID,
					status: mq.Failed,
					result: mq.Result{Error: err},
				})
				return
			}
			tm.childNodes.Set(parentNode, len(items))
			for i, item := range items {
				childNode := fmt.Sprintf("%s%s%d", edge.To.ID, Delimiter, i)
				ctx := context.WithValue(currentResult.ctx, ContextIndex, fmt.Sprintf("%d", i))
				tm.parentNodes.Set(childNode, parentNode)
				tm.enqueueTask(ctx, edge.To.ID, tm.taskID, item)
			}
		} else {
			tm.childNodes.Set(parentNode, 1)
			idx, _ := currentResult.ctx.Value(ContextIndex).(string)
			childNode := fmt.Sprintf("%s%s%s", edge.To.ID, Delimiter, idx)
			ctx := context.WithValue(currentResult.ctx, ContextIndex, idx)
			tm.parentNodes.Set(childNode, parentNode)
			tm.enqueueTask(ctx, edge.To.ID, tm.taskID, currentResult.result.Payload)
		}
	}
}

func (tm *TaskManager) retryDeferredTasks() {
	defer tm.wg.Done()
	ticker := time.NewTicker(tm.baseBackoff)
	defer ticker.Stop()
	for {
		select {
		case <-tm.stopCh:
			log.Println("Stopping deferred task retrier")
			return
		case <-ticker.C:
			tm.deferredTasks.ForEach(func(taskID string, tsk *task) bool {
				tm.enqueueTask(tsk.ctx, tsk.nodeID, taskID, tsk.payload)
				return true
			})
		}
	}
}

func (tm *TaskManager) processFinalResult(state *TaskState) {
	state.Status = mq.Completed
	state.targetResults.Clear()
	// update metrics using the task start time for duration calculation
	tm.dag.updateTaskMetrics(tm.taskID, state.Result, time.Since(tm.createdAt))
	if tm.dag.finalResult != nil {
		tm.dag.finalResult(tm.taskID, state.Result)
	}
}

func (tm *TaskManager) Pause() {
	tm.pauseMu.Lock()
	defer tm.pauseMu.Unlock()
	if tm.pauseCh == nil {
		tm.pauseCh = make(chan struct{})
		log.Println("TaskManager paused")
	}
}

func (tm *TaskManager) Resume() {
	tm.pauseMu.Lock()
	defer tm.pauseMu.Unlock()
	if tm.pauseCh != nil {
		close(tm.pauseCh)
		tm.pauseCh = nil
		log.Println("TaskManager resumed")
	}
}

// Stop gracefully stops the task manager
func (tm *TaskManager) Stop() {
	close(tm.stopCh)
	tm.wg.Wait()

	// Clean up resources
	tm.taskStates.Clear()
	tm.parentNodes.Clear()
	tm.childNodes.Clear()
	tm.deferredTasks.Clear()
	tm.currentNodePayload.Clear()
	tm.currentNodeResult.Clear()
}
