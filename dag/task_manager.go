package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

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

	wg sync.WaitGroup
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

func NewTaskManager(dag *DAG, taskID string, resultCh chan mq.Result, iteratorNodes storage.IMap[string, []Edge]) *TaskManager {
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
		iteratorNodes:      iteratorNodes,
		stopCh:             make(chan struct{}),
		resultCh:           resultCh,
		taskID:             taskID,
		dag:                dag,
	}

	tm.wg.Add(2)
	go tm.run()
	go tm.waitForResult()
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
		log.Println("task queue is full, deferring task")
		tm.deferredTasks.Set(taskID, t)
	}
}

func (tm *TaskManager) run() {
	defer tm.wg.Done()
	for {
		select {
		case <-tm.stopCh:
			log.Println("Stopping TaskManager")
			return
		case tsk := <-tm.taskQueue:
			tm.processNode(tsk)
		}
	}
}

func (tm *TaskManager) waitForResult() {
	defer tm.wg.Done()
	for {
		select {
		case <-tm.stopCh:
			log.Println("Stopping Result Listener")
			return
		case res := <-tm.resultQueue:
			tm.onNodeCompleted(res)
		}
	}
}

func (tm *TaskManager) processNode(exec *task) {
	startTime := time.Now()
	pureNodeID := strings.Split(exec.nodeID, Delimiter)[0]

	node, exists := tm.dag.nodes.Get(pureNodeID)
	if !exists {
		tm.dag.Logger().Error("Node not found while processing node",
			logger.Field{Key: "nodeID", Value: pureNodeID})
		return
	}

	state, _ := tm.taskStates.Get(exec.nodeID)
	if state == nil {
		tm.dag.Logger().Warn("State not found; creating new state",
			logger.Field{Key: "nodeID", Value: exec.nodeID})
		state = newTaskState(exec.nodeID)
		tm.taskStates.Set(exec.nodeID, state)
	}

	state.Status = mq.Processing
	state.UpdatedAt = time.Now()
	tm.currentNodePayload.Clear()
	tm.currentNodeResult.Clear()
	tm.currentNodePayload.Set(exec.nodeID, exec.payload)

	result := node.processor.ProcessTask(exec.ctx, mq.NewTask(exec.taskID, exec.payload, exec.nodeID))
	nodeLatency := time.Since(startTime)
	tm.logNodeExecution(exec, pureNodeID, result, nodeLatency)

	if isLast, err := tm.dag.IsLastNode(pureNodeID); err != nil {
		tm.dag.Logger().Error("Error checking if node is last",
			logger.Field{Key: "nodeID", Value: pureNodeID},
			logger.Field{Key: "error", Value: err.Error()})
	} else if isLast {
		result.Last = true
	}

	tm.currentNodeResult.Set(exec.nodeID, result)
	state.Result = result
	result.Topic = node.ID
	tm.updateTimestamps(&result)

	if result.Error != nil {
		result.Status = mq.Failed
		state.Status = mq.Failed
		state.Result.Status = mq.Failed
		state.Result.Latency = result.Latency
		tm.result = &result
		tm.resultCh <- result
		tm.processFinalResult(state)
		return
	}

	result.Status = mq.Completed
	state.Result.Status = mq.Completed
	state.Result.Latency = result.Latency

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

func (tm *TaskManager) logNodeExecution(exec *task, pureNodeID string, result mq.Result, latency time.Duration) {
	fields := []logger.Field{
		{Key: "nodeID", Value: exec.nodeID},
		{Key: "pureNodeID", Value: pureNodeID},
		{Key: "taskID", Value: exec.taskID},
		{Key: "latency", Value: latency.String()},
	}

	if result.Error != nil {
		fields = append(fields, logger.Field{Key: "error", Value: result.Error.Error()})
		fields = append(fields, logger.Field{Key: "status", Value: mq.Failed})
		tm.dag.Logger().Error("Node execution failed", fields...)
	} else {
		fields = append(fields, logger.Field{Key: "status", Value: mq.Completed})
		tm.dag.Logger().Info("Node executed successfully", fields...)
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

	targetCount, _ := tm.childNodes.Get(state.NodeID)
	size := state.targetResults.Size()

	if size == targetCount {
		if size > 1 {
			aggregatedData := make([]json.RawMessage, size)
			i := 0
			state.targetResults.ForEach(func(_ string, res mq.Result) bool {
				aggregatedData[i] = res.Payload
				i++
				return true
			})
			aggregatedPayload, err := json.Marshal(aggregatedData)
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

		nodeIDParts := strings.Split(state.NodeID, Delimiter)
		if edges, exists := tm.iteratorNodes.Get(nodeIDParts[0]); exists && state.Status == mq.Completed {
			state.Status = mq.Processing
			tm.iteratorNodes.Del(nodeIDParts[0])
			state.targetResults.Clear()
			if len(nodeIDParts) == 2 {
				ctx = context.WithValue(ctx, ContextIndex, nodeIDParts[1])
			}
			toProcess := nodeResult{
				ctx:    ctx,
				nodeID: state.NodeID,
				status: state.Status,
				result: state.Result,
			}
			tm.handleEdges(toProcess, edges)
		} else if size == targetCount {
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
		log.Println("Result queue is full, dropping result.")
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
			if targetNodeKey, exists := conditions[result.ConditionStatus]; exists {
				if targetNode, found := tm.dag.nodes.Get(targetNodeKey); found {
					edges = append(edges, Edge{From: node, To: targetNode})
				}
			} else if targetNodeKey, exists := conditions["default"]; exists {
				if targetNode, found := tm.dag.nodes.Get(targetNodeKey); found {
					edges = append(edges, Edge{From: node, To: targetNode})
				}
			}
		}
	}
	return edges
}

func (tm *TaskManager) handleEdges(currentResult nodeResult, edges []Edge) {
	for _, edge := range edges {
		index, ok := currentResult.ctx.Value(ContextIndex).(string)
		if !ok {
			index = "0"
		}
		parentNode := fmt.Sprintf("%s%s%s", edge.From.ID, Delimiter, index)

		switch edge.Type {
		case Simple:
			if _, exists := tm.iteratorNodes.Get(edge.From.ID); exists {
				continue
			}
			fallthrough
		case Iterator:
			if edge.Type == Iterator {
				var items []json.RawMessage
				if err := json.Unmarshal(currentResult.result.Payload, &items); err != nil {
					log.Printf("Error unmarshalling data for node %s: %v\n", edge.To.ID, err)
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
}

func (tm *TaskManager) retryDeferredTasks() {
	const maxRetries = 5
	backoff := time.Second
	for retries := 0; retries < maxRetries; retries++ {
		select {
		case <-tm.stopCh:
			log.Println("Stopping Deferred Task Retrier")
			return
		case <-time.After(backoff):
			tm.deferredTasks.ForEach(func(taskID string, tsk *task) bool {
				tm.enqueueTask(tsk.ctx, tsk.nodeID, taskID, tsk.payload)
				return true
			})
			backoff *= 2
		}
	}
}

func (tm *TaskManager) processFinalResult(state *TaskState) {
	state.Status = mq.Completed
	state.targetResults.Clear()
	if tm.dag.finalResult != nil {
		tm.dag.finalResult(tm.taskID, state.Result)
	}
}

func (tm *TaskManager) Stop() {
	close(tm.stopCh)
	tm.wg.Wait()
}
