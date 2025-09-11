package dag

import (
	"context"
	"fmt"
	"log"
	"strconv"
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

// PriorityQueueItem represents an item in the priority queue
type PriorityQueueItem struct {
	task      *task
	priority  int   // Lower number = higher priority
	timestamp int64 // For FIFO ordering within same priority
	index     int   // For heap operations
}

// PriorityQueue implements a priority queue for deterministic task execution
type PriorityQueue struct {
	items []*PriorityQueueItem
	mu    sync.RWMutex
}

// NewPriorityQueue creates a new priority queue
func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		items: make([]*PriorityQueueItem, 0),
	}
}

// Push adds a task to the priority queue
func (pq *PriorityQueue) Push(t *task, priority int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	item := &PriorityQueueItem{
		task:      t,
		priority:  priority,
		timestamp: time.Now().UnixNano(),
	}

	pq.items = append(pq.items, item)
	pq.bubbleUp(len(pq.items) - 1)
}

// Pop removes and returns the highest priority task
func (pq *PriorityQueue) Pop() *task {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		return nil
	}

	// Get the root (highest priority)
	root := pq.items[0]

	// Move last item to root and remove last
	lastIndex := len(pq.items) - 1
	pq.items[0] = pq.items[lastIndex]
	pq.items[0].index = 0
	pq.items = pq.items[:lastIndex]

	if len(pq.items) > 0 {
		pq.bubbleDown(0)
	}

	return root.task
}

// Peek returns the highest priority task without removing it
func (pq *PriorityQueue) Peek() *task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	if len(pq.items) == 0 {
		return nil
	}
	return pq.items[0].task
}

// Len returns the number of items in the queue
func (pq *PriorityQueue) Len() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return len(pq.items)
}

// IsEmpty returns true if the queue is empty
func (pq *PriorityQueue) IsEmpty() bool {
	return pq.Len() == 0
}

// bubbleUp moves an item up the heap to maintain heap property
func (pq *PriorityQueue) bubbleUp(index int) {
	for index > 0 {
		parentIndex := (index - 1) / 2
		if pq.compare(pq.items[index], pq.items[parentIndex]) >= 0 {
			break
		}
		pq.swap(index, parentIndex)
		index = parentIndex
	}
}

// bubbleDown moves an item down the heap to maintain heap property
func (pq *PriorityQueue) bubbleDown(index int) {
	size := len(pq.items)
	for {
		left := 2*index + 1
		right := 2*index + 2
		smallest := index

		if left < size && pq.compare(pq.items[left], pq.items[smallest]) < 0 {
			smallest = left
		}
		if right < size && pq.compare(pq.items[right], pq.items[smallest]) < 0 {
			smallest = right
		}

		if smallest == index {
			break
		}

		pq.swap(index, smallest)
		index = smallest
	}
}

// compare compares two priority queue items
func (pq *PriorityQueue) compare(a, b *PriorityQueueItem) int {
	// First compare by priority (lower number = higher priority)
	if a.priority != b.priority {
		return a.priority - b.priority
	}
	// Then by timestamp (earlier = higher priority)
	return int(a.timestamp - b.timestamp)
}

// swap swaps two items in the heap
func (pq *PriorityQueue) swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

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
	taskQueue          *PriorityQueue // Changed from chan *task to PriorityQueue
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
		taskQueue:          NewPriorityQueue(), // Changed from channel to PriorityQueue
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

	// Calculate priority based on topological order
	priority := tm.calculateTaskPriority(startNode)
	tm.taskQueue.Push(t, priority)
}

// calculateTaskPriority calculates the priority of a task based on topological order
func (tm *TaskManager) calculateTaskPriority(nodeID string) int {
	pureNodeID := strings.Split(nodeID, Delimiter)[0]

	// Get topological order if available
	if topoOrder, err := tm.dag.GetTopologicalOrder(); err == nil {
		for i, node := range topoOrder {
			if node == pureNodeID {
				return i // Lower index = higher priority (processed first)
			}
		}
	}

	// Fallback: use node ID hash for deterministic ordering
	hash := 0
	for _, char := range pureNodeID {
		hash = hash*31 + int(char)
	}
	return hash
}

func (tm *TaskManager) run() {
	defer tm.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond) // Check queue every 10ms
	defer ticker.Stop()

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
				select {
				case <-tm.stopCh:
					log.Println("Stopping TaskManager run loop during pause")
					return
				case <-pch:
					// Resume from pause
				}
			}

			select {
			case <-tm.stopCh:
				log.Println("Stopping TaskManager run loop")
				return
			case <-ticker.C:
				// Try to get next task from priority queue
				if tsk := tm.taskQueue.Pop(); tsk != nil {
					tm.processNode(tsk)
				}
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
			select {
			case <-tm.stopCh:
				log.Println("Stopping TaskManager result listener during processing")
				return
			default:
				tm.onNodeCompleted(nr)
			}
		}
	}
}

// areDependenciesMet checks if all previous nodes have completed successfully
func (tm *TaskManager) areDependenciesMet(nodeID string, ctx context.Context) bool {
	// Get previous nodes
	prevNodes, err := tm.dag.GetPreviousNodes(nodeID)
	if err != nil {
		tm.dag.Logger().Error("Error getting previous nodes", logger.Field{Key: "nodeID", Value: nodeID}, logger.Field{Key: "error", Value: err.Error()})
		return false
	}

	// Check if all previous nodes have completed successfully
	for _, prevNode := range prevNodes {
		stateKey := prevNode.ID
		state, exists := tm.taskStates.Get(stateKey)
		if !exists {
			// If state doesn't exist, dependencies are not met
			return false
		}
		if state.Status != mq.Completed {
			return false
		}
	}

	return true
}

func (tm *TaskManager) processNode(exec *task) {
	startTime := time.Now()
	pureNodeID := strings.Split(exec.nodeID, Delimiter)[0]
	node, exists := tm.dag.nodes.Get(pureNodeID)
	if !exists {
		tm.dag.Logger().Error("Node not found", logger.Field{Key: "nodeID", Value: pureNodeID})
		return
	}

	// Check if all dependencies are met before processing
	if !tm.areDependenciesMet(pureNodeID, exec.ctx) {
		tm.dag.Logger().Warn("Dependencies not met for node, deferring", logger.Field{Key: "nodeID", Value: pureNodeID})
		// Defer the task
		tm.deferredTasks.Set(exec.taskID, exec)
		return
	}

	// Wrap context with timeout if node.Timeout is configured.
	if node.Timeout > 0 {
		var cancel context.CancelFunc
		exec.ctx, cancel = context.WithTimeout(exec.ctx, node.Timeout)
		defer cancel()
	}

	// Check for context cancellation before processing
	select {
	case <-exec.ctx.Done():
		tm.dag.Logger().Warn("Context cancelled before node processing",
			logger.Field{Key: "nodeID", Value: exec.nodeID},
			logger.Field{Key: "taskID", Value: exec.taskID})
		return
	default:
	}

	// Invoke PreProcessHook if available.
	if tm.dag.PreProcessHook != nil {
		exec.ctx = tm.dag.PreProcessHook(exec.ctx, node, exec.taskID, exec.payload)
	}

	// Debug logging before processing
	if node.IsDebugEnabled(tm.dag.IsDebugEnabled()) {
		tm.debugNodeStart(exec, node)
	}

	state, _ := tm.taskStates.Get(pureNodeID)
	if state == nil {
		tm.dag.Logger().Warn("State not found; creating new state", logger.Field{Key: "nodeID", Value: pureNodeID})
		state = newTaskState(pureNodeID)
		tm.taskStates.Set(pureNodeID, state)
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
					tm.dag.Logger().Warn("Recoverable error on node, retrying",
						logger.Field{Key: "nodeID", Value: exec.nodeID},
						logger.Field{Key: "attempt", Value: attempts},
						logger.Field{Key: "backoff", Value: backoff.String()},
						logger.Field{Key: "error", Value: result.Error.Error()})
					select {
					case <-time.After(backoff):
					case <-exec.ctx.Done():
						tm.dag.Logger().Warn("Context cancelled for node", logger.Field{Key: "nodeID", Value: exec.nodeID})
						return
					}
					continue
				} else if tm.recoveryHandler != nil {
					if err := tm.recoveryHandler(exec.ctx, result); err == nil {
						result.Error = nil
						result.Status = mq.Completed
					} else {
						result.Error = fmt.Errorf("recovery failed for node %s: %w", exec.nodeID, err)
					}
				}
			} else {
				// Wrap non-recoverable errors with context
				result.Error = fmt.Errorf("node %s failed: %w", exec.nodeID, result.Error)
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

	// Debug logging after processing
	if node.IsDebugEnabled(tm.dag.IsDebugEnabled()) {
		tm.debugNodeComplete(exec, node, result, nodeLatency, attempts)
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
	state.Status = mq.Completed // <-- Add this line to set state status
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
		if tm.dag.debug {
			tm.dag.Logger().Info("Node execution completed", fields...)
		}
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
			state.Status = mq.Completed
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
		// Successfully enqueued
	default:
		tm.dag.Logger().Error("Result queue is full, dropping result",
			logger.Field{Key: "nodeID", Value: nr.nodeID},
			logger.Field{Key: "taskID", Value: nr.result.TaskID})
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

	// Handle conditional edges based on ConditionStatus or expression evaluation
	conditionStatus := result.ConditionStatus
	if conditionStatus == "" {
		// Try to evaluate conditions from payload if no explicit status
		conditionStatus = tm.evaluateConditionalStatus(node, result)
	}

	if conditionStatus != "" {
		if conditions, ok := tm.dag.conditions[node.ID]; ok {
			if targetKey, exists := conditions[conditionStatus]; exists {
				if targetNode, found := tm.dag.nodes.Get(targetKey); found {
					conditionalEdge := Edge{
						From:       node,
						FromSource: node.ID,
						To:         targetNode,
						Label:      fmt.Sprintf("condition:%s", conditionStatus),
						Type:       Simple,
					}
					edges = append(edges, conditionalEdge)
				}
			} else if targetKey, exists := conditions["default"]; exists {
				if targetNode, found := tm.dag.nodes.Get(targetKey); found {
					conditionalEdge := Edge{
						From:       node,
						FromSource: node.ID,
						To:         targetNode,
						Label:      "condition:default",
						Type:       Simple,
					}
					edges = append(edges, conditionalEdge)
				}
			}
		}
	}

	return edges
}

// evaluateConditionalStatus evaluates conditional expressions to determine the next path
func (tm *TaskManager) evaluateConditionalStatus(node *Node, result mq.Result) string {
	if conditions, ok := tm.dag.conditions[node.ID]; ok {
		// Extract data from result payload for expression evaluation
		var data map[string]interface{}
		if len(result.Payload) > 0 {
			if err := json.Unmarshal(result.Payload, &data); err != nil {
				tm.dag.Logger().Warn("Failed to unmarshal result payload for condition evaluation",
					logger.Field{Key: "nodeID", Value: node.ID},
					logger.Field{Key: "error", Value: err.Error()})
				return ""
			}
		}

		// Evaluate each condition
		for conditionKey := range conditions {
			if conditionKey == "default" {
				continue // Skip default, handle it separately
			}

			// Try to evaluate as expression
			if tm.evaluateConditionExpression(conditionKey, data) {
				return conditionKey
			}
		}
	}

	return ""
}

// evaluateConditionExpression evaluates a conditional expression
func (tm *TaskManager) evaluateConditionExpression(expression string, data map[string]interface{}) bool {
	// Simple expression evaluation - can be enhanced with a proper expression engine
	parts := strings.Fields(expression)
	if len(parts) < 3 {
		return false
	}

	field := parts[0]
	operator := parts[1]
	expectedValue := strings.Join(parts[2:], " ")

	if fieldValue, ok := data[field]; ok {
		actualStr := fmt.Sprintf("%v", fieldValue)
		switch operator {
		case "==", "=":
			return actualStr == expectedValue
		case "!=":
			return actualStr != expectedValue
		case ">":
			if actualNum, err := strconv.ParseFloat(actualStr, 64); err == nil {
				if expectedNum, err := strconv.ParseFloat(expectedValue, 64); err == nil {
					return actualNum > expectedNum
				}
			}
		case "<":
			if actualNum, err := strconv.ParseFloat(actualStr, 64); err == nil {
				if expectedNum, err := strconv.ParseFloat(expectedValue, 64); err == nil {
					return actualNum < expectedNum
				}
			}
		case ">=":
			if actualNum, err := strconv.ParseFloat(actualStr, 64); err == nil {
				if expectedNum, err := strconv.ParseFloat(expectedValue, 64); err == nil {
					return actualNum >= expectedNum
				}
			}
		case "<=":
			if actualNum, err := strconv.ParseFloat(actualStr, 64); err == nil {
				if expectedNum, err := strconv.ParseFloat(expectedValue, 64); err == nil {
					return actualNum <= expectedNum
				}
			}
		case "contains":
			return strings.Contains(actualStr, expectedValue)
		case "startswith":
			return strings.HasPrefix(actualStr, expectedValue)
		case "endswith":
			return strings.HasSuffix(actualStr, expectedValue)
		}
	}

	return false
}

func (tm *TaskManager) handleEdges(currentResult nodeResult, edges []Edge) {
	if len(edges) == 0 {
		return
	}

	if len(edges) == 1 {
		tm.processSingleEdge(currentResult, edges[0])
		return
	}

	// For multiple edges, process sequentially to avoid race conditions
	for _, edge := range edges {
		tm.processSingleEdge(currentResult, edge)
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
			if _, exists := tm.iteratorNodes.Get(edge.From.ID); !exists {
				return
			}
			parentNode = edge.From.ID
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
			// Process deferred tasks with a limit to prevent overwhelming the queue
			processed := 0
			tm.deferredTasks.ForEach(func(taskID string, tsk *task) bool {
				if processed >= 10 { // Process max 10 deferred tasks per tick
					return false
				}

				// Calculate priority and push to priority queue
				priority := tm.calculateTaskPriority(tsk.nodeID)
				tm.taskQueue.Push(tsk, priority)
				tm.deferredTasks.Del(taskID)
				processed++
				tm.dag.Logger().Debug("Retried deferred task",
					logger.Field{Key: "taskID", Value: taskID},
					logger.Field{Key: "nodeID", Value: tsk.nodeID},
					logger.Field{Key: "priority", Value: priority})
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

	// Cancel any pending operations
	tm.pauseMu.Lock()
	if tm.pauseCh != nil {
		close(tm.pauseCh)
		tm.pauseCh = nil
	}
	tm.pauseMu.Unlock()

	// Clean up resources
	tm.taskStates.Clear()
	tm.parentNodes.Clear()
	tm.childNodes.Clear()
	tm.deferredTasks.Clear()
	tm.currentNodePayload.Clear()
	tm.currentNodeResult.Clear()

	tm.dag.Logger().Info("TaskManager stopped gracefully",
		logger.Field{Key: "taskID", Value: tm.taskID})
}

// debugNodeStart logs debug information when a node starts processing
func (tm *TaskManager) debugNodeStart(exec *task, node *Node) {
	var payload map[string]any
	if err := json.Unmarshal(exec.payload, &payload); err != nil {
		payload = map[string]any{"raw_payload": string(exec.payload)}
	}

	tm.dag.Logger().Info("🐛 [DEBUG] Node processing started",
		logger.Field{Key: "dag_name", Value: tm.dag.name},
		logger.Field{Key: "task_id", Value: exec.taskID},
		logger.Field{Key: "node_id", Value: node.ID},
		logger.Field{Key: "node_type", Value: node.NodeType.String()},
		logger.Field{Key: "node_label", Value: node.Label},
		logger.Field{Key: "timestamp", Value: time.Now().Format(time.RFC3339)},
		logger.Field{Key: "has_timeout", Value: node.Timeout > 0},
		logger.Field{Key: "timeout_duration", Value: node.Timeout.String()},
		logger.Field{Key: "payload_size", Value: len(exec.payload)},
		logger.Field{Key: "payload_preview", Value: tm.getPayloadPreview(payload)},
		logger.Field{Key: "debug_mode", Value: "individual_node:" + fmt.Sprintf("%t", node.Debug) + ", dag_global:" + fmt.Sprintf("%t", tm.dag.IsDebugEnabled())},
	)

	// Log processor type if it implements the Debugger interface
	if debugger, ok := node.processor.(Debugger); ok {
		debugger.Debug(exec.ctx, mq.NewTask(exec.taskID, exec.payload, exec.nodeID, mq.WithDAG(tm.dag)))
	}
}

// debugNodeComplete logs debug information when a node completes processing
func (tm *TaskManager) debugNodeComplete(exec *task, node *Node, result mq.Result, latency time.Duration, attempts int) {
	var resultPayload map[string]any
	if len(result.Payload) > 0 {
		if err := json.Unmarshal(result.Payload, &resultPayload); err != nil {
			resultPayload = map[string]any{"raw_payload": string(result.Payload)}
		}
	}

	tm.dag.Logger().Info("🐛 [DEBUG] Node processing completed",
		logger.Field{Key: "dag_name", Value: tm.dag.name},
		logger.Field{Key: "task_id", Value: exec.taskID},
		logger.Field{Key: "node_id", Value: node.ID},
		logger.Field{Key: "node_type", Value: node.NodeType.String()},
		logger.Field{Key: "node_label", Value: node.Label},
		logger.Field{Key: "timestamp", Value: time.Now().Format(time.RFC3339)},
		logger.Field{Key: "status", Value: string(result.Status)},
		logger.Field{Key: "latency", Value: latency.String()},
		logger.Field{Key: "attempts", Value: attempts + 1},
		logger.Field{Key: "has_error", Value: result.Error != nil},
		logger.Field{Key: "error_message", Value: tm.getErrorMessage(result.Error)},
		logger.Field{Key: "result_size", Value: len(result.Payload)},
		logger.Field{Key: "result_preview", Value: tm.getPayloadPreview(resultPayload)},
		logger.Field{Key: "is_last_node", Value: result.Last},
	)
}

// getPayloadPreview returns a truncated version of the payload for debug logging
func (tm *TaskManager) getPayloadPreview(payload map[string]any) string {
	if payload == nil {
		return "null"
	}

	preview := make(map[string]any)
	count := 0
	maxFields := 5 // Limit to first 5 fields to avoid log spam

	for key, value := range payload {
		if count >= maxFields {
			preview["..."] = fmt.Sprintf("and %d more fields", len(payload)-maxFields)
			break
		}

		// Truncate string values if they're too long
		if strVal, ok := value.(string); ok && len(strVal) > 100 {
			preview[key] = strVal[:97] + "..."
		} else {
			preview[key] = value
		}
		count++
	}

	previewBytes, _ := json.Marshal(preview)
	return string(previewBytes)
}

// getErrorMessage safely extracts error message
func (tm *TaskManager) getErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
