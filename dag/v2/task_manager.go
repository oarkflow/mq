package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq"
	"log"
	"strings"
	"time"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

type TaskState struct {
	NodeID        string
	Status        mq.Status
	UpdatedAt     time.Time
	Result        mq.Result
	targetResults storage.IMap[string, mq.Result]
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
	taskStates    storage.IMap[string, *TaskState]
	parentNodes   storage.IMap[string, string]
	childNodes    storage.IMap[string, int]
	deferredTasks storage.IMap[string, *task]
	iteratorNodes storage.IMap[string, []Edge]
	currentNode   string
	dag           *DAG
	taskID        string
	taskQueue     chan *task
	resultQueue   chan nodeResult
	resultCh      chan mq.Result
	stopCh        chan struct{}
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
		taskStates:    memory.New[string, *TaskState](),
		parentNodes:   memory.New[string, string](),
		childNodes:    memory.New[string, int](),
		deferredTasks: memory.New[string, *task](),
		taskQueue:     make(chan *task, DefaultChannelSize),
		resultQueue:   make(chan nodeResult, DefaultChannelSize),
		iteratorNodes: iteratorNodes,
		stopCh:        make(chan struct{}),
		resultCh:      resultCh,
		taskID:        taskID,
		dag:           dag,
	}
	go tm.run()
	go tm.waitForResult()
	return tm
}

func (tm *TaskManager) ProcessTask(ctx context.Context, startNode string, payload json.RawMessage) {
	tm.send(ctx, startNode, tm.taskID, payload)
}

func (tm *TaskManager) send(ctx context.Context, startNode, taskID string, payload json.RawMessage) {
	if index, ok := ctx.Value(ContextIndex).(string); ok {
		startNode = strings.Split(startNode, Delimiter)[0]
		startNode = fmt.Sprintf("%s%s%s", startNode, Delimiter, index)
	}
	if _, exists := tm.taskStates.Get(startNode); !exists {
		tm.taskStates.Set(startNode, newTaskState(startNode))
	}
	t := newTask(ctx, taskID, startNode, payload)
	select {
	case tm.taskQueue <- t:
	default:
		log.Println("task queue is full, dropping task.")
		tm.deferredTasks.Set(taskID, t)
	}
}

func (tm *TaskManager) run() {
	for {
		select {
		case <-tm.stopCh:
			log.Println("Stopping TaskManager")
			return
		case task := <-tm.taskQueue:
			tm.processNode(task)
		}
	}
}

func (tm *TaskManager) waitForResult() {
	for {
		select {
		case <-tm.stopCh:
			log.Println("Stopping Result Listener")
			return
		case nr := <-tm.resultQueue:
			tm.onNodeCompleted(nr)
		}
	}
}

func (tm *TaskManager) processNode(exec *task) {
	pureNodeID := strings.Split(exec.nodeID, Delimiter)[0]
	node, exists := tm.dag.nodes.Get(pureNodeID)
	if !exists {
		log.Printf("Node %s does not exist while processing node\n", pureNodeID)
		return
	}
	state, _ := tm.taskStates.Get(exec.nodeID)
	if state == nil {
		log.Printf("State for node %s not found; creating new state.\n", exec.nodeID)
		state = newTaskState(exec.nodeID)
		tm.taskStates.Set(exec.nodeID, state)
	}
	state.Status = mq.Processing
	state.UpdatedAt = time.Now()
	tm.currentNode = exec.nodeID
	result := node.processor.ProcessTask(exec.ctx, mq.NewTask(exec.taskID, exec.payload, exec.nodeID))
	state.Result = result
	result.Topic = node.ID
	if result.Error != nil {
		tm.resultCh <- result
		tm.processFinalResult(state)
		return
	}
	if node.NodeType == Page {
		tm.resultCh <- result
		return
	}
	tm.handleNext(exec.ctx, node, state, result)
}

func (tm *TaskManager) handlePrevious(ctx context.Context, state *TaskState, result mq.Result, childNode string, dispatchFinal bool) {
	state.targetResults.Set(childNode, result)
	state.targetResults.Del(state.NodeID)
	targetsCount, _ := tm.childNodes.Get(state.NodeID)
	size := state.targetResults.Size()
	nodeID := strings.Split(state.NodeID, Delimiter)
	if size == targetsCount {
		if size > 1 {
			aggregatedData := make([]json.RawMessage, size)
			i := 0
			state.targetResults.ForEach(func(_ string, rs mq.Result) bool {
				aggregatedData[i] = rs.Payload
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
	pn, ok := tm.parentNodes.Get(state.NodeID)
	if edges, exists := tm.iteratorNodes.Get(nodeID[0]); exists && state.Status == mq.Completed {
		state.Status = mq.Processing
		tm.iteratorNodes.Del(nodeID[0])
		state.targetResults.Clear()
		if len(nodeID) == 2 {
			ctx = context.WithValue(ctx, ContextIndex, nodeID[1])
		}
		toProcess := nodeResult{
			ctx:    ctx,
			nodeID: state.NodeID,
			status: state.Status,
			result: state.Result,
		}
		tm.handleEdges(toProcess, edges)
	} else if ok {
		if targetsCount == size {
			parentState, _ := tm.taskStates.Get(pn)
			if parentState != nil {
				state.Result.Topic = state.NodeID
				tm.handlePrevious(ctx, parentState, state.Result, state.NodeID, dispatchFinal)
			}
		}
	} else {
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
	select {
	case tm.resultQueue <- nodeResult{
		ctx:    ctx,
		nodeID: state.NodeID,
		result: result,
		status: state.Status,
	}:
	default:
		log.Println("Result queue is full, dropping result.")
	}
}

func (tm *TaskManager) onNodeCompleted(rs nodeResult) {
	nodeID := strings.Split(rs.nodeID, Delimiter)[0]
	node, ok := tm.dag.nodes.Get(nodeID)
	if !ok {
		return
	}
	edges := tm.getConditionalEdges(node, rs.result)
	hasErrorOrCompleted := rs.result.Error != nil || len(edges) == 0
	if hasErrorOrCompleted {
		if index, ok := rs.ctx.Value(ContextIndex).(string); ok {
			childNode := fmt.Sprintf("%s%s%s", node.ID, Delimiter, index)
			pn, ok := tm.parentNodes.Get(childNode)
			if ok {
				parentState, _ := tm.taskStates.Get(pn)
				if parentState != nil {
					pn = strings.Split(pn, Delimiter)[0]
					tm.handlePrevious(rs.ctx, parentState, rs.result, rs.nodeID, true)
				}
			}
		}
		return
	}
	tm.handleEdges(rs, edges)
}

func (tm *TaskManager) getConditionalEdges(node *Node, result mq.Result) []Edge {
	edges := make([]Edge, len(node.Edges))
	copy(edges, node.Edges)
	if result.ConditionStatus != "" {
		if conditions, ok := tm.dag.conditions[result.Topic]; ok {
			if targetNodeKey, ok := conditions[result.ConditionStatus]; ok {
				if targetNode, ok := tm.dag.nodes.Get(targetNodeKey); ok {
					edges = append(edges, Edge{From: node, To: targetNode})
				}
			} else if targetNodeKey, ok = conditions["default"]; ok {
				if targetNode, ok := tm.dag.nodes.Get(targetNodeKey); ok {
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
		if edge.Type == Simple {
			if _, ok := tm.iteratorNodes.Get(edge.From.ID); ok {
				continue
			}
		}
		if edge.Type == Iterator {
			var items []json.RawMessage
			err := json.Unmarshal(currentResult.result.Payload, &items)
			if err != nil {
				log.Printf("Error unmarshalling data for node %s: %v\n", edge.To.ID, err)
				tm.resultQueue <- nodeResult{
					ctx:    currentResult.ctx,
					nodeID: edge.To.ID,
					status: mq.Failed,
					result: mq.Result{Error: err},
				}
				return
			}
			tm.childNodes.Set(parentNode, len(items))
			for i, item := range items {
				childNode := fmt.Sprintf("%s%s%d", edge.To.ID, Delimiter, i)
				ctx := context.WithValue(currentResult.ctx, ContextIndex, fmt.Sprintf("%d", i))
				tm.parentNodes.Set(childNode, parentNode)
				tm.send(ctx, edge.To.ID, tm.taskID, item)
			}
		} else {
			tm.childNodes.Set(parentNode, 1)
			idx, ok := currentResult.ctx.Value(ContextIndex).(string)
			if !ok {
				idx = "0"
			}
			childNode := fmt.Sprintf("%s%s%s", edge.To.ID, Delimiter, idx)
			ctx := context.WithValue(currentResult.ctx, ContextIndex, idx)
			tm.parentNodes.Set(childNode, parentNode)
			tm.send(ctx, edge.To.ID, tm.taskID, currentResult.result.Payload)
		}
	}
}

func (tm *TaskManager) retryDeferredTasks() {
	const maxRetries = 5
	retries := 0
	for retries < maxRetries {
		select {
		case <-tm.stopCh:
			log.Println("Stopping Deferred task Retrier")
			return
		case <-time.After(RetryInterval):
			tm.deferredTasks.ForEach(func(taskID string, task *task) bool {
				tm.send(task.ctx, task.nodeID, taskID, task.payload)
				retries++
				return true
			})
		}
	}
}

func (tm *TaskManager) processFinalResult(state *TaskState) {
	state.targetResults.Clear()
	if tm.dag.finalResult != nil {
		tm.dag.finalResult(tm.taskID, state.Result)
	}
}

func (tm *TaskManager) Stop() {
	close(tm.stopCh)
}
