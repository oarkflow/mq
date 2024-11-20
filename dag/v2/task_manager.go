package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

const (
	Delimiter          = "___"
	ContextIndex       = "index"
	DefaultChannelSize = 100
	RetryInterval      = 5 * time.Second
)

type TaskState struct {
	NodeID        string
	Status        TaskStatus
	UpdatedAt     time.Time
	Result        Result
	targetResults storage.IMap[string, Result]
}

func newTaskState(nodeID string) *TaskState {
	return &TaskState{
		NodeID:        nodeID,
		Status:        StatusPending,
		UpdatedAt:     time.Now(),
		targetResults: memory.New[string, Result](),
	}
}

type nodeResult struct {
	ctx    context.Context
	nodeID string
	status TaskStatus
	result Result
}

type TaskManager struct {
	taskStates    storage.IMap[string, *TaskState]
	parentNodes   storage.IMap[string, string]
	childNodes    storage.IMap[string, int]
	deferredTasks storage.IMap[string, *Task]
	currentNode   string
	dag           *DAG
	taskID        string
	taskQueue     chan *Task
	resultQueue   chan nodeResult
	resultCh      chan Result
	stopCh        chan struct{}
}

type Task struct {
	ctx     context.Context
	taskID  string
	nodeID  string
	payload json.RawMessage
}

func NewTask(ctx context.Context, taskID, nodeID string, payload json.RawMessage) *Task {
	return &Task{
		ctx:     ctx,
		taskID:  taskID,
		nodeID:  nodeID,
		payload: payload,
	}
}

func NewTaskManager(dag *DAG, taskID string, resultCh chan Result) *TaskManager {
	tm := &TaskManager{
		taskStates:    memory.New[string, *TaskState](),
		parentNodes:   memory.New[string, string](),
		childNodes:    memory.New[string, int](),
		deferredTasks: memory.New[string, *Task](),
		taskQueue:     make(chan *Task, DefaultChannelSize),
		resultQueue:   make(chan nodeResult, DefaultChannelSize),
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
	task := NewTask(ctx, taskID, startNode, payload)
	select {
	case tm.taskQueue <- task:
	default:
		log.Println("Task queue is full, dropping task.")
		tm.deferredTasks.Set(taskID, task)
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

func (tm *TaskManager) processNode(exec *Task) {
	pureNodeID := strings.Split(exec.nodeID, Delimiter)[0]
	node, exists := tm.dag.nodes.Get(pureNodeID)
	if !exists {
		log.Printf("Node %s does not exist while processing node\n", pureNodeID)
		return
	}
	state, _ := tm.taskStates.Get(exec.nodeID)
	if state == nil {
		state = newTaskState(exec.nodeID)
		tm.taskStates.Set(exec.nodeID, state)
	}
	state.Status = StatusProcessing
	state.UpdatedAt = time.Now()
	tm.currentNode = exec.nodeID
	result := node.Handler(exec.ctx, exec.payload)
	state.Result = result
	result.Topic = node.ID
	if result.Error != nil {
		tm.resultCh <- result
		tm.processFinalResult(state)
		return
	}
	if node.Type == Page {
		tm.resultCh <- result
		return
	}
	tm.handleNext(exec.ctx, node, state, result)
}

func (tm *TaskManager) handlePrevious(ctx context.Context, state *TaskState, result Result, childNode string) {
	state.targetResults.Set(childNode, result)
	state.targetResults.Del(state.NodeID)
	targetsCount, _ := tm.childNodes.Get(state.NodeID)
	size := state.targetResults.Size()
	nodeID := strings.Split(state.NodeID, Delimiter)
	if size == targetsCount {
		if size > 1 {
			aggregatedData := make([]json.RawMessage, size)
			i := 0
			state.targetResults.ForEach(func(_ string, rs Result) bool {
				aggregatedData[i] = rs.Data
				i++
				return true
			})
			aggregatedPayload, err := json.Marshal(aggregatedData)
			if err != nil {
				panic(err)
			}
			state.Result = Result{Data: aggregatedPayload, Status: StatusCompleted, Ctx: ctx, Topic: state.NodeID}
		} else if size == 1 {
			state.Result = state.targetResults.Values()[0]
		}
		state.Status = result.Status
		state.Result.Status = result.Status
	}
	if state.Result.Data == nil {
		state.Result.Data = result.Data
	}
	state.UpdatedAt = time.Now()
	if result.Ctx == nil {
		result.Ctx = ctx
	}
	if result.Error != nil {
		state.Status = StatusFailed
	}
	pn, ok := tm.parentNodes.Get(state.NodeID)
	if edges, exists := tm.dag.iteratorNodes.Get(nodeID[0]); exists && state.Status == StatusCompleted {
		state.Status = StatusProcessing
		tm.dag.iteratorNodes.Del(nodeID[0])
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
				tm.handlePrevious(ctx, parentState, state.Result, state.NodeID)
			}
		}
	} else {
		tm.resultCh <- state.Result
		tm.processFinalResult(state)
	}
}

func (tm *TaskManager) handleNext(ctx context.Context, node *Node, state *TaskState, result Result) {
	state.UpdatedAt = time.Now()
	if result.Ctx == nil {
		result.Ctx = ctx
	}
	if result.Error != nil {
		state.Status = StatusFailed
	} else {
		edges := tm.getConditionalEdges(node, result)
		if len(edges) == 0 {
			state.Status = StatusCompleted
		}
	}
	if node.Type == Page {
		tm.resultCh <- result
		return
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
					tm.handlePrevious(rs.ctx, parentState, rs.result, rs.nodeID)
				}
			}
		}
		return
	}
	tm.handleEdges(rs, edges)
}

func (tm *TaskManager) getConditionalEdges(node *Node, result Result) []Edge {
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
			if _, ok := tm.dag.iteratorNodes.Get(edge.From.ID); ok {
				continue
			}
		}
		if edge.Type == Iterator {
			var items []json.RawMessage
			err := json.Unmarshal(currentResult.result.Data, &items)
			if err != nil {
				tm.resultQueue <- nodeResult{
					ctx:    currentResult.ctx,
					nodeID: edge.To.ID,
					status: StatusFailed,
					result: Result{Error: err},
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
			tm.send(ctx, edge.To.ID, tm.taskID, currentResult.result.Data)
		}
	}
}

func (tm *TaskManager) retryDeferredTasks() {
	for {
		select {
		case <-tm.stopCh:
			log.Println("Stopping Deferred Task Retrier")
			return
		case <-time.After(RetryInterval):
			tm.deferredTasks.ForEach(func(taskID string, task *Task) bool {
				tm.send(task.ctx, task.nodeID, taskID, task.payload)
				return true
			})
		}
	}
}

func (tm *TaskManager) processFinalResult(state *TaskState) {
	state.targetResults.Clear()
	tm.dag.finalResult(tm.taskID, state.Result)
}

func (tm *TaskManager) Stop() {
	close(tm.stopCh)
}
