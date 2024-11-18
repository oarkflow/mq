package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

type TaskState struct {
	NodeID        string
	Status        TaskStatus
	UpdatedAt     time.Time
	Result        Result
	targetResults storage.IMap[string, Result]
}

type nodeResult struct {
	ctx    context.Context
	taskID string
	nodeID string
	result Result
}

type TaskManager struct {
	taskStates  map[string]*TaskState
	currentNode string
	dag         *DAG
	mu          sync.RWMutex
	taskQueue   chan *Task
	resultQueue chan nodeResult
	resultCh    chan Result
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

func NewTaskManager(dag *DAG, resultCh chan Result) *TaskManager {
	tm := &TaskManager{
		taskStates:  make(map[string]*TaskState),
		taskQueue:   make(chan *Task, 100),
		resultQueue: make(chan nodeResult, 100),
		resultCh:    resultCh,
		dag:         dag,
	}
	go tm.Run()
	go tm.WaitForResult()
	return tm
}

func (tm *TaskManager) ProcessTask(ctx context.Context, taskID, startNode string, payload json.RawMessage) {
	tm.mu.Lock()
	tm.taskStates[startNode] = newTaskState(startNode)
	tm.mu.Unlock()
	tm.taskQueue <- NewTask(ctx, taskID, startNode, payload)
}

func newTaskState(nodeID string) *TaskState {
	return &TaskState{
		NodeID:        nodeID,
		Status:        StatusPending,
		UpdatedAt:     time.Now(),
		targetResults: memory.New[string, Result](),
	}
}

func (tm *TaskManager) Run() {
	go func() {
		for task := range tm.taskQueue {
			tm.processNode(task)
		}
	}()
}

func (tm *TaskManager) processNode(exec *Task) {
	node, exists := tm.dag.nodes.Get(exec.nodeID)
	if !exists {
		fmt.Printf("Node %s does not exist\n", exec.nodeID)
		return
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	state := tm.taskStates[exec.nodeID]
	if state == nil {
		state = newTaskState(exec.nodeID)
		tm.taskStates[exec.nodeID] = state
	}
	state.Status = StatusProcessing
	state.UpdatedAt = time.Now()
	tm.currentNode = exec.nodeID
	result := node.Handler(exec.ctx, exec.payload)
	state.UpdatedAt = time.Now()
	state.Result = result
	if result.Ctx == nil {
		result.Ctx = exec.ctx
	}
	if result.Error != nil {
		state.Status = StatusFailed
	} else {
		state.Status = StatusCompleted
	}
	if node.Type == Page {
		tm.resultCh <- result
		return
	}
	tm.resultQueue <- nodeResult{taskID: exec.taskID, nodeID: exec.nodeID, result: result, ctx: exec.ctx}
}

func (tm *TaskManager) WaitForResult() {
	go func() {
		for nr := range tm.resultQueue {
			tm.onNodeCompleted(nr)
		}
	}()
}

func (tm *TaskManager) onNodeCompleted(nodeResult nodeResult) {
	node, ok := tm.dag.nodes.Get(nodeResult.nodeID)
	if !ok {
		return
	}
	if nodeResult.result.Error != nil || len(node.Edges) == 0 {
		parentNodes, err := tm.dag.GetPreviousNodes(nodeResult.nodeID)
		if err == nil {
			for _, parentNode := range parentNodes {
				tm.mu.Lock()
				state := tm.taskStates[parentNode.ID]
				if state == nil {
					state = newTaskState(parentNode.ID)
					tm.taskStates[parentNode.ID] = state
				}
				state.targetResults.Set(nodeResult.nodeID, nodeResult.result)
				allTargetNodesDone := len(parentNode.Edges) == state.targetResults.Size()
				tm.mu.Unlock()

				if tm.areAllTargetNodesCompleted(parentNode.ID) && allTargetNodesDone {
					tm.aggregateResults(parentNode.ID, nodeResult.taskID)
				}
			}
		}
		return
	}
	for _, edge := range node.Edges {
		tm.mu.Lock()
		if _, exists := tm.taskStates[edge.To.ID]; !exists {
			tm.taskStates[edge.To.ID] = newTaskState(edge.To.ID)
		}
		tm.mu.Unlock()
		tm.taskQueue <- NewTask(nodeResult.ctx, nodeResult.taskID, edge.To.ID, nodeResult.result.Data)
	}
}

func (tm *TaskManager) areAllTargetNodesCompleted(parentNodeID string) bool {
	parentNode, ok := tm.dag.nodes.Get(parentNodeID)
	if !ok {
		return false
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, targetNode := range parentNode.Edges {
		state := tm.taskStates[targetNode.To.ID]
		if state == nil || state.Status != StatusCompleted {
			return false
		}
	}
	return true
}

func (tm *TaskManager) aggregateResults(parentNode string, taskID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	state := tm.taskStates[parentNode]
	if state.targetResults.Size() > 1 {
		aggregatedData := make([]json.RawMessage, state.targetResults.Size())
		i := 0
		state.targetResults.ForEach(func(_ string, result Result) bool {
			aggregatedData[i] = result.Data
			i++
			return true
		})
		aggregatedPayload, _ := json.Marshal(aggregatedData)
		state.Result = Result{Data: aggregatedPayload, Status: StatusCompleted}
	} else if state.targetResults.Size() == 1 {
		state.Result = state.targetResults.Values()[0]
	}
	tm.processFinalResult(taskID, state)
}

func (tm *TaskManager) processFinalResult(taskID string, state *TaskState) {
	state.targetResults.Clear()
	tm.dag.finalResult(taskID, state.Result)
}
