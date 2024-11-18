package v2

import (
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
	Timestamp     time.Time
	Result        Result
	targetResults storage.IMap[string, Result]
}

type nodeResult struct {
	taskID string
	nodeID string
	result Result
}

type TaskManager struct {
	taskStates  map[string]*TaskState
	dag         *DAG
	mu          sync.Mutex
	taskQueue   chan taskExecution
	resultQueue chan nodeResult
}

type taskExecution struct {
	taskID  string
	nodeID  string
	payload json.RawMessage
}

func NewTaskManager(dag *DAG) *TaskManager {
	tm := &TaskManager{
		taskStates:  make(map[string]*TaskState),
		taskQueue:   make(chan taskExecution, 100),
		resultQueue: make(chan nodeResult, 100),
		dag:         dag,
	}
	go tm.Run()
	go tm.WaitForResult()
	return tm
}

func (tm *TaskManager) Trigger(taskID, startNode string, payload json.RawMessage) {
	tm.mu.Lock()
	tm.taskStates[startNode] = newTaskState(startNode)
	tm.mu.Unlock()
	tm.taskQueue <- taskExecution{taskID: taskID, nodeID: startNode, payload: payload}
}

func newTaskState(nodeID string) *TaskState {
	return &TaskState{
		NodeID:        nodeID,
		Status:        StatusPending,
		Timestamp:     time.Now(),
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

func (tm *TaskManager) processNode(exec taskExecution) {
	node, exists := tm.dag.nodes.Get(exec.nodeID)
	if !exists {
		fmt.Printf("Node %s does not exist\n", exec.nodeID)
		return
	}
	tm.mu.Lock()
	state := tm.taskStates[exec.nodeID]
	if state == nil {
		state = newTaskState(exec.nodeID)
		tm.taskStates[exec.nodeID] = state
	}
	state.Status = StatusProcessing
	state.Timestamp = time.Now()
	tm.mu.Unlock()
	result := node.Handler(exec.payload)
	tm.mu.Lock()
	state.Timestamp = time.Now()
	state.Result = result
	state.Status = result.Status
	tm.mu.Unlock()
	if result.Status == StatusFailed {
		fmt.Printf("Task %s failed at node %s: %v\n", exec.taskID, exec.nodeID, result.Error)
		tm.processFinalResult(exec.taskID, state)
		return
	}
	tm.resultQueue <- nodeResult{taskID: exec.taskID, nodeID: exec.nodeID, result: result}
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
	if len(node.Edges) > 0 {
		for _, edge := range node.Edges {
			tm.mu.Lock()
			if _, exists := tm.taskStates[edge.To.ID]; !exists {
				tm.taskStates[edge.To.ID] = newTaskState(edge.To.ID)
			}
			tm.mu.Unlock()
			tm.taskQueue <- taskExecution{taskID: nodeResult.taskID, nodeID: edge.To.ID, payload: nodeResult.result.Data}
		}
	} else {
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
