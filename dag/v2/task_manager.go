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
	nodeID string
	status TaskStatus
	result Result
}

type TaskManager struct {
	taskStates  map[string]*TaskState
	currentNode string
	dag         *DAG
	taskID      string
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

func NewTaskManager(dag *DAG, taskID string, resultCh chan Result) *TaskManager {
	tm := &TaskManager{
		taskStates:  make(map[string]*TaskState),
		taskQueue:   make(chan *Task, 100),
		resultQueue: make(chan nodeResult, 100),
		resultCh:    resultCh,
		taskID:      taskID,
		dag:         dag,
	}
	go tm.Run()
	go tm.WaitForResult()
	return tm
}

func (tm *TaskManager) ProcessTask(ctx context.Context, startNode string, payload json.RawMessage) {
	tm.mu.Lock()
	tm.taskStates[startNode] = newTaskState(startNode)
	tm.mu.Unlock()
	tm.taskQueue <- NewTask(ctx, tm.taskID, startNode, payload)
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
	result.Topic = node.ID
	state.UpdatedAt = time.Now()
	state.Result = result
	if result.Ctx == nil {
		result.Ctx = exec.ctx
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
	tm.resultQueue <- nodeResult{nodeID: exec.nodeID, result: result, ctx: exec.ctx, status: state.Status}
}

func (tm *TaskManager) WaitForResult() {
	go func() {
		for nr := range tm.resultQueue {
			tm.onNodeCompleted(nr)
		}
	}()
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

func (tm *TaskManager) onNodeCompleted(nodeRS nodeResult) {
	node, ok := tm.dag.nodes.Get(nodeRS.nodeID)
	if !ok {
		return
	}
	edges := tm.getConditionalEdges(node, nodeRS.result)
	if nodeRS.result.Error != nil || len(edges) == 0 || nodeRS.status == StatusFailed {
		parentNodes, err := tm.dag.GetPreviousNodes(nodeRS.nodeID)
		if err == nil {
			for _, parentNode := range parentNodes {
				tm.mu.Lock()
				state := tm.taskStates[parentNode.ID]
				if state == nil {
					state = newTaskState(parentNode.ID)
					tm.taskStates[parentNode.ID] = state
				}
				state.targetResults.Set(nodeRS.nodeID, nodeRS.result)
				allTargetNodesDone := len(parentNode.Edges) == state.targetResults.Size()
				tm.mu.Unlock()
				if tm.areAllTargetNodesCompleted(parentNode.ID) && allTargetNodesDone {
					tm.aggregateResults(parentNode.ID)
				}
			}
		}
		return
	}
	for _, edge := range edges {
		if edge.Type == Simple {
			if _, ok := tm.dag.iteratorNodes.Get(edge.From.ID); ok {
				continue
			}
		}
		tm.mu.Lock()
		if _, exists := tm.taskStates[edge.To.ID]; !exists {
			tm.taskStates[edge.To.ID] = newTaskState(edge.To.ID)
		}
		tm.mu.Unlock()
		if edge.Type == Iterator {
			var items []json.RawMessage
			err := json.Unmarshal(nodeRS.result.Data, &items)
			if err != nil {
				tm.resultQueue <- nodeResult{
					ctx:    nodeRS.ctx,
					nodeID: edge.To.ID,
					status: StatusFailed,
					result: Result{Error: err},
				}
				return
			}
			for _, item := range items {
				tm.taskQueue <- NewTask(nodeRS.ctx, tm.taskID, edge.To.ID, item)
			}
		} else {
			tm.taskQueue <- NewTask(nodeRS.ctx, tm.taskID, edge.To.ID, nodeRS.result.Data)
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

func (tm *TaskManager) aggregateResults(parentNode string) {
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
	tm.resultCh <- state.Result
	tm.processFinalResult(state)
}

func (tm *TaskManager) processFinalResult(state *TaskState) {
	state.targetResults.Clear()
	tm.dag.finalResult(tm.taskID, state.Result)
}
