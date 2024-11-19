package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

var (
	Delimiter = "___"
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
	itemsTopic  storage.IMap[string, string]
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
		itemsTopic:  memory.New[string, string](),
		resultCh:    resultCh,
		taskID:      taskID,
		dag:         dag,
	}
	go tm.Run()
	go tm.WaitForResult()
	return tm
}

func (tm *TaskManager) ProcessTask(ctx context.Context, startNode string, payload json.RawMessage) {
	tm.NewState(startNode)
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

func (tm *TaskManager) GetPreviousNodes(node string) ([]*Node, error) {
	node = strings.Split(node, Delimiter)[0]
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.dag.GetPreviousNodes(node)
}

func (tm *TaskManager) GetState(node string) *TaskState {
	node = getKey(node)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.taskStates[node]
}

func (tm *TaskManager) NewState(node string) *TaskState {
	node = getKey(node)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	state := newTaskState(node)
	tm.taskStates[node] = state
	return state
}

func getKey(node string) string {
	if !strings.Contains(node, Delimiter) {
		node = fmt.Sprintf("%s%s%s", node, Delimiter, "0")
	}
	return node
}

func (tm *TaskManager) Run() {
	go func() {
		for {
			select {
			case task, ok := <-tm.taskQueue:
				if !ok {
					fmt.Println("Task queue closed")
					return
				}
				fmt.Printf("Processing task for node: %s with payload: %s\n", task.nodeID, string(task.payload))
				tm.processNode(task)
			case <-time.After(5 * time.Second):
				fmt.Println("Task queue idle for 5 seconds")
			}
		}
	}()
}

func (tm *TaskManager) processNode(exec *Task) {
	node, exists := tm.dag.GetNode(exec.nodeID)
	if !exists {
		fmt.Printf("Node %s does not exist\n", exec.nodeID)
		return
	}
	state := tm.GetState(exec.nodeID)
	if state == nil {
		state = tm.NewState(exec.nodeID)
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
	result.Status = state.Status
	if node.Type == Page {
		tm.resultCh <- result
		return
	}
	tm.resultQueue <- nodeResult{nodeID: exec.nodeID, result: result, ctx: exec.ctx, status: state.Status}
}

func (tm *TaskManager) WaitForResult() {
	go func() {
		for {
			select {
			case nr, ok := <-tm.resultQueue:
				if !ok {
					fmt.Println("Result queue closed")
					return
				}
				fmt.Printf("Processing result for node: %s with result: %v %s\n", nr.nodeID, string(nr.result.Data), nr.result.Status)
				tm.onNodeCompleted(nr)
			case <-time.After(5 * time.Second):
				fmt.Println("Result queue idle for 5 seconds")
			}
		}
	}()
}

func (tm *TaskManager) getConditionalEdges(node *Node, result Result) []Edge {
	edges := make([]Edge, len(node.Edges))
	copy(edges, node.Edges)
	if result.ConditionStatus != "" {
		if conditions, ok := tm.dag.conditions[result.Topic]; ok {
			if targetNodeKey, ok := conditions[result.ConditionStatus]; ok {
				if targetNode, ok := tm.dag.GetNode(targetNodeKey); ok {
					edges = append(edges, Edge{From: node, To: targetNode})
				}
			} else if targetNodeKey, ok = conditions["default"]; ok {
				if targetNode, ok := tm.dag.GetNode(targetNodeKey); ok {
					edges = append(edges, Edge{From: node, To: targetNode})
				}
			}
		}
	}
	return edges
}

func (tm *TaskManager) onNodeCompleted(nodeRS nodeResult) {
	node, ok := tm.dag.GetNode(nodeRS.nodeID)
	if !ok {
		return
	}
	edges := tm.getConditionalEdges(node, nodeRS.result)
	hasErrorOrCompleted := nodeRS.result.Error != nil || len(edges) == 0 || nodeRS.status == StatusFailed
	tm.checkAndAggregateResults(nodeRS, hasErrorOrCompleted)
	if hasErrorOrCompleted {
		return
	}
	for _, edge := range edges {
		if edge.Type == Simple {
			if _, ok := tm.dag.iteratorNodes.Get(edge.From.ID); ok {
				continue
			}
		}
		if state := tm.GetState(edge.To.ID); state == nil {
			tm.NewState(edge.To.ID)
		}
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
			for i, item := range items {
				suffixedNodeID := fmt.Sprintf("%s%s%d", edge.To.ID, Delimiter, i)
				ctx := context.WithValue(nodeRS.ctx, "index", suffixedNodeID)
				tm.itemsTopic.Set(suffixedNodeID, edge.To.ID)
				tm.taskQueue <- NewTask(ctx, tm.taskID, suffixedNodeID, item)
			}
		} else {
			index := "0"
			possibleIndex, exists := nodeRS.ctx.Value("index").(string)
			if exists {
				i := strings.Split(possibleIndex, Delimiter)
				if len(i) == 2 {
					index = i[1]
				}
			}
			suffixedNodeID := fmt.Sprintf("%s%s%s", edge.To.ID, Delimiter, index)
			tm.itemsTopic.Set(suffixedNodeID, edge.To.ID)
			tm.taskQueue <- NewTask(nodeRS.ctx, tm.taskID, suffixedNodeID, nodeRS.result.Data)
		}
	}
}

func (tm *TaskManager) checkAndAggregateResults(nodeRS nodeResult, hasErrorOrCompleted bool) {
	if !hasErrorOrCompleted {
		return
	}
	parentNodes, err := tm.GetPreviousNodes(nodeRS.nodeID)
	if err != nil {
		return
	}
	fmt.Println(nodeRS.nodeID)
	if len(parentNodes) == 0 {
		currentNodeState := tm.GetState(nodeRS.nodeID)
		if currentNodeState != nil {
			tm.processFinalResult(currentNodeState)
		}
		return
	}
	for _, parentNode := range parentNodes {
		state := tm.GetState(parentNode.ID)
		if state == nil {
			state = tm.NewState(parentNode.ID)
		}
		originalNodeID := nodeRS.nodeID
		if mappedID, exists := tm.itemsTopic.Get(nodeRS.nodeID); exists {
			originalNodeID = mappedID // Retrieve original node ID
		}
		state.targetResults.Set(originalNodeID, nodeRS.result)
		allTargetNodesDone := len(parentNode.Edges) == state.targetResults.Size()

		if tm.areAllTargetNodesCompleted(parentNode.ID) && allTargetNodesDone {
			parentState := tm.aggregateResults(parentNode.ID)
			tm.checkAndAggregateResults(nodeResult{
				ctx:    nodeRS.ctx,
				nodeID: parentNode.ID,
				status: nodeRS.status,
				result: parentState.Result,
			}, true)
		}
	}
}

func (tm *TaskManager) areAllTargetNodesCompleted(parentNodeID string) bool {
	parentNode, ok := tm.dag.GetNode(parentNodeID)
	if !ok {
		return false
	}
	for _, targetNode := range parentNode.Edges {
		state := tm.GetState(targetNode.To.ID)
		if state == nil || state.Status != StatusCompleted {
			return false
		}
	}
	return true
}

func (tm *TaskManager) aggregateResults(parentNodeID string) *TaskState {
	state := tm.GetState(parentNodeID)
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
	return state
}

func (tm *TaskManager) processFinalResult(state *TaskState) {
	tm.resultCh <- state.Result
	state.targetResults.Clear()
	tm.dag.finalResult(tm.taskID, state.Result)
}
