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

var Delimiter = "___"

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
	taskStates  map[string]*TaskState
	parentNodes storage.IMap[string, string]
	childNodes  storage.IMap[string, int]
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
		parentNodes: memory.New[string, string](),
		childNodes:  memory.New[string, int](),
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
	tm.send(ctx, startNode, tm.taskID, payload)
}

func (tm *TaskManager) send(ctx context.Context, startNode, taskID string, payload json.RawMessage) {
	if index, ok := ctx.Value("index").(string); ok {
		startNode = strings.Split(startNode, Delimiter)[0]
		startNode = fmt.Sprintf("%s%s%s", startNode, Delimiter, index)
	}

	tm.mu.Lock()
	tm.taskStates[startNode] = newTaskState(startNode)
	tm.mu.Unlock()
	tm.taskQueue <- NewTask(ctx, taskID, startNode, payload)
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
				tm.processNode(task)
			}
		}
	}()
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
				fmt.Printf("Processing result for node: %s %v %s\n", nr.nodeID, string(nr.result.Data), nr.status)
				tm.onNodeCompleted(nr)
			}
		}
	}()
}

func (tm *TaskManager) processNode(exec *Task) {
	pureNodeID := strings.Split(exec.nodeID, Delimiter)[0]
	node, exists := tm.dag.nodes.Get(pureNodeID)
	if !exists {
		fmt.Printf("Node %s does not exist\n", pureNodeID)
		return
	}
	tm.mu.Lock()
	state := tm.taskStates[exec.nodeID]
	if state == nil {
		state = newTaskState(exec.nodeID)
		tm.taskStates[exec.nodeID] = state
	}
	tm.mu.Unlock()
	state.Status = StatusProcessing
	state.UpdatedAt = time.Now()
	tm.currentNode = exec.nodeID
	result := node.Handler(exec.ctx, exec.payload)
	result.Topic = node.ID
	if result.Error != nil {
		tm.resultCh <- result
		tm.processFinalResult(state)
		return
	}
	if node.Type == Page {
		tm.resultCh <- state.Result
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
	fmt.Printf("Processing result for node: %s %v %s\n", state.NodeID, string(state.Result.Data), state.Status)
	pn, ok := tm.parentNodes.Get(state.NodeID)
	if edges, exists := tm.dag.iteratorNodes.Get(nodeID[0]); exists && state.Status == StatusCompleted {
		state.Status = StatusProcessing
		tm.dag.iteratorNodes.Del(nodeID[0])
		state.targetResults.Clear()
		if len(nodeID) == 2 {
			ctx = context.WithValue(ctx, "index", nodeID[1])
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
			parentState, _ := tm.taskStates[pn]
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
	tm.resultQueue <- nodeResult{nodeID: state.NodeID, result: result, ctx: ctx, status: state.Status}
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
		if index, ok := rs.ctx.Value("index").(string); ok {
			childNode := fmt.Sprintf("%s%s%s", node.ID, Delimiter, index)
			pn, ok := tm.parentNodes.Get(childNode)
			if ok {
				parentState, _ := tm.taskStates[pn]
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
		index, ok := currentResult.ctx.Value("index").(string)
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
				ctx := context.WithValue(currentResult.ctx, "index", fmt.Sprintf("%d", i))
				tm.parentNodes.Set(childNode, parentNode)
				tm.send(ctx, edge.To.ID, tm.taskID, item)
			}
		} else {
			tm.childNodes.Set(parentNode, 1)
			index, ok := currentResult.ctx.Value("index").(string)
			if !ok {
				index = "0"
			}
			childNode := fmt.Sprintf("%s%s%s", edge.To.ID, Delimiter, index)
			ctx := context.WithValue(currentResult.ctx, "index", index)
			tm.parentNodes.Set(childNode, parentNode)
			tm.send(ctx, edge.To.ID, tm.taskID, currentResult.result.Data)
		}
	}
}

func (tm *TaskManager) processFinalResult(state *TaskState) {
	state.targetResults.Clear()
	tm.dag.finalResult(tm.taskID, state.Result)
}
