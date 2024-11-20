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
	parentNodes map[string]string
	childNodes  map[string]int
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
		parentNodes: make(map[string]string),
		childNodes:  make(map[string]int),
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
	tm.handleNext(exec.ctx, node, state, result)
}

func (tm *TaskManager) handleResult(ctx context.Context, node *Node, state *TaskState, result Result) {
	if state.targetResults.Size() == len(node.Edges) {
		if state.targetResults.Size() > 1 {
			aggregatedData := make([]json.RawMessage, state.targetResults.Size())
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
		} else if state.targetResults.Size() == 1 {
			state.Result = state.targetResults.Values()[0]
		}
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
	if _, hasParent := tm.parentNodes[result.Topic]; !hasParent {
		if len(node.Edges) == 0 {
			state.Status = StatusCompleted
		}
	}
	if strings.Contains(state.NodeID, tm.dag.startNode) {
		fmt.Println(string(state.Result.Data))
	}
}

func (tm *TaskManager) handlePrevious(ctx context.Context, node *Node, state *TaskState, result Result) {
	tm.handleResult(ctx, node, state, result)

	if index, ok := ctx.Value("index").(string); ok {
		childNode := fmt.Sprintf("%s%s%s", node.ID, Delimiter, index)
		tm.mu.Lock()
		pn, ok := tm.parentNodes[childNode]
		tm.mu.Unlock()
		if !ok {
			childNode = fmt.Sprintf("%s%s%s", node.ID, Delimiter, "0")
			pn, ok = tm.parentNodes[childNode]
		}
		if ok {
			parentState, _ := tm.taskStates[pn]
			if parentState != nil {
				result.Topic = childNode
				_, hasParent := tm.parentNodes[pn]
				if !hasParent {
					parentState.targetResults.Set(childNode, state.Result)
				} else {
					parentState.targetResults.Set(childNode, result)
				}
				pn = strings.Split(pn, Delimiter)[0]
				parentNode, _ := tm.dag.nodes.Get(pn)
				tm.handlePrevious(ctx, parentNode, parentState, result)
			}
		}
	}
}

func (tm *TaskManager) handleNext(ctx context.Context, node *Node, state *TaskState, result Result) {
	tm.handleResult(ctx, node, state, result)
	if node.Type == Page {
		tm.resultCh <- result
		return
	}
	if result.Status == "" {
		result.Status = state.Status
	}
	tm.resultQueue <- nodeResult{nodeID: node.ID, result: result, ctx: ctx, status: state.Status}
}

func (tm *TaskManager) onNodeCompleted(nodeRS nodeResult) {
	node, ok := tm.dag.nodes.Get(nodeRS.nodeID)
	if !ok {
		return
	}
	edges := tm.getConditionalEdges(node, nodeRS.result)
	hasErrorOrCompleted := nodeRS.result.Error != nil || len(edges) == 0 || nodeRS.status == StatusFailed
	if hasErrorOrCompleted {
		if index, ok := nodeRS.ctx.Value("index").(string); ok {
			childNode := fmt.Sprintf("%s%s%s", node.ID, Delimiter, index)
			tm.mu.Lock()
			pn, ok := tm.parentNodes[childNode]
			tm.mu.Unlock()
			if ok {
				parentState, _ := tm.taskStates[pn]
				if parentState != nil {
					pn = strings.Split(pn, Delimiter)[0]
					parentNode, _ := tm.dag.nodes.Get(pn)
					parentState.Status = nodeRS.status
					parentState.Result.Status = nodeRS.status
					nodeRS.nodeID = pn
					nodeRS.result.Topic = pn
					nodeRS.result.Status = parentState.Status
					tm.handlePrevious(nodeRS.ctx, parentNode, parentState, nodeRS.result)
				}
			}
		}
		return
	}
	tm.handleEdges(nodeRS, edges)
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
			tm.mu.Lock()
			tm.childNodes[parentNode] = len(items)
			tm.mu.Unlock()
			for i, item := range items {
				childNode := fmt.Sprintf("%s%s%d", edge.To.ID, Delimiter, i)
				ctx := context.WithValue(currentResult.ctx, "index", fmt.Sprintf("%d", i))
				tm.mu.Lock()
				tm.parentNodes[childNode] = parentNode
				tm.mu.Unlock()
				tm.send(ctx, edge.To.ID, tm.taskID, item)
			}
		} else {
			tm.mu.Lock()
			tm.childNodes[parentNode] = 1
			tm.mu.Unlock()
			index, ok := currentResult.ctx.Value("index").(string)
			if !ok {
				index = "0"
			}
			childNode := fmt.Sprintf("%s%s%s", edge.To.ID, Delimiter, index)
			ctx := context.WithValue(currentResult.ctx, "index", index)
			tm.mu.Lock()
			tm.parentNodes[childNode] = parentNode
			tm.mu.Unlock()
			tm.send(ctx, edge.To.ID, tm.taskID, currentResult.result.Data)
		}
	}
}

func (tm *TaskManager) processFinalResult(state *TaskState) {
	state.targetResults.Clear()
	tm.dag.finalResult(tm.taskID, state.Result)
}
