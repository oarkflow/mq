package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

type DAG struct {
	Nodes       map[string]*Node
	Edges       map[string][]string
	ParentNodes map[string]string
	taskManager storage.IMap[string, *TaskManager]
	finalResult func(taskID string, result Result)
}

func NewDAG(finalResultCallback func(taskID string, result Result)) *DAG {
	return &DAG{
		Nodes:       make(map[string]*Node),
		Edges:       make(map[string][]string),
		ParentNodes: make(map[string]string),
		taskManager: memory.New[string, *TaskManager](),
		finalResult: finalResultCallback,
	}
}

func (tm *DAG) AddNode(nodeID string, handler func(payload json.RawMessage) Result) {
	tm.Nodes[nodeID] = &Node{ID: nodeID, Handler: handler}
}

func (tm *DAG) AddEdge(from string, to ...string) {
	tm.Edges[from] = append(tm.Edges[from], to...)
	for _, targetNode := range to {
		tm.ParentNodes[targetNode] = from
	}
}

type TaskStatus string

const (
	StatusPending    TaskStatus = "Pending"
	StatusProcessing TaskStatus = "Processing"
	StatusCompleted  TaskStatus = "Completed"
	StatusFailed     TaskStatus = "Failed"
)

type Result struct {
	Data   json.RawMessage
	Error  error
	Status TaskStatus
}

type Node struct {
	ID      string
	Handler func(payload json.RawMessage) Result
}

type TaskState struct {
	NodeID        string
	Status        TaskStatus
	Timestamp     time.Time
	Result        Result
	targetResults map[string]Result
	my            sync.Mutex
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
	go tm.WaitForResult()
	return tm
}

func (tm *TaskManager) Trigger(taskID, startNode string, payload json.RawMessage) {
	tm.mu.Lock()
	tm.taskStates[startNode] = &TaskState{
		NodeID:        startNode,
		Status:        StatusPending,
		Timestamp:     time.Now(),
		targetResults: make(map[string]Result),
	}
	tm.mu.Unlock()
	tm.taskQueue <- taskExecution{taskID: taskID, nodeID: startNode, payload: payload}
}

func (tm *TaskManager) Run() {
	go func() {
		for task := range tm.taskQueue {
			tm.processNode(task)
		}
	}()
}

func (tm *TaskManager) processNode(exec taskExecution) {
	node, exists := tm.dag.Nodes[exec.nodeID]
	if !exists {
		fmt.Printf("Node %s does not exist\n", exec.nodeID)
		return
	}
	tm.mu.Lock()
	state := tm.taskStates[exec.nodeID]
	if state == nil {
		state = &TaskState{NodeID: exec.nodeID, Status: StatusPending, Timestamp: time.Now(), targetResults: make(map[string]Result)}
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
	nextNodes := tm.dag.Edges[nodeResult.nodeID]
	if len(nextNodes) > 0 {
		for _, nextNodeID := range nextNodes {
			tm.mu.Lock()
			if _, exists := tm.taskStates[nextNodeID]; !exists {
				tm.taskStates[nextNodeID] = &TaskState{
					NodeID:        nextNodeID,
					Status:        StatusPending,
					Timestamp:     time.Now(),
					targetResults: make(map[string]Result),
				}
			}
			tm.mu.Unlock()
			tm.taskQueue <- taskExecution{taskID: nodeResult.taskID, nodeID: nextNodeID, payload: nodeResult.result.Data}
		}
	} else {
		parentNode := tm.dag.ParentNodes[nodeResult.nodeID]
		if parentNode != "" {
			tm.mu.Lock()
			state := tm.taskStates[parentNode]
			if state == nil {
				state = &TaskState{NodeID: parentNode, Status: StatusPending, Timestamp: time.Now(), targetResults: make(map[string]Result)}
				tm.taskStates[parentNode] = state
			}
			state.targetResults[nodeResult.nodeID] = nodeResult.result
			allTargetNodesdone := len(tm.dag.Edges[parentNode]) == len(state.targetResults)
			tm.mu.Unlock()

			if tm.areAllTargetNodesCompleted(parentNode) && allTargetNodesdone {
				tm.aggregateResults(parentNode, nodeResult.taskID)
			}
		}
	}
}

func (tm *TaskManager) areAllTargetNodesCompleted(parentNode string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, targetNode := range tm.dag.Edges[parentNode] {
		state := tm.taskStates[targetNode]
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
	if len(state.targetResults) > 1 {
		aggregatedData := make([]json.RawMessage, len(state.targetResults))
		i := 0
		for _, result := range state.targetResults {
			aggregatedData[i] = result.Data
			i++
		}
		aggregatedPayload, _ := json.Marshal(aggregatedData)
		state.Result = Result{Data: aggregatedPayload, Status: StatusCompleted}
	} else if len(state.targetResults) == 1 {
		state.Result = maps.Values(state.targetResults)[0]
	}
	tm.processFinalResult(taskID, state)
}

func (tm *TaskManager) processFinalResult(taskID string, state *TaskState) {
	clear(state.targetResults)
	tm.dag.finalResult(taskID, state.Result)
}

func finalResultCallback(taskID string, result Result) {
	fmt.Printf("Final result for Task %s: %s\n", taskID, string(result.Data))
}

func generateTaskID() string {
	return strconv.Itoa(rand.Intn(100000))
}

func (tm *DAG) formHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		http.ServeFile(w, r, "webroot/form.html")
	} else if r.Method == "POST" {
		r.ParseForm()
		email := r.FormValue("email")
		age := r.FormValue("age")
		gender := r.FormValue("gender")
		taskID := generateTaskID()
		manager := NewTaskManager(tm)
		tm.taskManager.Set(taskID, manager)
		go manager.Run()
		payload := fmt.Sprintf(`{"email": "%s", "age": "%s", "gender": "%s"}`, email, age, gender)
		manager.Trigger(taskID, "NodeA", json.RawMessage(payload))
		http.Redirect(w, r, "/result?taskID="+taskID, http.StatusFound)
	}
}

func (tm *DAG) resultHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "webroot/result.html")
}

func (tm *DAG) taskStatusHandler(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("taskID")
	if taskID == "" {
		http.Error(w, `{"message": "taskID is missing"}`, http.StatusBadRequest)
		return
	}
	manager, ok := tm.taskManager.Get(taskID)
	if !ok {
		http.Error(w, `{"message": "Invalid TaskID"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(manager.taskStates)
}

func main() {
	dag := NewDAG(finalResultCallback)
	dag.AddNode("NodeA", func(payload json.RawMessage) Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return Result{Error: err, Status: StatusFailed}
		}
		data["allowed_voting"] = data["age"] == "18"
		updatedPayload, _ := json.Marshal(data)
		return Result{Data: updatedPayload, Status: StatusCompleted}
	})
	dag.AddNode("NodeB", func(payload json.RawMessage) Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return Result{Error: err, Status: StatusFailed}
		}
		data["female_voter"] = data["gender"] == "female"
		updatedPayload, _ := json.Marshal(data)
		return Result{Data: updatedPayload, Status: StatusCompleted}
	})
	dag.AddNode("NodeC", func(payload json.RawMessage) Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return Result{Error: err, Status: StatusFailed}
		}
		data["voted"] = true
		updatedPayload, _ := json.Marshal(data)
		return Result{Data: updatedPayload, Status: StatusCompleted}
	})
	dag.AddNode("Result", func(payload json.RawMessage) Result {
		var data map[string]any
		json.Unmarshal(payload, &data)

		return Result{Data: payload, Status: StatusCompleted}
	})
	dag.AddEdge("Form", "NodeA")
	dag.AddEdge("NodeA", "NodeB")
	dag.AddEdge("NodeB", "NodeC")
	dag.AddEdge("NodeC", "Result")
	http.HandleFunc("/form", dag.formHandler)
	http.HandleFunc("/result", dag.resultHandler)
	http.HandleFunc("/task-result", dag.taskStatusHandler)
	http.ListenAndServe(":8080", nil)
}
