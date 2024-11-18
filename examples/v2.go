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
)

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
	Nodes       map[string]*Node
	Edges       map[string][]string
	ParentNodes map[string]string
	TaskStates  map[string]map[string]*TaskState
	mu          sync.Mutex
	taskQueue   chan taskExecution
	resultQueue chan nodeResult
	finalResult func(taskID string, result Result)
}

type taskExecution struct {
	taskID  string
	nodeID  string
	payload json.RawMessage
}

func NewTaskManager(finalResultCallback func(taskID string, result Result)) *TaskManager {
	tm := &TaskManager{
		Nodes:       make(map[string]*Node),
		Edges:       make(map[string][]string),
		ParentNodes: make(map[string]string),
		TaskStates:  make(map[string]map[string]*TaskState),
		taskQueue:   make(chan taskExecution, 100),
		resultQueue: make(chan nodeResult, 100),
		finalResult: finalResultCallback,
	}
	go tm.WaitForResult()
	return tm
}

func (tm *TaskManager) AddNode(nodeID string, handler func(payload json.RawMessage) Result) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.Nodes[nodeID] = &Node{ID: nodeID, Handler: handler}
}

func (tm *TaskManager) AddEdge(from string, to ...string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.Edges[from] = append(tm.Edges[from], to...)
	for _, targetNode := range to {
		tm.ParentNodes[targetNode] = from
	}
}

func (tm *TaskManager) Trigger(taskID, startNode string, payload json.RawMessage) {
	tm.mu.Lock()
	if _, exists := tm.TaskStates[taskID]; !exists {
		tm.TaskStates[taskID] = make(map[string]*TaskState)
	}
	tm.TaskStates[taskID][startNode] = &TaskState{
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
	node, exists := tm.Nodes[exec.nodeID]
	if !exists {
		fmt.Printf("Node %s does not exist\n", exec.nodeID)
		return
	}
	tm.mu.Lock()
	state := tm.TaskStates[exec.taskID][exec.nodeID]
	if state == nil {
		state = &TaskState{NodeID: exec.nodeID, Status: StatusPending, Timestamp: time.Now(), targetResults: make(map[string]Result)}
		tm.TaskStates[exec.taskID][exec.nodeID] = state
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
	nextNodes := tm.Edges[nodeResult.nodeID]
	if len(nextNodes) > 0 {
		for _, nextNodeID := range nextNodes {
			tm.mu.Lock()
			if _, exists := tm.TaskStates[nodeResult.taskID][nextNodeID]; !exists {
				tm.TaskStates[nodeResult.taskID][nextNodeID] = &TaskState{
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
		parentNode := tm.ParentNodes[nodeResult.nodeID]
		if parentNode != "" {
			tm.mu.Lock()
			state := tm.TaskStates[nodeResult.taskID][parentNode]
			if state == nil {
				state = &TaskState{NodeID: parentNode, Status: StatusPending, Timestamp: time.Now(), targetResults: make(map[string]Result)}
				tm.TaskStates[nodeResult.taskID][parentNode] = state
			}
			state.targetResults[nodeResult.nodeID] = nodeResult.result
			allTargetNodesdone := len(tm.Edges[parentNode]) == len(state.targetResults)
			tm.mu.Unlock()

			if tm.areAllTargetNodesCompleted(parentNode, nodeResult.taskID) && allTargetNodesdone {
				tm.aggregateResults(parentNode, nodeResult.taskID)
			}
		}
	}
}

func (tm *TaskManager) areAllTargetNodesCompleted(parentNode string, taskID string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, targetNode := range tm.Edges[parentNode] {
		state := tm.TaskStates[taskID][targetNode]
		if state == nil || state.Status != StatusCompleted {
			return false
		}
	}
	return true
}

func (tm *TaskManager) aggregateResults(parentNode string, taskID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	state := tm.TaskStates[taskID][parentNode]
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
	tm.finalResult(taskID, state.Result)
}

func finalResultCallback(taskID string, result Result) {
	fmt.Printf("Final result for Task %s: %s\n", taskID, string(result.Data))
}

func generateTaskID() string {
	return strconv.Itoa(rand.Intn(100000))
}

func (tm *TaskManager) formHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		http.ServeFile(w, r, "webroot/form.html")
	} else if r.Method == "POST" {
		r.ParseForm()
		email := r.FormValue("email")
		age := r.FormValue("age")
		gender := r.FormValue("gender")
		taskID := generateTaskID()
		payload := fmt.Sprintf(`{"email": "%s", "age": "%s", "gender": "%s"}`, email, age, gender)
		tm.Trigger(taskID, "NodeA", json.RawMessage(payload))
		http.Redirect(w, r, "/result?taskID="+taskID, http.StatusFound)
	}
}

func (tm *TaskManager) resultHandler(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "webroot/result.html")
}

func (tm *TaskManager) taskStatusHandler(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("taskID")
	if taskID == "" {
		http.Error(w, "taskID is missing", http.StatusBadRequest)
		return
	}
	tm.mu.Lock()
	state := tm.TaskStates[taskID]
	tm.mu.Unlock()
	if state == nil {
		http.Error(w, "Invalid taskID", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

func main() {
	tm := NewTaskManager(finalResultCallback)
	tm.AddNode("NodeA", func(payload json.RawMessage) Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return Result{Error: err, Status: StatusFailed}
		}
		data["allowed_voting"] = data["age"] == "18"
		updatedPayload, _ := json.Marshal(data)
		return Result{Data: updatedPayload, Status: StatusCompleted}
	})
	tm.AddNode("NodeB", func(payload json.RawMessage) Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return Result{Error: err, Status: StatusFailed}
		}
		data["female_voter"] = data["gender"] == "female"
		updatedPayload, _ := json.Marshal(data)
		return Result{Data: updatedPayload, Status: StatusCompleted}
	})
	tm.AddNode("NodeC", func(payload json.RawMessage) Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return Result{Error: err, Status: StatusFailed}
		}
		data["voted"] = true
		updatedPayload, _ := json.Marshal(data)
		return Result{Data: updatedPayload, Status: StatusCompleted}
	})
	tm.AddNode("Result", func(payload json.RawMessage) Result {
		var data map[string]any
		json.Unmarshal(payload, &data)

		return Result{Data: payload, Status: StatusCompleted}
	})
	tm.AddEdge("Form", "NodeA")
	tm.AddEdge("NodeA", "NodeB")
	tm.AddEdge("NodeB", "NodeC")
	tm.AddEdge("NodeC", "Result")
	http.HandleFunc("/form", tm.formHandler)
	http.HandleFunc("/result", tm.resultHandler)
	http.HandleFunc("/task-result", tm.taskStatusHandler)
	go tm.Run()
	http.ListenAndServe(":8080", nil)
}
