package v2

/*
import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/jet"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

type Node interface {
	ProcessTask(ctx context.Context, task *Task) Result
	GetNodeType() string
}

type Result struct {
	ConditionStatus string
	Payload         json.RawMessage
	Error           error
}

type Task struct {
	ID            string
	CurrentNodeID string
	Payload       json.RawMessage
	FinalResult   string
}

type Operation struct {
	ID      string
	Type    string
	Content string
	Func    func(task *Task) Result
}

func (n *Operation) ProcessTask(ctx context.Context, task *Task) Result {
	var data map[string]any
	if task.Payload != nil {
		err := json.Unmarshal(task.Payload, &data)
		if err != nil {
			return Result{Error: err}
		}
	}
	if data == nil {
		data = make(map[string]any)
	}
	if n.Type == "process" && n.Func != nil {
		return n.Func(task)
	}
	if n.Type == "page" && n.Content != "" {
		parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
		data["taskID"] = task.ID
		tmpl := fmt.Sprintf("%s<br><h1>Request</h1><p>{{request_data|writeJson}}</p>", n.Content)
		rs, err := parser.ParseTemplate(tmpl, map[string]any{
			"request_data": data,
			"taskID":       task.ID,
		})
		if err != nil {
			return Result{Error: err}
		}
		return Result{Payload: []byte(rs)}
	}
	return Result{Payload: task.Payload}
}

func (n *Operation) GetNodeType() string {
	return n.Type
}

type Graph struct {
	Nodes map[string]Node
	Edges map[string][]string
	Tm    *TaskManager
}

func NewGraph() *Graph {
	g := &Graph{
		Nodes: make(map[string]Node),
		Edges: make(map[string][]string),
	}
	tm := NewTaskManager(g)
	g.Tm = tm
	return g
}

func (g *Graph) Start() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		startTaskHandler(w, r, g.Tm)
	})
	http.HandleFunc("/render", func(w http.ResponseWriter, r *http.Request) {
		renderHandler(w, r, g.Tm)
	})
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		submitHandler(w, r, g.Tm)
	})

	fmt.Println("Server starting on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func (g *Graph) AddNode(nt Node) {
	switch n := nt.(type) {
	case *Operation:
		g.Nodes[n.ID] = n
	}
}

func (g *Graph) AddEdge(fromID, toID string) {
	g.Edges[fromID] = append(g.Edges[fromID], toID)
}

type TaskManager struct {
	tasks map[string]*Task
	Graph *Graph
}

func NewTaskManager(graph *Graph) *TaskManager {
	return &TaskManager{
		tasks: make(map[string]*Task),
		Graph: graph,
	}
}

func (tm *TaskManager) GetTask(taskID string) (*Task, bool) {
	task, exists := tm.tasks[taskID]
	return task, exists
}

func (tm *TaskManager) StartTask() *Task {
	taskID := generateTaskID()
	task := &Task{
		ID:            taskID,
		CurrentNodeID: "customRegistration",
	}
	tm.tasks[taskID] = task
	return task
}

func (tm *TaskManager) UpdateTask(task *Task) {
	tm.tasks[task.ID] = task
}

func (tm *TaskManager) GetNextNode(task *Task) (Node, bool) {
	if task == nil {
		return nil, false
	}
	edges, _ := tm.Graph.Edges[task.CurrentNodeID]
	if len(edges) > 0 {
		nextNodeID := edges[0]
		nextNode, exists := tm.Graph.Nodes[nextNodeID]
		if exists {
			return nextNode, true
		}
	}
	return nil, false
}

func generateTaskID() string {
	rand.Seed(time.Now().UnixNano())
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var result strings.Builder
	for i := 0; i < 8; i++ {
		result.WriteByte(charset[rand.Intn(len(charset))])
	}
	return result.String()
}

func processNode(w http.ResponseWriter, r *http.Request, task *Task, tm *TaskManager) {
	for {
		log.Printf("Processing taskID: %s, Current Node: %s", task.ID, task.CurrentNodeID)
		node, exists := tm.Graph.Nodes[task.CurrentNodeID]
		if !exists {
			http.Error(w, "Node not found", http.StatusInternalServerError)
			return
		}
		result := node.ProcessTask(context.Background(), task)
		log.Printf("Node %s processed. Result ConditionStatus: %s", task.CurrentNodeID, result.ConditionStatus)
		if node.GetNodeType() == "page" {
			fmt.Fprintf(w, string(result.Payload))
			return
		}
		if result.Error != nil {
			http.Error(w, result.Error.Error(), http.StatusInternalServerError)
			return
		}
		nextNodeID := result.ConditionStatus
		if nextNodeID == "" {
			edges := tm.Graph.Edges[task.CurrentNodeID]
			if len(edges) > 0 {
				nextNodeID = edges[0]
				log.Printf("No ConditionStatus found, following edge to next Operation: %s", nextNodeID)
			} else {
				log.Printf("Task %s completed. Final result: %s", task.ID, task.FinalResult)
				fmt.Fprintf(w, "<html><body><h1>Function Completed</h1><p>%s</p></body></html>", task.FinalResult)
				return
			}
		}
		task.CurrentNodeID = nextNodeID
		tm.UpdateTask(task)
		if nextNode, nextExists := tm.Graph.Nodes[nextNodeID]; nextExists && nextNode.GetNodeType() == "page" {
			log.Printf("Redirecting to next page: %s", nextNodeID)
			http.Redirect(w, r, fmt.Sprintf("/render?taskID=%s", task.ID), http.StatusFound)
			return
		}
	}
}

func renderHandler(w http.ResponseWriter, r *http.Request, tm *TaskManager) {
	taskID := r.URL.Query().Get("taskID")
	task, exists := tm.GetTask(taskID)
	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}
	processNode(w, r, task, tm)
}

func submitHandler(w http.ResponseWriter, r *http.Request, tm *TaskManager) {
	taskID := r.URL.Query().Get("taskID")
	task, exists := tm.GetTask(taskID)
	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	inputData := make(map[string]string)
	for key, values := range r.Form {
		if len(values) > 0 {
			inputData[key] = values[0]
		}
	}
	rawInputs, _ := json.Marshal(inputData)
	task.Payload = rawInputs
	nextNode, exists := tm.GetNextNode(task)
	if !exists {
		log.Printf("Task %s completed. Final result: %s", task.ID, task.FinalResult)
		fmt.Fprintf(w, "<html><body><h1>Function Completed</h1><p>%s</p></body></html>", task.FinalResult)
		return
	}
	switch nextNode := nextNode.(type) {
	case *Operation:
		task.CurrentNodeID = nextNode.ID
	}
	processNode(w, r, task, tm)
}

func startTaskHandler(w http.ResponseWriter, r *http.Request, tm *TaskManager) {
	task := tm.StartTask()
	http.Redirect(w, r, fmt.Sprintf("/render?taskID=%s", task.ID), http.StatusFound)
}
*/
