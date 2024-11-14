package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const (
	PageType     = "page"
	FunctionType = "function"
)

type Node interface {
	ProcessTask(ctx context.Context, task *Task) Result
	GetNodeType() string
}

type Result struct {
	NextNodeID string
	Message    string
	Error      error
}

type Task struct {
	ID            string
	CurrentNodeID string
	Inputs        map[string]string
	FinalResult   string
}

type PageNode struct {
	ID      string
	Content string
}

func (n *PageNode) ProcessTask(ctx context.Context, task *Task) Result {
	return Result{Message: n.Content}
}

func (n *PageNode) GetNodeType() string {
	return PageType
}

type FunctionNode struct {
	ID   string
	Func func(task *Task) Result
}

func (n *FunctionNode) ProcessTask(ctx context.Context, task *Task) Result {
	return n.Func(task)
}

func (n *FunctionNode) GetNodeType() string {
	return FunctionType
}

type Graph struct {
	Nodes map[string]Node
	Edges map[string][]string
}

func NewGraph() *Graph {
	return &Graph{
		Nodes: make(map[string]Node),
		Edges: make(map[string][]string),
	}
}

func (g *Graph) AddNode(node Node) {
	switch n := node.(type) {
	case *PageNode:
		g.Nodes[n.ID] = n
	case *FunctionNode:
		g.Nodes[n.ID] = n
	}
}

func (g *Graph) AddEdge(fromID, toID string) {
	g.Edges[fromID] = append(g.Edges[fromID], toID)
}

// TaskManager to handle task state and transitions
type TaskManager struct {
	tasks map[string]*Task
	graph *Graph
}

func NewTaskManager(graph *Graph) *TaskManager {
	return &TaskManager{
		tasks: make(map[string]*Task),
		graph: graph,
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
		CurrentNodeID: "login", // starting point
		Inputs:        make(map[string]string),
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
	edges, _ := tm.graph.Edges[task.CurrentNodeID]
	if len(edges) > 0 {
		nextNodeID := edges[0]
		nextNode, exists := tm.graph.Nodes[nextNodeID]
		if exists {
			return nextNode, true
		}
	}
	return nil, false
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
	task.Inputs = make(map[string]string)
	for key, values := range r.Form {
		if len(values) > 0 {
			task.Inputs[key] = values[0]
		}
	} // Fetch next nodes from the graph based on the current node
	nextNodes, exists := tm.GetNextNode(task)
	if !exists {
		log.Printf("Task %s completed. Final result: %s", task.ID, task.FinalResult)
		fmt.Fprintf(w, "<html><body><h1>Process Completed</h1><p>%s</p></body></html>", task.FinalResult)
		return
	}

	switch nextNodes := nextNodes.(type) {
	case *PageNode:
		task.CurrentNodeID = nextNodes.ID
	case *FunctionNode:
		task.CurrentNodeID = nextNodes.ID
	}
	fmt.Println(task.CurrentNodeID)
	processNode(w, r, task, tm)
}

func startTaskHandler(w http.ResponseWriter, r *http.Request, tm *TaskManager) {
	task := tm.StartTask()
	http.Redirect(w, r, fmt.Sprintf("/render?taskID=%s", task.ID), http.StatusFound)
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
		node, exists := tm.graph.Nodes[task.CurrentNodeID]
		if !exists {
			http.Error(w, "Node not found", http.StatusInternalServerError)
			return
		}
		result := node.ProcessTask(context.Background(), task)
		log.Printf("Node %s processed. Next NodeID: %s", task.CurrentNodeID, result.NextNodeID)

		if node.GetNodeType() == PageType {
			contentWithTaskID := strings.ReplaceAll(result.Message, "{{taskID}}", task.ID)
			fmt.Fprintf(w, contentWithTaskID)
			return
		}

		if result.Error != nil {
			http.Error(w, result.Error.Error(), http.StatusInternalServerError)
			return
		}

		if result.NextNodeID == "" {
			log.Printf("Task %s completed. Final result: %s", task.ID, task.FinalResult)
			fmt.Fprintf(w, "<html><body><h1>Process Completed</h1><p>%s</p></body></html>", task.FinalResult)
			return
		}

		task.CurrentNodeID = result.NextNodeID
		tm.UpdateTask(task)

		if nextNode, nextExists := tm.graph.Nodes[result.NextNodeID]; nextExists && nextNode.GetNodeType() == PageType {
			log.Printf("Redirecting to next page: %s", result.NextNodeID)
			http.Redirect(w, r, fmt.Sprintf("/render?taskID=%s", task.ID), http.StatusFound)
			return
		}
	}
}

func main() {
	graph := NewGraph()
	tm := NewTaskManager(graph)

	loginNode := &PageNode{
		ID:      "login",
		Content: `<html><body><h1>Login</h1><form method="POST" action="/submit?taskID={{taskID}}"><label>Username: </label><input type="text" name="username"><br><label>Password: </label><input type="password" name="password"><br><input type="submit" value="Login"></form>{{error}}</body></html>`,
	}
	checkCredentialsNode := &FunctionNode{
		ID: "checkCredentials",
		Func: func(task *Task) Result {
			username, password := task.Inputs["username"], task.Inputs["password"]
			if username == "user" && password == "password123" {
				task.FinalResult = "Login Successful"
				return Result{NextNodeID: "dashboard"}
			}
			return Result{NextNodeID: "login", Message: "Invalid credentials. Please try again."}
		},
	}
	dashboardNode := &PageNode{
		ID:      "dashboard",
		Content: `<html><body><h1>Dashboard</h1><p>Welcome to the Dashboard. Login Successful!</p></body></html>`,
	}

	graph.AddNode(loginNode)
	graph.AddNode(checkCredentialsNode)
	graph.AddNode(dashboardNode)
	graph.AddEdge("login", "checkCredentials")
	graph.AddEdge("checkCredentials", "login")
	graph.AddEdge("checkCredentials", "dashboard")

	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		startTaskHandler(w, r, tm)
	})
	http.HandleFunc("/render", func(w http.ResponseWriter, r *http.Request) {
		renderHandler(w, r, tm)
	})
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		submitHandler(w, r, tm)
	})

	fmt.Println("Server starting on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
