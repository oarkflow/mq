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
	ConditionStatus string
	Message         string
	Error           error
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
		CurrentNodeID: "customRegistration",
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

		// Retrieve the current node
		node, exists := tm.graph.Nodes[task.CurrentNodeID]
		if !exists {
			http.Error(w, "Node not found", http.StatusInternalServerError)
			return
		}

		// Process the task with the current node
		result := node.ProcessTask(context.Background(), task)
		log.Printf("Node %s processed. Result ConditionStatus: %s", task.CurrentNodeID, result.ConditionStatus)

		// If the node type is PageType, render the content with the task ID and return
		if node.GetNodeType() == PageType {
			contentWithTaskID := strings.ReplaceAll(result.Message, "{{taskID}}", task.ID)
			fmt.Fprintf(w, contentWithTaskID)
			return
		}

		// Handle any processing error
		if result.Error != nil {
			http.Error(w, result.Error.Error(), http.StatusInternalServerError)
			return
		}

		// Determine the next node ID
		nextNodeID := result.ConditionStatus
		if nextNodeID == "" {
			// If ConditionStatus is empty, try to find the next node in the DAG's edges
			edges := tm.graph.Edges[task.CurrentNodeID]
			if len(edges) > 0 {
				nextNodeID = edges[0] // Get the first edge's destination as the next node
				log.Printf("No ConditionStatus found, following edge to next node: %s", nextNodeID)
			} else {
				// If no edges are found, the process is complete
				log.Printf("Task %s completed. Final result: %s", task.ID, task.FinalResult)
				fmt.Fprintf(w, "<html><body><h1>Process Completed</h1><p>%s</p></body></html>", task.FinalResult)
				return
			}
		}

		// Update the task with the new node ID and persist the task state
		task.CurrentNodeID = nextNodeID
		tm.UpdateTask(task)

		// Check if the next node is a PageType and handle redirection if necessary
		if nextNode, nextExists := tm.graph.Nodes[nextNodeID]; nextExists && nextNode.GetNodeType() == PageType {
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
	task.Inputs = make(map[string]string)
	for key, values := range r.Form {
		if len(values) > 0 {
			task.Inputs[key] = values[0]
		}
	}
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

func isValidEmail(email string) bool {
	return true
}

func isValidPhone(phone string) bool {
	return true
}

func verifyHandler(w http.ResponseWriter, r *http.Request, tm *TaskManager) {
	taskID := r.URL.Query().Get("taskID")
	if taskID == "" {
		http.Error(w, "Missing taskID", http.StatusBadRequest)
		return
	}
	task, exists := tm.GetTask(taskID)
	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}
	task.Inputs["email_verified"] = "true"
	log.Printf("Email for taskID %s successfully verified.", task.ID)
	nextNode, exists := tm.graph.Nodes["dashboard"]
	if !exists {
		http.Error(w, "Dashboard node not found", http.StatusInternalServerError)
		return
	}
	result := nextNode.ProcessTask(context.Background(), task)
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}
	contentWithTaskID := strings.ReplaceAll(result.Message, "{{taskID}}", task.ID)
	fmt.Fprintf(w, contentWithTaskID)
}

func main() {
	graph := NewGraph()
	tm := NewTaskManager(graph)
	customRegistrationNode := &PageNode{
		ID:      "customRegistration",
		Content: `<html><body><h1>Custom Registration</h1><form method="POST" action="/submit?taskID={{taskID}}"><label>Email: </label><input type="text" name="email"><br><label>Phone: </label><input type="text" name="phone"><br><label>City: </label><input type="text" name="city"><br><input type="submit" value="Submit"></form></body></html>`,
	}
	checkValidityNode := &FunctionNode{
		ID: "checkValidity",
		Func: func(task *Task) Result {
			email, phone := task.Inputs["email"], task.Inputs["phone"]
			if !isValidEmail(email) || !isValidPhone(phone) {
				return Result{
					ConditionStatus: "customRegistration",
					Message:         "Invalid email or phone number. Please try again.",
				}
			}
			return Result{ConditionStatus: "checkManualVerification"}
		},
	}
	checkManualVerificationNode := &FunctionNode{
		ID: "checkManualVerification",
		Func: func(task *Task) Result {
			city := task.Inputs["city"]
			if city != "Kathmandu" {
				return Result{ConditionStatus: "manualVerificationPage"}
			}
			return Result{ConditionStatus: "approveCustomer"}
		},
	}
	approveCustomerNode := &FunctionNode{
		ID: "approveCustomer",
		Func: func(task *Task) Result {
			task.FinalResult = "Customer approved"
			return Result{}
		},
	}
	sendVerificationEmailNode := &FunctionNode{
		ID: "sendVerificationEmail",
		Func: func(task *Task) Result {
			return Result{}
		},
	}
	verificationLinkPageNode := &PageNode{
		ID:      "verificationLinkPage",
		Content: `<html><body><h1>Verify Email</h1><p>Click here to verify your email</p><a href="/verify?taskID={{taskID}}">Verify</a></body></html>`,
	}
	dashboardNode := &PageNode{
		ID:      "dashboard",
		Content: `<html><body><h1>Dashboard</h1><p>Welcome to your dashboard!</p></body></html>`,
	}
	manualVerificationNode := &PageNode{
		ID:      "manualVerificationPage",
		Content: `<html><body><h1>Manual Verification</h1><p>Please verify the user's information manually.</p><form method="POST" action="/verify?taskID={{taskID}}"><input type="submit" value="Approve"></form></body></html>`,
	}
	verifyApprovedNode := &FunctionNode{
		ID: "verifyApproved",
		Func: func(task *Task) Result {
			return Result{}
		},
	}
	denyVerificationNode := &FunctionNode{
		ID: "denyVerification",
		Func: func(task *Task) Result {
			task.FinalResult = "Verification Denied"
			return Result{}
		},
	}

	graph.AddNode(customRegistrationNode)
	graph.AddNode(checkValidityNode)
	graph.AddNode(checkManualVerificationNode)
	graph.AddNode(approveCustomerNode)
	graph.AddNode(sendVerificationEmailNode)
	graph.AddNode(verificationLinkPageNode)
	graph.AddNode(dashboardNode)
	graph.AddNode(manualVerificationNode)
	graph.AddNode(verifyApprovedNode)
	graph.AddNode(denyVerificationNode)

	graph.AddEdge("customRegistration", "checkValidity")
	graph.AddEdge("checkValidity", "checkManualVerification")
	graph.AddEdge("checkManualVerification", "approveCustomer")
	graph.AddEdge("checkManualVerification", "manualVerificationPage")
	graph.AddEdge("approveCustomer", "sendVerificationEmail")
	graph.AddEdge("sendVerificationEmail", "verificationLinkPage")
	graph.AddEdge("verificationLinkPage", "dashboard")
	graph.AddEdge("manualVerificationPage", "verifyApproved")
	graph.AddEdge("manualVerificationPage", "denyVerification")
	graph.AddEdge("verifyApproved", "approveCustomer")
	graph.AddEdge("denyVerification", "verificationLinkPage")

	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		startTaskHandler(w, r, tm)
	})
	http.HandleFunc("/render", func(w http.ResponseWriter, r *http.Request) {
		renderHandler(w, r, tm)
	})
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		submitHandler(w, r, tm)
	})
	http.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		verifyHandler(w, r, tm)
	})

	fmt.Println("Server starting on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
