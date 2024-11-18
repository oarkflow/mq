package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "Pending"
	StatusProcessing TaskStatus = "Processing"
	StatusCompleted  TaskStatus = "Completed"
	StatusFailed     TaskStatus = "Failed"
)

type Result struct {
	Ctx    context.Context
	Data   json.RawMessage
	Error  error
	Status TaskStatus
}

type NodeType int

func (c NodeType) IsValid() bool { return c >= Process && c <= Page }

const (
	Process NodeType = iota
	Page
)

type Node struct {
	Type    NodeType
	ID      string
	Handler func(ctx context.Context, payload json.RawMessage) Result
	Edges   []Edge
}

type EdgeType int

func (c EdgeType) IsValid() bool { return c >= Simple && c <= Iterator }

const (
	Simple EdgeType = iota
	Iterator
)

type Edge struct {
	From *Node
	To   *Node
	Type EdgeType
}

type DAG struct {
	nodes       storage.IMap[string, *Node]
	taskManager storage.IMap[string, *TaskManager]
	finalResult func(taskID string, result Result)
	mu          sync.Mutex
	Error       error
	startNode   string
}

func NewDAG(finalResultCallback func(taskID string, result Result)) *DAG {
	return &DAG{
		nodes:       memory.New[string, *Node](),
		taskManager: memory.New[string, *TaskManager](),
		finalResult: finalResultCallback,
	}
}

func (tm *DAG) Validate(ctx context.Context) (string, error) {
	return tm.parseInitialNode(ctx)
}

func (tm *DAG) parseInitialNode(ctx context.Context) (string, error) {
	val := ctx.Value("initial_node")
	initialNode, ok := val.(string)
	if ok {
		return initialNode, nil
	}
	if tm.startNode == "" {
		firstNode := tm.findStartNode()
		if firstNode != nil {
			tm.startNode = firstNode.ID
		}
	}

	if tm.startNode == "" {
		return "", fmt.Errorf("initial node not found")
	}
	return tm.startNode, nil
}

func (tm *DAG) findStartNode() *Node {
	incomingEdges := make(map[string]bool)
	connectedNodes := make(map[string]bool)
	for _, node := range tm.nodes.AsMap() {
		for _, edge := range node.Edges {
			if edge.Type.IsValid() {
				connectedNodes[node.ID] = true
				connectedNodes[edge.To.ID] = true
				incomingEdges[edge.To.ID] = true
			}
		}
	}
	for nodeID, node := range tm.nodes.AsMap() {
		if !incomingEdges[nodeID] && connectedNodes[nodeID] {
			return node
		}
	}
	return nil
}

func (tm *DAG) AddNode(nodeType NodeType, nodeID string, handler func(ctx context.Context, payload json.RawMessage) Result) *DAG {
	if tm.Error != nil {
		return tm
	}
	tm.nodes.Set(nodeID, &Node{ID: nodeID, Handler: handler, Type: nodeType})
	return tm
}

func (tm *DAG) AddEdge(from string, targets ...string) *DAG {
	if tm.Error != nil {
		return tm
	}
	node, ok := tm.nodes.Get(from)
	if !ok {
		tm.Error = fmt.Errorf("node not found %s", from)
		return tm
	}
	for _, target := range targets {
		if targetNode, ok := tm.nodes.Get(target); ok {
			edge := Edge{From: node, To: targetNode, Type: Simple}
			node.Edges = append(node.Edges, edge)
		}
	}
	return tm
}

func (tm *DAG) GetNextNodes(key string) ([]*Node, error) {
	node, exists := tm.nodes.Get(key)
	if !exists {
		return nil, fmt.Errorf("node with key %s does not exist", key)
	}
	var successors []*Node
	for _, edge := range node.Edges {
		successors = append(successors, edge.To)
	}
	return successors, nil
}

func (tm *DAG) GetPreviousNodes(key string) ([]*Node, error) {
	var predecessors []*Node
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		for _, target := range node.Edges {
			if target.To.ID == key {
				predecessors = append(predecessors, node)
			}
		}
		return true
	})
	return predecessors, nil
}

func (tm *DAG) formHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		http.ServeFile(w, r, "webroot/form.html")
	} else if r.Method == "POST" {
		r.ParseForm()
		email := r.FormValue("email")
		age := r.FormValue("age")
		gender := r.FormValue("gender")
		taskID := mq.NewID()
		manager := NewTaskManager(tm)
		tm.taskManager.Set(taskID, manager)
		payload := fmt.Sprintf(`{"email": "%s", "age": "%s", "gender": "%s"}`, email, age, gender)
		manager.ProcessTask(r.Context(), taskID, "NodeA", json.RawMessage(payload))
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

func (tm *DAG) Start(addr string) {
	http.HandleFunc("/", func(w http.ResponseWriter, request *http.Request) {
		firstNode, err := tm.Validate(request.Context())
		if err != nil {
			http.Error(w, `{"message": "taskID is missing"}`, http.StatusBadRequest)
			return
		}
		node, _ := tm.nodes.Get(firstNode)
		if node.Type == Page {

		}
		w.Write([]byte(firstNode))
	})
	http.HandleFunc("/form", tm.formHandler)
	http.HandleFunc("/result", tm.resultHandler)
	http.HandleFunc("/task-result", tm.taskStatusHandler)
	http.ListenAndServe(addr, nil)
}
