package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/oarkflow/xid"
)

type Handler func(ctx context.Context, task *Task) Result

type Result struct {
	Ctx     context.Context
	TaskID  string          `json:"task_id"`
	Payload json.RawMessage `json:"payload"`
	Status  string          `json:"status"`
	Error   error           `json:"error"`
	nodeKey string
}

func (r Result) Unmarshal(data any) error {
	if r.Payload == nil {
		return fmt.Errorf("payload is nil")
	}
	return json.Unmarshal(r.Payload, data)
}

func (r Result) String() string {
	return string(r.Payload)
}

func HandleError(ctx context.Context, err error, status ...string) Result {
	st := "Failed"
	if len(status) > 0 {
		st = status[0]
	}
	if err == nil {
		return Result{}
	}
	return Result{
		Status: st,
		Error:  err,
		Ctx:    ctx,
	}
}

func (r Result) WithData(status string, data []byte) Result {
	if r.Error != nil {
		return r
	}
	return Result{
		Status:  status,
		Payload: data,
		Error:   nil,
		Ctx:     r.Ctx,
	}
}

type Task struct {
	ID      string            `json:"id"`
	NodeKey string            `json:"node_key"`
	Payload json.RawMessage   `json:"payload"`
	Results map[string]Result `json:"results"`
}

func NewTask(id string, payload json.RawMessage, nodeKey string, results ...map[string]Result) *Task {
	if id == "" {
		id = xid.New().String()
	}
	result := make(map[string]Result)
	if len(results) > 0 && results[0] != nil {
		result = results[0]
	}
	return &Task{ID: id, Payload: payload, NodeKey: nodeKey, Results: result}
}

type Node struct {
	Key     string
	Edges   []Edge
	handler Handler
}

type EdgeType int

func (c EdgeType) IsValid() bool { return c >= SimpleEdge && c <= LoopEdge }

const (
	SimpleEdge EdgeType = iota
	LoopEdge
)

type Edge struct {
	From *Node
	To   *Node
	Type EdgeType
}

type DAG struct {
	Nodes       map[string]*Node
	taskContext map[string]*TaskManager
	conditions  map[string]map[string]string
	mu          sync.RWMutex
}

func NewDAG() *DAG {
	return &DAG{
		Nodes:       make(map[string]*Node),
		taskContext: make(map[string]*TaskManager),
		conditions:  make(map[string]map[string]string),
	}
}

func (tm *DAG) AddNode(key string, handler Handler) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.Nodes[key] = &Node{
		Key:     key,
		handler: handler,
	}
}

func (tm *DAG) AddCondition(fromNode string, conditions map[string]string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.conditions[fromNode] = conditions
}

func (tm *DAG) AddEdge(from, to string, edgeTypes ...EdgeType) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	fromNode, ok := tm.Nodes[from]
	if !ok {
		return
	}
	toNode, ok := tm.Nodes[to]
	if !ok {
		return
	}
	edge := Edge{From: fromNode, To: toNode}
	if len(edgeTypes) > 0 && edgeTypes[0].IsValid() {
		edge.Type = edgeTypes[0]
	}
	fromNode.Edges = append(fromNode.Edges, edge)
}

func (tm *DAG) ProcessTask(ctx context.Context, node string, payload []byte) Result {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	taskID := xid.New().String()
	task := NewTask(taskID, payload, node, make(map[string]Result))
	manager := NewTaskManager(tm)
	tm.taskContext[taskID] = manager
	return manager.processTask(ctx, node, task)
}
