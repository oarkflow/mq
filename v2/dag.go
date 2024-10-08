package v2

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/oarkflow/xid"
)

type Handler func(ctx context.Context, task *Task) Result

type Result struct {
	TaskID  string          `json:"task_id"`
	Payload json.RawMessage `json:"payload"`
	Status  string          `json:"status"`
	Error   error           `json:"error"`
	nodeKey string
}

type Task struct {
	ID      string            `json:"id"`
	NodeKey string            `json:"node_key"`
	Payload json.RawMessage   `json:"payload"`
	Results map[string]Result `json:"results"`
}

type Node struct {
	Key     string
	Edges   []Edge
	handler Handler
}

type EdgeType int

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

func (tm *DAG) AddEdge(from, to string, edgeType EdgeType) {
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
	fromNode.Edges = append(fromNode.Edges, Edge{
		From: fromNode,
		To:   toNode,
		Type: edgeType,
	})
}

func (tm *DAG) ProcessTask(ctx context.Context, node string, payload []byte) Result {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	taskID := xid.New().String()
	task := &Task{
		ID:      taskID,
		NodeKey: node,
		Payload: payload,
		Results: make(map[string]Result),
	}
	manager := NewTaskManager(tm)
	tm.taskContext[taskID] = manager
	return manager.processTask(ctx, node, task)
}
