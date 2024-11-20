package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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
	Ctx             context.Context `json:"-"`
	Data            json.RawMessage
	Error           error
	Status          TaskStatus
	ConditionStatus string
	Topic           string
}

type NodeType int

func (c NodeType) IsValid() bool { return c >= Function && c <= Page }

const (
	Function NodeType = iota
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
	nodes         storage.IMap[string, *Node]
	taskManager   storage.IMap[string, *TaskManager]
	iteratorNodes storage.IMap[string, []Edge]
	finalResult   func(taskID string, result Result)
	Error         error
	startNode     string
	conditions    map[string]map[string]string
}

func NewDAG(finalResultCallback func(taskID string, result Result)) *DAG {
	return &DAG{
		nodes:         memory.New[string, *Node](),
		taskManager:   memory.New[string, *TaskManager](),
		iteratorNodes: memory.New[string, []Edge](),
		conditions:    make(map[string]map[string]string),
		finalResult:   finalResultCallback,
	}
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
		if cond, ok := tm.conditions[node.ID]; ok {
			for _, target := range cond {
				connectedNodes[target] = true
				incomingEdges[target] = true
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

func (tm *DAG) AddCondition(fromNode string, conditions map[string]string) *DAG {
	tm.conditions[fromNode] = conditions
	return tm
}

type Handler func(ctx context.Context, payload json.RawMessage) Result

func (tm *DAG) AddNode(nodeType NodeType, nodeID string, handler Handler, startNode ...bool) *DAG {
	if tm.Error != nil {
		return tm
	}
	tm.nodes.Set(nodeID, &Node{ID: nodeID, Handler: handler, Type: nodeType})
	if len(startNode) > 0 && startNode[0] {
		tm.startNode = nodeID
	}
	return tm
}

func (tm *DAG) AddEdge(edgeType EdgeType, from string, targets ...string) *DAG {
	if tm.Error != nil {
		return tm
	}
	if edgeType == Iterator {
		tm.iteratorNodes.Set(from, []Edge{})
	}
	node, ok := tm.nodes.Get(from)
	if !ok {
		tm.Error = fmt.Errorf("node not found %s", from)
		return tm
	}
	for _, target := range targets {
		if targetNode, ok := tm.nodes.Get(target); ok {
			edge := Edge{From: node, To: targetNode, Type: edgeType}
			node.Edges = append(node.Edges, edge)
			if edgeType != Iterator {
				if edges, ok := tm.iteratorNodes.Get(node.ID); ok {
					edges = append(edges, edge)
					tm.iteratorNodes.Set(node.ID, edges)
				}
			}
		}
	}
	return tm
}

func (tm *DAG) GetNextNodes(key string) ([]*Node, error) {
	key = strings.Split(key, Delimiter)[0]
	node, exists := tm.nodes.Get(key)
	if !exists {
		return nil, fmt.Errorf("Node with key %s does not exist while getting next node", key)
	}
	var successors []*Node
	for _, edge := range node.Edges {
		successors = append(successors, edge.To)
	}
	if conds, exists := tm.conditions[key]; exists {
		for _, targetKey := range conds {
			if targetNode, exists := tm.nodes.Get(targetKey); exists {
				successors = append(successors, targetNode)
			}
		}
	}
	return successors, nil
}

func (tm *DAG) GetPreviousNodes(key string) ([]*Node, error) {
	key = strings.Split(key, Delimiter)[0]
	var predecessors []*Node
	tm.nodes.ForEach(func(_ string, node *Node) bool {
		for _, target := range node.Edges {
			if target.To.ID == key {
				predecessors = append(predecessors, node)
			}
		}
		return true
	})
	for fromNode, conds := range tm.conditions {
		for _, targetKey := range conds {
			if targetKey == key {
				node, exists := tm.nodes.Get(fromNode)
				if !exists {
					return nil, fmt.Errorf("Node with key %s does not exist while getting previous node", fromNode)
				}
				predecessors = append(predecessors, node)
			}
		}
	}
	return predecessors, nil
}

func (tm *DAG) ProcessTask(ctx context.Context, payload []byte) Result {
	var taskID string
	userCtx := UserContext(ctx)
	if val := userCtx.Get("task_id"); val != "" {
		taskID = val
	} else {
		taskID = mq.NewID()
	}
	ctx = context.WithValue(ctx, "task_id", taskID)
	userContext := UserContext(ctx)
	next := userContext.Get("next")
	manager, ok := tm.taskManager.Get(taskID)
	resultCh := make(chan Result, 1)
	if !ok {
		manager = NewTaskManager(tm, taskID, resultCh)
		tm.taskManager.Set(taskID, manager)
	} else {
		manager.resultCh = resultCh
	}
	if next == "true" {
		nodes, err := tm.GetNextNodes(manager.currentNode)
		if err != nil {
			return Result{Error: err, Ctx: ctx}
		}
		if len(nodes) > 0 {
			ctx = context.WithValue(ctx, "initial_node", nodes[0].ID)
		}
	}
	firstNode, err := tm.parseInitialNode(ctx)
	if err != nil {
		return Result{Error: err, Ctx: ctx}
	}
	node, ok := tm.nodes.Get(firstNode)
	if ok && node.Type != Page && payload == nil {
		return Result{Error: fmt.Errorf("payload is required for node %s", firstNode), Ctx: ctx}
	}
	ctx = context.WithValue(ctx, ContextIndex, "0")
	manager.ProcessTask(ctx, firstNode, payload)
	return <-resultCh
}
