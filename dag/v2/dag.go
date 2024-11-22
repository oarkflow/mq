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

type Result struct {
	Ctx             context.Context `json:"-"`
	Payload         json.RawMessage
	Error           error
	Status          Status
	ConditionStatus string
	Topic           string
}

type Node struct {
	NodeType NodeType
	ID       string
	Handler  func(ctx context.Context, payload json.RawMessage) Result
	Edges    []Edge
}

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

func (tm *DAG) AddCondition(fromNode string, conditions map[string]string) *DAG {
	tm.conditions[fromNode] = conditions
	return tm
}

type Handler func(ctx context.Context, payload json.RawMessage) Result

func (tm *DAG) AddNode(nodeType NodeType, nodeID string, handler Handler, startNode ...bool) *DAG {
	if tm.Error != nil {
		return tm
	}
	tm.nodes.Set(nodeID, &Node{ID: nodeID, Handler: handler, NodeType: nodeType})
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
		manager = NewTaskManager(tm, taskID, resultCh, tm.iteratorNodes.Clone())
		tm.taskManager.Set(taskID, manager)
	} else {
		manager.resultCh = resultCh
	}
	currentNode := strings.Split(manager.currentNode, Delimiter)[0]
	node, exists := tm.nodes.Get(currentNode)
	method, ok := ctx.Value("method").(string)
	if method == "GET" && exists && node.NodeType == Page {
		ctx = context.WithValue(ctx, "initial_node", currentNode)
	} else if next == "true" {
		nodes, err := tm.GetNextNodes(currentNode)
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
	node, ok = tm.nodes.Get(firstNode)
	if ok && node.NodeType != Page && payload == nil {
		return Result{Error: fmt.Errorf("payload is required for node %s", firstNode), Ctx: ctx}
	}
	ctx = context.WithValue(ctx, ContextIndex, "0")
	manager.ProcessTask(ctx, firstNode, payload)
	return <-resultCh
}
