package mq

import (
	"context"
	"errors"
	"fmt"

	"github.com/oarkflow/xsync"
)

const (
	triggerNodeKey string = "triggerNode"
)

type Node interface {
	Queue() string
	Consumer() *Consumer
	Handler() Handler
}

type node struct {
	queue    string
	consumer *Consumer
	handler  Handler
}

func (n *node) Queue() string {
	return n.queue
}

func (n *node) Consumer() *Consumer {
	return n.consumer
}

func (n *node) Handler() Handler {
	return n.handler
}

type DAG struct {
	nodes      *xsync.MapOf[string, Node]
	edges      [][]string
	loopEdges  [][]string
	broker     *Broker
	startNode  Node
	conditions map[string]map[string]string
	syncMode   bool
}

func NewDAG(syncMode bool) *DAG {
	dag := &DAG{
		nodes:      xsync.NewMap[string, Node](),
		conditions: make(map[string]map[string]string),
		syncMode:   syncMode,
	}
	dag.broker = NewBroker(WithCallback(dag.TaskCallback))
	return dag
}

func (dag *DAG) TaskCallback(ctx context.Context, task *Task) error {
	return nil
}

func (dag *DAG) AddNode(queue string, handler Handler, firstNode ...bool) {
	con := NewConsumer("consume-" + queue)
	con.RegisterHandler(queue, handler)
	dag.broker.NewQueue(queue)
	n := &node{
		queue:    queue,
		consumer: con,
		handler:  handler,
	}
	if len(firstNode) > 0 && firstNode[0] {
		dag.startNode = n
	}
	dag.nodes.Set(queue, n)
}

func (dag *DAG) AddEdge(fromNodeID, toNodeID string) error {
	err := dag.validateNodes(fromNodeID, toNodeID)
	if err != nil {
		return err
	}
	dag.edges = append(dag.edges, []string{fromNodeID, toNodeID})
	return nil
}

func (dag *DAG) AddCondition(conditionNodeID string, conditions map[string]string) error {
	for _, nodeID := range conditions {
		if err := dag.validateNodes(nodeID); err != nil {
			return err
		}
	}
	dag.conditions[conditionNodeID] = conditions
	return nil
}

func (dag *DAG) AddLoop(fromNodeID, toNodeID string) error {
	err := dag.validateNodes(fromNodeID, toNodeID)
	if err != nil {
		return err
	}
	dag.loopEdges = append(dag.loopEdges, []string{fromNodeID, toNodeID})
	return nil
}

func (dag *DAG) Start(ctx context.Context) error {
	if dag.syncMode {
		return nil
	}
	return dag.broker.Start(ctx)
}

func (dag *DAG) Prepare(ctx context.Context) error {
	startNode, err := dag.findInitialNode()
	if err != nil {
		return err
	}
	if startNode == nil {
		return fmt.Errorf("no initial node found")
	}
	dag.startNode = startNode
	if dag.syncMode {
		return nil
	}
	dag.nodes.ForEach(func(_ string, node Node) bool {
		go node.Consumer().Consume(ctx)
		return true
	})
	return nil
}

func (dag *DAG) ProcessTask(ctx context.Context, task Task) Result {
	return dag.processNode(ctx, &task, dag.startNode.Queue())
}

func (dag *DAG) getConditionalNode(status, currentNode string) string {
	conditions, ok := dag.conditions[currentNode]
	if !ok {
		return ""
	}
	conditionNodeID, ok := conditions[status]
	if !ok {
		return ""
	}
	return conditionNodeID
}

func (dag *DAG) validateNodes(nodeIDs ...string) error {
	for _, nodeID := range nodeIDs {
		if _, ok := dag.nodes.Get(nodeID); !ok {
			return fmt.Errorf("node %s not found", nodeID)
		}
	}
	return nil
}

func (dag *DAG) processEdge(ctx context.Context, id string, payload []byte, targets []string) {
	newTask := &Task{
		ID:      id,
		Payload: payload,
	}
	for _, target := range targets {
		if target != "" {
			dag.processNode(ctx, newTask, target)
		}
	}
}

func (dag *DAG) calculateForFirstNode() (string, bool) {
	inDegree := make(map[string]int)
	for _, n := range dag.nodes.Keys() {
		inDegree[n] = 0
	}
	for _, edge := range dag.edges {
		inDegree[edge[1]]++
	}
	for _, edge := range dag.loopEdges {
		inDegree[edge[1]]++
	}
	for n, count := range inDegree {
		if count == 0 {
			return n, true
		}
	}
	return "", false
}

func (dag *DAG) findInitialNode() (Node, error) {
	if dag.startNode != nil {
		return dag.startNode, nil
	}
	var nt Node
	n, ok := dag.calculateForFirstNode()
	if !ok {
		return nil, errors.New("no initial node found")
	}
	nt, ok = dag.nodes.Get(n)
	if !ok {
		return nil, errors.New("no initial node found")
	}
	return nt, nil
}

func (dag *DAG) processNode(ctx context.Context, task *Task, queue string) Result {
	if !dag.syncMode {
		if err := dag.broker.Publish(ctx, *task, queue); err != nil {
			fmt.Println("Failed to publish task:", err)
		}
		return Result{}
	}
	n, ok := dag.nodes.Get(queue)
	if task.CurrentQueue == "" {
		task.CurrentQueue = queue
	}
	if !ok {
		fmt.Println("Node not found:", queue)
		return Result{Error: fmt.Errorf("node not found %s", queue)}
	}
	_, err := dag.broker.AddMessageToQueue(task, queue)
	if err != nil {
		return Result{Error: err}
	}
	result := n.Handler()(ctx, *task)
	if result.Queue == "" {
		result.Queue = task.CurrentQueue
	}
	if result.MessageID == "" {
		result.MessageID = task.ID
	}
	if result.Error != nil {
		return result
	}
	err = dag.broker.HandleProcessedMessage(ctx, result)
	if err != nil {
		return Result{Error: err, Status: result.Status}
	}
	return result
}
