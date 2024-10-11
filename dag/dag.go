package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

func NewTask(id string, payload json.RawMessage, nodeKey string) *mq.Task {
	if id == "" {
		id = xid.New().String()
	}
	return &mq.Task{ID: id, Payload: payload, Topic: nodeKey}
}

type EdgeType int

func (c EdgeType) IsValid() bool { return c >= Simple && c <= Iterator }

const (
	Simple EdgeType = iota
	Iterator
)

type Node struct {
	Name     string
	Key      string
	Edges    []Edge
	isReady  bool
	consumer *mq.Consumer
}

func (n *Node) ProcessTask(ctx context.Context, msg *mq.Task) mq.Result {
	return n.consumer.ProcessTask(ctx, msg)
}

type Edge struct {
	Label string
	From  *Node
	To    []*Node
	Type  EdgeType
}

type (
	FromNode string
	When     string
	Then     string
)

type DAG struct {
	name        string
	startNode   string
	nodes       map[string]*Node
	server      *mq.Broker
	taskContext map[string]*TaskManager
	conditions  map[FromNode]map[When]Then
	mu          sync.RWMutex
	paused      bool
	opts        []mq.Option
}

func NewDAG(name string, opts ...mq.Option) *DAG {
	d := &DAG{
		name:        name,
		nodes:       make(map[string]*Node),
		taskContext: make(map[string]*TaskManager),
		conditions:  make(map[FromNode]map[When]Then),
	}
	opts = append(opts, mq.WithCallback(d.onTaskCallback), mq.WithConsumerOnSubscribe(d.onConsumerJoin), mq.WithConsumerOnClose(d.onConsumerClose))
	d.server = mq.NewBroker(opts...)
	d.opts = opts
	return d
}

func (tm *DAG) PrintGraph() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	fmt.Println("DAG Graph structure:")
	for _, node := range tm.nodes {
		fmt.Printf("Node: %s (%s) -> ", node.Name, node.Key)
		if conditions, ok := tm.conditions[FromNode(node.Key)]; ok {
			var c []string
			for when, then := range conditions {
				if target, ok := tm.nodes[string(then)]; ok {
					c = append(c, fmt.Sprintf("If [%s] Then %s (%s)", when, target.Name, target.Key))
				}
			}
			fmt.Println(strings.Join(c, ", "))
		}
		var c []string
		for _, edge := range node.Edges {
			for _, target := range edge.To {
				c = append(c, fmt.Sprintf("%s (%s)", target.Name, target.Key))
			}
		}
		fmt.Println(strings.Join(c, ", "))
	}
}

func (tm *DAG) ClassifyEdges(startNodes ...string) {
	startNode := tm.GetStartNode()
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if len(startNodes) > 0 && startNodes[0] != "" {
		startNode = startNodes[0]
	}
	visited := make(map[string]bool)
	discoveryTime := make(map[string]int)
	finishedTime := make(map[string]int)
	timeVal := 0
	if startNode == "" {
		firstNode := tm.findStartNode()
		if firstNode != nil {
			startNode = firstNode.Key
		}
	}
	if startNode != "" {
		tm.dfs(startNode, visited, discoveryTime, finishedTime, &timeVal)
	}
}

func (tm *DAG) dfs(v string, visited map[string]bool, discoveryTime, finishedTime map[string]int, timeVal *int) {
	visited[v] = true
	*timeVal++
	discoveryTime[v] = *timeVal
	node := tm.nodes[v]
	for _, edge := range node.Edges {
		for _, adj := range edge.To {
			switch edge.Type {
			case Simple:
				if !visited[adj.Key] {
					fmt.Printf("Simple Edge: %s -> %s\n", v, adj.Key)
					tm.dfs(adj.Key, visited, discoveryTime, finishedTime, timeVal)
				}
			case Iterator:
				if !visited[adj.Key] {
					fmt.Printf("Iterator Edge: %s -> %s\n", v, adj.Key)
					tm.dfs(adj.Key, visited, discoveryTime, finishedTime, timeVal)
				}
			}

		}
	}
	tm.handleConditionalEdges(v, visited, discoveryTime, finishedTime, timeVal)
	*timeVal++
	finishedTime[v] = *timeVal
}

func (tm *DAG) handleConditionalEdges(v string, visited map[string]bool, discoveryTime, finishedTime map[string]int, time *int) {
	node := tm.nodes[v]
	for when, then := range tm.conditions[FromNode(node.Key)] {
		if targetNodeKey, ok := tm.nodes[string(then)]; ok {
			if !visited[targetNodeKey.Key] {
				fmt.Printf("Conditional Edge [%s]: %s -> %s\n", when, v, targetNodeKey.Key)
				tm.dfs(targetNodeKey.Key, visited, discoveryTime, finishedTime, time)
			} else {
				if discoveryTime[v] > discoveryTime[targetNodeKey.Key] {
					fmt.Printf("Conditional Loop Edge [%s]: %s -> %s\n", when, v, targetNodeKey.Key)
				}
			}
		}
	}
}

func (tm *DAG) ExportDOT() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("digraph \"%s\" {\n", tm.name))
	sb.WriteString("  node [shape=box, style=rounded];\n")

	// Perform topological sorting to order nodes
	sortedNodes := tm.TopologicalSort()

	// Add nodes in topological order
	for _, nodeKey := range sortedNodes {
		node := tm.nodes[nodeKey]
		sb.WriteString(fmt.Sprintf("  \"%s\" [label=\"%s\"];\n", node.Key, node.Name))
	}

	// Add edges (both simple and loop edges) in topological order
	for _, nodeKey := range sortedNodes {
		node := tm.nodes[nodeKey]
		for _, edge := range node.Edges {
			edgeStyle := "solid"
			if edge.Type == Iterator {
				edgeStyle = "dashed" // Use dashed lines for loop edges
			}
			for _, to := range edge.To {
				sb.WriteString(fmt.Sprintf("  \"%s\" -> \"%s\" [label=\"%s\", style=%s];\n", node.Key, to.Key, edge.Label, edgeStyle))
			}
		}
	}

	// Add conditional edges
	for fromNodeKey, conditions := range tm.conditions {
		for when, then := range conditions {
			if toNode, ok := tm.nodes[string(then)]; ok {
				sb.WriteString(fmt.Sprintf("  \"%s\" -> \"%s\" [label=\"%s\", style=dotted];\n", fromNodeKey, toNode.Key, when))
			}
		}
	}

	sb.WriteString("}\n")
	return sb.String()
}

// TopologicalSort performs topological sorting and returns an ordered list of node keys
func (tm *DAG) TopologicalSort() []string {
	visited := make(map[string]bool)
	stack := []string{}
	for _, node := range tm.nodes {
		if !visited[node.Key] {
			tm.topologicalSortUtil(node.Key, visited, &stack)
		}
	}

	// Reverse the stack to get the correct topological order
	for i, j := 0, len(stack)-1; i < j; i, j = i+1, j-1 {
		stack[i], stack[j] = stack[j], stack[i]
	}

	return stack
}

// topologicalSortUtil is a recursive utility function for topological sorting
func (tm *DAG) topologicalSortUtil(v string, visited map[string]bool, stack *[]string) {
	visited[v] = true
	node := tm.nodes[v]

	for _, edge := range node.Edges {
		for _, to := range edge.To {
			if !visited[to.Key] {
				tm.topologicalSortUtil(to.Key, visited, stack)
			}
		}
	}

	*stack = append(*stack, v)
}

func (tm *DAG) onTaskCallback(ctx context.Context, result mq.Result) mq.Result {
	if taskContext, ok := tm.taskContext[result.TaskID]; ok && result.Topic != "" {
		return taskContext.handleCallback(ctx, result)
	}
	return mq.Result{}
}

func (tm *DAG) onConsumerJoin(_ context.Context, topic, _ string) {
	if node, ok := tm.nodes[topic]; ok {
		log.Printf("DAG - CONSUMER ~> ready on %s", topic)
		node.isReady = true
	}
}

func (tm *DAG) onConsumerClose(_ context.Context, topic, _ string) {
	if node, ok := tm.nodes[topic]; ok {
		log.Printf("DAG - CONSUMER ~> down on %s", topic)
		node.isReady = false
	}
}

func (tm *DAG) SetStartNode(node string) {
	tm.startNode = node
}

func (tm *DAG) GetStartNode() string {
	return tm.startNode
}

func (tm *DAG) Start(ctx context.Context, addr string) error {
	if !tm.server.SyncMode() {
		go func() {
			err := tm.server.Start(ctx)
			if err != nil {
				panic(err)
			}
		}()
		for _, con := range tm.nodes {
			if con.isReady {
				go func(con *Node) {
					time.Sleep(1 * time.Second)
					err := con.consumer.Consume(ctx)
					if err != nil {
						panic(err)
					}
				}(con)
			} else {
				log.Printf("[WARNING] - Consumer %s is not ready yet", con.Key)
			}
		}
	}
	log.Printf("DAG - HTTP_SERVER ~> started on %s", addr)
	config := tm.server.TLSConfig()
	if config.UseTLS {
		return http.ListenAndServeTLS(addr, config.CertPath, config.KeyPath, nil)
	}
	return http.ListenAndServe(addr, nil)
}

func (tm *DAG) AddNode(name, key string, handler mq.Handler, firstNode ...bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	con := mq.NewConsumer(key, key, handler, tm.opts...)
	tm.nodes[key] = &Node{
		Name:     name,
		Key:      key,
		consumer: con,
		isReady:  true,
	}
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
}

func (tm *DAG) AddDeferredNode(name, key string, firstNode ...bool) error {
	if tm.server.SyncMode() {
		return fmt.Errorf("DAG cannot have deferred node in Sync Mode")
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.nodes[key] = &Node{
		Name: name,
		Key:  key,
	}
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return nil
}

func (tm *DAG) IsReady() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, node := range tm.nodes {
		if !node.isReady {
			return false
		}
	}
	return true
}

func (tm *DAG) AddCondition(fromNode FromNode, conditions map[When]Then) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.conditions[fromNode] = conditions
}

func (tm *DAG) AddLoop(label, from string, targets ...string) {
	tm.addEdge(Iterator, label, from, targets...)
}

func (tm *DAG) AddEdge(label, from string, targets ...string) {
	tm.addEdge(Simple, label, from, targets...)
}

func (tm *DAG) addEdge(edgeType EdgeType, label, from string, targets ...string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	fromNode, ok := tm.nodes[from]
	if !ok {
		return
	}
	var nodes []*Node
	for _, target := range targets {
		toNode, ok := tm.nodes[target]
		if !ok {
			return
		}
		nodes = append(nodes, toNode)
	}
	edge := Edge{From: fromNode, To: nodes, Type: edgeType, Label: label}
	fromNode.Edges = append(fromNode.Edges, edge)
}

func (tm *DAG) ProcessTask(ctx context.Context, payload []byte) mq.Result {
	tm.mu.RLock()
	if tm.paused {
		tm.mu.RUnlock()
		return mq.Result{Error: fmt.Errorf("unable to process task, error: DAG is not accepting any task")}
	}
	tm.mu.RUnlock()
	if !tm.IsReady() {
		return mq.Result{Error: fmt.Errorf("unable to process task, error: DAG is not ready yet")}
	}
	val := ctx.Value("initial_node")
	initialNode, ok := val.(string)
	if !ok {
		if tm.startNode == "" {
			firstNode := tm.findStartNode()
			if firstNode != nil {
				tm.startNode = firstNode.Key
			}
		}
		if tm.startNode == "" {
			return mq.Result{Error: fmt.Errorf("initial node not found")}
		}
		initialNode = tm.startNode
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	taskID := xid.New().String()
	manager := NewTaskManager(tm, taskID)
	tm.taskContext[taskID] = manager
	return manager.processTask(ctx, initialNode, payload)
}

func (tm *DAG) findStartNode() *Node {
	incomingEdges := make(map[string]bool)
	connectedNodes := make(map[string]bool)
	for _, node := range tm.nodes {
		for _, edge := range node.Edges {
			if edge.Type.IsValid() {
				connectedNodes[node.Key] = true
				for _, to := range edge.To {
					connectedNodes[to.Key] = true
					incomingEdges[to.Key] = true
				}

			}
		}
		if cond, ok := tm.conditions[FromNode(node.Key)]; ok {
			for _, target := range cond {
				connectedNodes[string(target)] = true
				incomingEdges[string(target)] = true
			}
		}
	}
	for nodeID, node := range tm.nodes {
		if !incomingEdges[nodeID] && connectedNodes[nodeID] {
			return node
		}
	}
	return nil
}

func (tm *DAG) Pause(pause bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.paused = pause
}

func (tm *DAG) PauseConsumer(ctx context.Context, id string) {
	tm.doConsumer(ctx, id, consts.CONSUMER_PAUSE)
}

func (tm *DAG) ResumeConsumer(ctx context.Context, id string) {
	tm.doConsumer(ctx, id, consts.CONSUMER_RESUME)
}

func (tm *DAG) doConsumer(ctx context.Context, id string, action consts.CMD) {
	if node, ok := tm.nodes[id]; ok {
		switch action {
		case consts.CONSUMER_PAUSE:
			err := node.consumer.Pause(ctx)
			if err == nil {
				node.isReady = false
			}
		case consts.CONSUMER_RESUME:
			err := node.consumer.Resume(ctx)
			if err == nil {
				node.isReady = true
			}
		}
	}
}
