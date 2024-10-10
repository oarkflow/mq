package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/mq"
)

func NewTask(id string, payload json.RawMessage, nodeKey string) *mq.Task {
	if id == "" {
		id = xid.New().String()
	}
	return &mq.Task{ID: id, Payload: payload, Topic: nodeKey}
}

type EdgeType int

func (c EdgeType) IsValid() bool { return c >= SimpleEdge && c <= LoopEdge }

const (
	SimpleEdge EdgeType = iota
	LoopEdge
)

type Node struct {
	Key      string
	Edges    []Edge
	isReady  bool
	consumer *mq.Consumer
}

type Edge struct {
	From *Node
	To   *Node
	Type EdgeType
}

type DAG struct {
	FirstNode   string
	Nodes       map[string]*Node
	server      *mq.Broker
	taskContext map[string]*TaskManager
	conditions  map[string]map[string]string
	mu          sync.RWMutex
	paused      bool
	opts        []mq.Option
}

func NewDAG(opts ...mq.Option) *DAG {
	d := &DAG{
		Nodes:       make(map[string]*Node),
		taskContext: make(map[string]*TaskManager),
		conditions:  make(map[string]map[string]string),
	}
	opts = append(opts, mq.WithCallback(d.onTaskCallback), mq.WithConsumerOnSubscribe(d.onConsumerJoin), mq.WithConsumerOnClose(d.onConsumerClose))
	d.server = mq.NewBroker(opts...)
	d.opts = opts
	return d
}

func (tm *DAG) onTaskCallback(ctx context.Context, result mq.Result) mq.Result {
	if taskContext, ok := tm.taskContext[result.TaskID]; ok && result.Topic != "" {
		return taskContext.handleCallback(ctx, result)
	}
	return mq.Result{}
}

func (tm *DAG) onConsumerJoin(_ context.Context, topic, _ string) {
	if node, ok := tm.Nodes[topic]; ok {
		log.Printf("DAG - CONSUMER ~> ready on %s", topic)
		node.isReady = true
	}
}

func (tm *DAG) onConsumerClose(_ context.Context, topic, _ string) {
	if node, ok := tm.Nodes[topic]; ok {
		log.Printf("DAG - CONSUMER ~> down on %s", topic)
		node.isReady = false
	}
}

func (tm *DAG) Start(ctx context.Context, addr string) error {
	if !tm.server.SyncMode() {
		go func() {
			err := tm.server.Start(ctx)
			if err != nil {
				panic(err)
			}
		}()
		for _, con := range tm.Nodes {
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

func (tm *DAG) AddNode(key string, handler mq.Handler, firstNode ...bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	con := mq.NewConsumer(key, key, handler, tm.opts...)
	tm.Nodes[key] = &Node{
		Key:      key,
		consumer: con,
		isReady:  true,
	}
	if len(firstNode) > 0 && firstNode[0] {
		tm.FirstNode = key
	}
}

func (tm *DAG) AddDeferredNode(key string, firstNode ...bool) error {
	if tm.server.SyncMode() {
		return fmt.Errorf("DAG cannot have deferred node in Sync Mode")
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.Nodes[key] = &Node{
		Key: key,
	}
	if len(firstNode) > 0 && firstNode[0] {
		tm.FirstNode = key
	}
	return nil
}

func (tm *DAG) IsReady() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, node := range tm.Nodes {
		if !node.isReady {
			return false
		}
	}
	return true
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

func (tm *DAG) ProcessTask(ctx context.Context, payload []byte) mq.Result {
	if tm.paused {
		return mq.Result{Error: fmt.Errorf("unable to process task, error: DAG is not accepting any task")}
	}
	if !tm.IsReady() {
		return mq.Result{Error: fmt.Errorf("unable to process task, error: DAG is not ready yet")}
	}
	val := ctx.Value("initial_node")
	initialNode, ok := val.(string)
	if !ok {
		if tm.FirstNode == "" {
			firstNode := tm.FindInitialNode()
			if firstNode != nil {
				tm.FirstNode = firstNode.Key
			}
		}
		if tm.FirstNode == "" {
			return mq.Result{Error: fmt.Errorf("initial node not found")}
		}
		initialNode = tm.FirstNode
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	taskID := xid.New().String()
	manager := NewTaskManager(tm, taskID)
	tm.taskContext[taskID] = manager
	return manager.processTask(ctx, initialNode, payload)
}

func (tm *DAG) FindInitialNode() *Node {
	incomingEdges := make(map[string]bool)
	connectedNodes := make(map[string]bool)
	for _, node := range tm.Nodes {
		for _, edge := range node.Edges {
			if edge.Type.IsValid() {
				connectedNodes[node.Key] = true
				connectedNodes[edge.To.Key] = true
				incomingEdges[edge.To.Key] = true
			}
		}
		if cond, ok := tm.conditions[node.Key]; ok {
			for _, target := range cond {
				connectedNodes[target] = true
				incomingEdges[target] = true
			}
		}
	}
	for nodeID, node := range tm.Nodes {
		if !incomingEdges[nodeID] && connectedNodes[nodeID] {
			return node
		}
	}
	return nil
}

func (tm *DAG) Pause() {
	tm.paused = true
	log.Printf("DAG - PAUSED")
}

func (tm *DAG) Resume() {
	tm.paused = false
	log.Printf("DAG - RESUMED")
}

func (tm *DAG) PauseConsumer(id string) {
	if node, ok := tm.Nodes[id]; ok {
		node.consumer.Pause()
		node.isReady = false
	}
}

func (tm *DAG) ResumeConsumer(id string) {
	if node, ok := tm.Nodes[id]; ok {
		node.consumer.Resume()
		node.isReady = true
	}
}
