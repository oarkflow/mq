package v2

import (
	"context"
	"encoding/json"
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

type Node struct {
	Key      string
	Edges    []Edge
	consumer *mq.Consumer
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
	server      *mq.Broker
	taskContext map[string]*TaskManager
	conditions  map[string]map[string]string
	mu          sync.RWMutex
}

func NewDAG(opts ...mq.Option) *DAG {
	d := &DAG{
		Nodes:       make(map[string]*Node),
		taskContext: make(map[string]*TaskManager),
		conditions:  make(map[string]map[string]string),
	}
	opts = append(opts, mq.WithCallback(d.onTaskCallback))
	d.server = mq.NewBroker(opts...)
	return d
}

func (tm *DAG) onTaskCallback(ctx context.Context, result mq.Result) mq.Result {
	if taskContext, ok := tm.taskContext[result.TaskID]; ok {
		return taskContext.handleCallback(ctx, result)
	}
	return mq.Result{}
}

func (tm *DAG) Start(ctx context.Context, addr string) error {
	if tm.server.SyncMode() {
		return nil
	}
	go func() {
		err := tm.server.Start(ctx)
		if err != nil {
			panic(err)
		}
	}()
	for _, con := range tm.Nodes {
		go func(con *Node) {
			time.Sleep(1 * time.Second)
			con.consumer.Consume(ctx)
		}(con)
	}
	log.Printf("HTTP server started on %s", addr)
	config := tm.server.TLSConfig()
	if config.UseTLS {
		return http.ListenAndServeTLS(addr, config.CertPath, config.KeyPath, nil)
	}
	return http.ListenAndServe(addr, nil)
}

func (tm *DAG) AddNode(key string, handler mq.Handler) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	con := mq.NewConsumer(key, key, handler)
	tm.Nodes[key] = &Node{
		Key:      key,
		consumer: con,
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

func (tm *DAG) ProcessTask(ctx context.Context, node string, payload []byte) mq.Result {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	taskID := xid.New().String()
	manager := NewTaskManager(tm, taskID)
	tm.taskContext[taskID] = manager
	return manager.processTask(ctx, node, payload)
}
