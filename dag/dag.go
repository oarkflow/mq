package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/oarkflow/mq/consts"

	"github.com/oarkflow/mq"
)

type taskContext struct {
	totalItems      int
	completed       int
	results         []json.RawMessage
	result          json.RawMessage
	multipleResults bool
}

type DAG struct {
	FirstNode   string
	server      *mq.Broker
	nodes       map[string]*mq.Consumer
	edges       map[string]string
	conditions  map[string]map[string]string
	loopEdges   map[string][]string
	taskChMap   map[string]chan mq.Result
	taskResults map[string]map[string]*taskContext
	mu          sync.Mutex
}

func New(opts ...mq.Option) *DAG {
	d := &DAG{
		nodes:       make(map[string]*mq.Consumer),
		edges:       make(map[string]string),
		conditions:  make(map[string]map[string]string),
		loopEdges:   make(map[string][]string),
		taskChMap:   make(map[string]chan mq.Result),
		taskResults: make(map[string]map[string]*taskContext),
	}
	opts = append(opts, mq.WithCallback(d.TaskCallback))
	d.server = mq.NewBroker(opts...)
	return d
}

func (d *DAG) AddNode(name string, handler mq.Handler, firstNode ...bool) {
	tlsConfig := d.server.TLSConfig()
	con := mq.NewConsumer(name, mq.WithTLS(tlsConfig.UseTLS, tlsConfig.CertPath, tlsConfig.KeyPath), mq.WithCAPath(tlsConfig.CAPath))
	if len(firstNode) > 0 {
		d.FirstNode = name
	}
	con.RegisterHandler(name, handler)
	d.nodes[name] = con
}

func (d *DAG) AddCondition(fromNode string, conditions map[string]string) {
	d.conditions[fromNode] = conditions
}

func (d *DAG) AddEdge(fromNode string, toNodes string) {
	d.edges[fromNode] = toNodes
}

func (d *DAG) AddLoop(fromNode string, toNode ...string) {
	d.loopEdges[fromNode] = toNode
}

func (d *DAG) Prepare() {
	if d.FirstNode == "" {
		firstNode, ok := d.FindFirstNode()
		if ok && firstNode != "" {
			d.FirstNode = firstNode
		}
	}
}

func (d *DAG) Start(ctx context.Context, addr string) error {
	d.Prepare()
	if d.server.SyncMode() {
		return nil
	}
	go func() {
		err := d.server.Start(ctx)
		if err != nil {
			panic(err)
		}
	}()
	for _, con := range d.nodes {
		go func(con *mq.Consumer) {
			con.Consume(ctx)
		}(con)
	}
	log.Printf("HTTP server started on %s", addr)
	config := d.server.TLSConfig()
	if config.UseTLS {
		return http.ListenAndServeTLS(addr, config.CertPath, config.KeyPath, nil)
	}
	return http.ListenAndServe(addr, nil)
}

func (d *DAG) PublishTask(ctx context.Context, payload json.RawMessage, taskID ...string) mq.Result {
	queue, ok := mq.GetQueue(ctx)
	if !ok {
		queue = d.FirstNode
	}
	var id string
	if len(taskID) > 0 {
		id = taskID[0]
	} else {
		id = mq.NewID()
	}
	task := mq.Task{
		ID:        id,
		Payload:   payload,
		CreatedAt: time.Now(),
	}
	err := d.server.Publish(ctx, task, queue)
	if err != nil {
		return mq.Result{Error: err}
	}
	return mq.Result{
		Payload: payload,
		Topic:   queue,
		TaskID:  id,
	}
}

func (d *DAG) FindFirstNode() (string, bool) {
	inDegree := make(map[string]int)
	for n, _ := range d.nodes {
		inDegree[n] = 0
	}
	for _, outNode := range d.edges {
		inDegree[outNode]++
	}
	for _, targets := range d.loopEdges {
		for _, outNode := range targets {
			inDegree[outNode]++
		}
	}
	for n, count := range inDegree {
		if count == 0 {
			return n, true
		}
	}
	return "", false
}

func (d *DAG) Request(ctx context.Context, payload []byte) mq.Result {
	return d.sendSync(ctx, mq.Result{Payload: payload})
}

func (d *DAG) Send(ctx context.Context, payload []byte) mq.Result {
	if d.FirstNode == "" {
		return mq.Result{Error: fmt.Errorf("initial node not defined")}
	}
	if d.server.SyncMode() {
		return d.sendSync(ctx, mq.Result{Payload: payload})
	}
	resultCh := make(chan mq.Result)
	result := d.PublishTask(ctx, payload)
	if result.Error != nil {
		return result
	}
	d.mu.Lock()
	d.taskChMap[result.TaskID] = resultCh
	d.mu.Unlock()
	finalResult := <-resultCh
	return finalResult
}

func (d *DAG) processNode(ctx context.Context, task mq.Result) mq.Result {
	if con, ok := d.nodes[task.Topic]; ok {
		return con.ProcessTask(ctx, &mq.Task{
			ID:      task.TaskID,
			Payload: task.Payload,
		})
	}
	return mq.Result{Error: fmt.Errorf("no consumer to process %s", task.Topic)}
}

func (d *DAG) sendSync(ctx context.Context, task mq.Result) mq.Result {
	if task.TaskID == "" {
		task.TaskID = mq.NewID()
	}
	if task.Topic == "" {
		task.Topic = d.FirstNode
	}
	ctx = mq.SetHeaders(ctx, map[string]string{
		consts.QueueKey: task.Topic,
	})
	result := d.processNode(ctx, task)
	if result.Error != nil {
		return result
	}
	for _, target := range d.loopEdges[task.Topic] {
		var items, results []json.RawMessage
		if err := json.Unmarshal(result.Payload, &items); err != nil {
			return mq.Result{Error: err}
		}
		for _, item := range items {
			ctx = mq.SetHeaders(ctx, map[string]string{
				consts.QueueKey: target,
			})
			result = d.sendSync(ctx, mq.Result{
				Payload: item,
				Topic:   target,
				TaskID:  result.TaskID,
			})
			if result.Error != nil {
				return result
			}
			results = append(results, result.Payload)
		}
		bt, err := json.Marshal(results)
		if err != nil {
			return mq.Result{Error: err}
		}
		result.Payload = bt
	}
	if conditions, ok := d.conditions[task.Topic]; ok {
		if target, exists := conditions[result.Status]; exists {
			ctx = mq.SetHeaders(ctx, map[string]string{
				consts.QueueKey: target,
			})
			result = d.sendSync(ctx, mq.Result{
				Payload: result.Payload,
				Topic:   target,
				TaskID:  result.TaskID,
			})
			if result.Error != nil {
				return result
			}
		}
	}
	if target, ok := d.edges[task.Topic]; ok {
		ctx = mq.SetHeaders(ctx, map[string]string{
			consts.QueueKey: target,
		})
		result = d.sendSync(ctx, mq.Result{
			Payload: result.Payload,
			Topic:   target,
			TaskID:  result.TaskID,
		})
		if result.Error != nil {
			return result
		}
	}
	return result
}

func (d *DAG) getCompletedResults(task mq.Result, ok bool, triggeredNode string) ([]byte, bool, bool) {
	var result any
	var payload []byte
	completed := false
	multipleResults := false
	if ok && triggeredNode != "" {
		taskResults, ok := d.taskResults[task.TaskID]
		if ok {
			nodeResult, exists := taskResults[triggeredNode]
			if exists {
				multipleResults = nodeResult.multipleResults
				nodeResult.completed++
				if nodeResult.completed == nodeResult.totalItems {
					completed = true
				}
				if multipleResults {
					nodeResult.results = append(nodeResult.results, task.Payload)
					if completed {
						result = nodeResult.results
					}
				} else {
					nodeResult.result = task.Payload
					if completed {
						result = nodeResult.result
					}
				}
			}
			if completed {
				delete(taskResults, triggeredNode)
			}
		}
	}
	if completed {
		payload, _ = json.Marshal(result)
	} else {
		payload = task.Payload
	}
	return payload, completed, multipleResults
}

func (d *DAG) TaskCallback(ctx context.Context, task mq.Result) mq.Result {
	if task.Error != nil {
		return mq.Result{Error: task.Error}
	}
	triggeredNode, ok := mq.GetTriggerNode(ctx)
	payload, completed, multipleResults := d.getCompletedResults(task, ok, triggeredNode)
	if loopNodes, exists := d.loopEdges[task.Topic]; exists {
		var items []json.RawMessage
		if err := json.Unmarshal(payload, &items); err != nil {
			return mq.Result{Error: task.Error}
		}
		d.taskResults[task.TaskID] = map[string]*taskContext{
			task.Topic: {
				totalItems:      len(items),
				multipleResults: true,
			},
		}

		ctx = mq.SetHeaders(ctx, map[string]string{consts.TriggerNode: task.Topic})
		for _, loopNode := range loopNodes {
			for _, item := range items {
				ctx = mq.SetHeaders(ctx, map[string]string{
					consts.QueueKey: loopNode,
				})
				result := d.PublishTask(ctx, item, task.TaskID)
				if result.Error != nil {
					return result
				}
			}
		}

		return task
	}
	if multipleResults && completed {
		task.Topic = triggeredNode
	}
	if conditions, ok := d.conditions[task.Topic]; ok {
		if target, exists := conditions[task.Status]; exists {
			d.taskResults[task.TaskID] = map[string]*taskContext{
				task.Topic: {
					totalItems: len(conditions),
				},
			}
			ctx = mq.SetHeaders(ctx, map[string]string{
				consts.QueueKey:    target,
				consts.TriggerNode: task.Topic,
			})
			result := d.PublishTask(ctx, payload, task.TaskID)
			if result.Error != nil {
				return result
			}
		}
	} else {
		ctx = mq.SetHeaders(ctx, map[string]string{consts.TriggerNode: task.Topic})
		edge, exists := d.edges[task.Topic]
		if exists {
			d.taskResults[task.TaskID] = map[string]*taskContext{
				task.Topic: {
					totalItems: 1,
				},
			}
			ctx = mq.SetHeaders(ctx, map[string]string{
				consts.QueueKey: edge,
			})
			result := d.PublishTask(ctx, payload, task.TaskID)
			if result.Error != nil {
				return result
			}
		} else if completed {
			d.mu.Lock()
			if resultCh, ok := d.taskChMap[task.TaskID]; ok {
				resultCh <- mq.Result{
					Payload: payload,
					Topic:   task.Topic,
					TaskID:  task.TaskID,
					Status:  "done",
				}
				delete(d.taskChMap, task.TaskID)
				delete(d.taskResults, task.TaskID)
			}
			d.mu.Unlock()
		}
	}

	return task
}
