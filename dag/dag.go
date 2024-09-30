package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/oarkflow/mq"
)

type taskContext struct {
	totalItems int
	completed  int
	results    []json.RawMessage
	result     json.RawMessage
	nodeType   string
}

type DAG struct {
	FirstNode   string
	server      *mq.Broker
	nodes       map[string]*mq.Consumer
	edges       map[string]string
	loopEdges   map[string][]string
	taskChMap   map[string]chan mq.Result
	taskResults map[string]map[string]*taskContext
	mu          sync.Mutex
}

func New(opts ...mq.Option) *DAG {
	d := &DAG{
		nodes:       make(map[string]*mq.Consumer),
		edges:       make(map[string]string),
		loopEdges:   make(map[string][]string),
		taskChMap:   make(map[string]chan mq.Result),
		taskResults: make(map[string]map[string]*taskContext),
	}
	opts = append(opts, mq.WithCallback(d.TaskCallback))
	d.server = mq.NewBroker(opts...)
	return d
}

func (d *DAG) AddNode(name string, handler mq.Handler, firstNode ...bool) {
	con := mq.NewConsumer(name)
	if len(firstNode) > 0 {
		d.FirstNode = name
	}
	con.RegisterHandler(name, handler)
	d.nodes[name] = con
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

func (d *DAG) Start(ctx context.Context) error {
	d.Prepare()
	if d.server.SyncMode() {
		return nil
	}
	for _, con := range d.nodes {
		go con.Consume(ctx)
	}
	return d.server.Start(ctx)
}

func (d *DAG) PublishTask(ctx context.Context, payload []byte, queueName string, taskID ...string) mq.Result {
	task := mq.Task{
		Payload: payload,
	}
	if len(taskID) > 0 {
		task.ID = taskID[0]
	}
	return d.server.Publish(ctx, task, queueName)
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
	result := d.PublishTask(ctx, payload, d.FirstNode)
	if result.Error != nil {
		return result
	}
	d.mu.Lock()
	d.taskChMap[result.MessageID] = resultCh
	d.mu.Unlock()
	finalResult := <-resultCh
	return finalResult
}

func (d *DAG) processNode(ctx context.Context, task mq.Result) mq.Result {
	if con, ok := d.nodes[task.Queue]; ok {
		return con.ProcessTask(ctx, mq.Task{
			ID:           task.MessageID,
			Payload:      task.Payload,
			CurrentQueue: task.Queue,
		})
	}
	return mq.Result{Error: fmt.Errorf("no consumer to process %s", task.Queue)}
}

func (d *DAG) sendSync(ctx context.Context, task mq.Result) mq.Result {
	if task.MessageID == "" {
		task.MessageID = mq.NewID()
	}
	if task.Queue == "" {
		task.Queue = d.FirstNode
	}
	result := d.processNode(ctx, task)
	if result.Error != nil {
		return result
	}
	for _, target := range d.loopEdges[task.Queue] {
		var items, results []json.RawMessage
		if err := json.Unmarshal(result.Payload, &items); err != nil {
			return mq.Result{Error: err}
		}
		for _, item := range items {
			result = d.sendSync(ctx, mq.Result{
				Command:   result.Command,
				Payload:   item,
				Queue:     target,
				MessageID: result.MessageID,
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
	if target, ok := d.edges[task.Queue]; ok {
		result = d.sendSync(ctx, mq.Result{
			Command:   result.Command,
			Payload:   result.Payload,
			Queue:     target,
			MessageID: result.MessageID,
		})
		if result.Error != nil {
			return result
		}
	}
	return result
}

func (d *DAG) TaskCallback(ctx context.Context, task mq.Result) mq.Result {
	if task.Error != nil {
		return mq.Result{Error: task.Error}
	}
	triggeredNode, ok := mq.GetTriggerNode(ctx)
	var result any
	var payload []byte
	completed := false
	var nodeType string
	if ok && triggeredNode != "" {
		taskResults, ok := d.taskResults[task.MessageID]
		if ok {
			nodeResult, exists := taskResults[triggeredNode]
			if exists {
				nodeResult.completed++
				if nodeResult.completed == nodeResult.totalItems {
					completed = true
				}
				switch nodeResult.nodeType {
				case "loop":
					nodeResult.results = append(nodeResult.results, task.Payload)
					if completed {
						result = nodeResult.results
					}
					nodeType = "loop"
				case "edge":
					nodeResult.result = task.Payload
					if completed {
						result = nodeResult.result
					}
					nodeType = "edge"
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
	if loopNodes, exists := d.loopEdges[task.Queue]; exists {
		var items []json.RawMessage
		if err := json.Unmarshal(payload, &items); err != nil {
			return mq.Result{Error: task.Error}
		}
		d.taskResults[task.MessageID] = map[string]*taskContext{
			task.Queue: {
				totalItems: len(items),
				nodeType:   "loop",
			},
		}

		ctx = mq.SetHeaders(ctx, map[string]string{mq.TriggerNode: task.Queue})
		for _, loopNode := range loopNodes {
			for _, item := range items {
				rs := d.PublishTask(ctx, item, loopNode, task.MessageID)
				if rs.Error != nil {
					return rs
				}
			}
		}

		return mq.Result{}
	}
	if nodeType == "loop" && completed {
		task.Queue = triggeredNode
	}
	ctx = mq.SetHeaders(ctx, map[string]string{mq.TriggerNode: task.Queue})
	edge, exists := d.edges[task.Queue]
	if exists {
		d.taskResults[task.MessageID] = map[string]*taskContext{
			task.Queue: {
				totalItems: 1,
				nodeType:   "edge",
			},
		}
		rs := d.PublishTask(ctx, payload, edge, task.MessageID)
		if rs.Error != nil {
			return rs
		}
	} else if completed {
		d.mu.Lock()
		if resultCh, ok := d.taskChMap[task.MessageID]; ok {
			resultCh <- mq.Result{
				Command:   "complete",
				Payload:   payload,
				Queue:     task.Queue,
				MessageID: task.MessageID,
				Status:    "done",
			}
			delete(d.taskChMap, task.MessageID)
			delete(d.taskResults, task.MessageID)
		}
		d.mu.Unlock()
	}
	return task
}
