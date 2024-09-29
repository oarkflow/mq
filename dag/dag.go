package dag

import (
	"context"
	"encoding/json"
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
	server      *mq.Broker
	nodes       map[string]*mq.Consumer
	edges       map[string][]string
	loopEdges   map[string][]string
	taskChMap   map[string]chan mq.Result
	taskResults map[string]map[string]*taskContext
	mu          sync.Mutex
}

func New(opts ...mq.Option) *DAG {
	d := &DAG{
		nodes:       make(map[string]*mq.Consumer),
		edges:       make(map[string][]string),
		loopEdges:   make(map[string][]string),
		taskChMap:   make(map[string]chan mq.Result),
		taskResults: make(map[string]map[string]*taskContext),
	}
	opts = append(opts, mq.WithCallback(d.TaskCallback))
	d.server = mq.NewBroker(opts...)
	return d
}

func (d *DAG) AddNode(name string, handler mq.Handler) {
	con := mq.NewConsumer(name)
	con.RegisterHandler(name, handler)
	d.nodes[name] = con
}

func (d *DAG) AddEdge(fromNode string, toNodes ...string) {
	d.edges[fromNode] = toNodes
}

func (d *DAG) AddLoop(fromNode string, toNode ...string) {
	d.loopEdges[fromNode] = toNode
}

func (d *DAG) Start(ctx context.Context) error {
	for _, con := range d.nodes {
		go con.Consume(ctx)
	}
	return d.server.Start(ctx)
}

func (d *DAG) PublishTask(ctx context.Context, payload []byte, queueName string, taskID ...string) (*mq.Task, error) {
	task := mq.Task{
		Payload: payload,
	}
	if len(taskID) > 0 {
		task.ID = taskID[0]
	}
	return d.server.Publish(ctx, task, queueName)
}

func (d *DAG) Send(payload []byte) mq.Result {
	resultCh := make(chan mq.Result)
	task, err := d.PublishTask(context.TODO(), payload, "queue2")
	if err != nil {
		panic(err)
	}
	d.mu.Lock()
	d.taskChMap[task.ID] = resultCh
	d.mu.Unlock()
	finalResult := <-resultCh
	return finalResult
}

func (d *DAG) TaskCallback(ctx context.Context, task *mq.Task) error {
	if task.Error != nil {
		return task.Error
	}
	triggeredNode, ok := mq.GetTriggerNode(ctx)
	var result any
	var payload []byte
	completed := false
	var nodeType string
	if ok && triggeredNode != "" {
		taskResults, ok := d.taskResults[task.ID]
		if ok {
			nodeResult, exists := taskResults[triggeredNode]
			if exists {
				nodeResult.completed++
				if nodeResult.completed == nodeResult.totalItems {
					completed = true
				}
				switch nodeResult.nodeType {
				case "loop":
					nodeResult.results = append(nodeResult.results, task.Result)
					result = nodeResult.results
					nodeType = "loop"
				case "edge":
					nodeResult.result = task.Result
					result = nodeResult.result
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
		payload = task.Result
	}
	if loopNodes, exists := d.loopEdges[task.CurrentQueue]; exists {
		var items []json.RawMessage
		if err := json.Unmarshal(payload, &items); err != nil {
			return err
		}
		d.taskResults[task.ID] = map[string]*taskContext{
			task.CurrentQueue: {
				totalItems: len(items),
				nodeType:   "loop",
			},
		}

		ctx = mq.SetHeaders(ctx, map[string]string{mq.TriggerNode: task.CurrentQueue})
		for _, loopNode := range loopNodes {
			for _, item := range items {
				_, err := d.PublishTask(ctx, item, loopNode, task.ID)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}
	if nodeType == "loop" && completed {
		task.CurrentQueue = triggeredNode
	}
	ctx = mq.SetHeaders(ctx, map[string]string{mq.TriggerNode: task.CurrentQueue})
	edges, exists := d.edges[task.CurrentQueue]
	if exists {
		d.taskResults[task.ID] = map[string]*taskContext{
			task.CurrentQueue: {
				totalItems: 1,
				nodeType:   "edge",
			},
		}
		for _, edge := range edges {
			_, err := d.PublishTask(ctx, payload, edge, task.ID)
			if err != nil {
				return err
			}
		}
	} else if completed {
		d.mu.Lock()
		if resultCh, ok := d.taskChMap[task.ID]; ok {
			resultCh <- mq.Result{
				Command:   "complete",
				Payload:   payload,
				Queue:     task.CurrentQueue,
				MessageID: task.ID,
				Status:    "done",
			}
			delete(d.taskChMap, task.ID)
			delete(d.taskResults, task.ID)
		}
		d.mu.Unlock()
	}
	return nil
}
