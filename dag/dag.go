package dag

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/oarkflow/mq"
)

type DAG struct {
	server      *mq.Broker
	nodes       map[string]*mq.Consumer
	edges       map[string][]string
	loopEdges   map[string]string
	taskChMap   map[string]chan mq.Result
	loopTaskMap map[string]*loopTaskContext
	mu          sync.Mutex
}

type loopTaskContext struct {
	subResultCh chan mq.Result
	totalItems  int
	completed   int
	results     []json.RawMessage
}

func New(opts ...mq.Option) *DAG {
	d := &DAG{
		nodes:       make(map[string]*mq.Consumer),
		edges:       make(map[string][]string),
		loopEdges:   make(map[string]string),
		taskChMap:   make(map[string]chan mq.Result),
		loopTaskMap: make(map[string]*loopTaskContext),
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

func (d *DAG) AddLoop(fromNode string, toNode string) {
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
	task, err := d.PublishTask(context.TODO(), payload, "queue1")
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
	log.Printf("Callback from queue %s with result: %s", task.CurrentQueue, string(task.Result))
	d.mu.Lock()
	loopCtx, isLoopTask := d.loopTaskMap[task.ID]
	d.mu.Unlock()
	if isLoopTask {
		loopCtx.subResultCh <- mq.Result{Payload: task.Result, MessageID: task.ID}
	}

	if loopNode, exists := d.loopEdges[task.CurrentQueue]; exists {
		var items []json.RawMessage
		if err := json.Unmarshal(task.Result, &items); err != nil {
			return err
		}
		loopCtx := &loopTaskContext{
			subResultCh: make(chan mq.Result, len(items)),
			totalItems:  len(items),
			results:     make([]json.RawMessage, 0, len(items)),
		}
		d.mu.Lock()
		d.loopTaskMap[task.ID] = loopCtx
		d.mu.Unlock()
		for _, item := range items {
			_, err := d.PublishTask(ctx, item, loopNode, task.ID)
			if err != nil {
				return err
			}
		}
		go d.waitForLoopCompletion(ctx, task.ID, task.CurrentQueue)
		return nil
	}
	edges, exists := d.edges[task.CurrentQueue]
	if exists {
		for _, edge := range edges {
			_, err := d.PublishTask(ctx, task.Result, edge, task.ID)
			if err != nil {
				return err
			}
		}
	} else {
		d.mu.Lock()
		if resultCh, ok := d.taskChMap[task.ID]; ok {
			resultCh <- mq.Result{
				Command:   "complete",
				Payload:   task.Result,
				Queue:     task.CurrentQueue,
				MessageID: task.ID,
				Status:    "done",
			}
			delete(d.taskChMap, task.ID)
		}
		d.mu.Unlock()
	}
	return nil
}

func (d *DAG) waitForLoopCompletion(ctx context.Context, taskID string, currentQueue string) {
	d.mu.Lock()
	loopCtx := d.loopTaskMap[taskID]
	d.mu.Unlock()
	for result := range loopCtx.subResultCh {
		loopCtx.results = append(loopCtx.results, result.Payload)
		loopCtx.completed++
		if loopCtx.completed == loopCtx.totalItems {
			close(loopCtx.subResultCh)
			aggregatedResult, err := json.Marshal(loopCtx.results)
			if err != nil {
				log.Printf("Error aggregating results: %v", err)
				return
			}
			d.mu.Lock()
			delete(d.loopTaskMap, taskID)
			d.mu.Unlock()
			edges, exists := d.edges[currentQueue]
			if exists {
				for _, edge := range edges {
					_, err := d.PublishTask(ctx, aggregatedResult, edge, taskID)
					if err != nil {
						log.Printf("Error publishing aggregated result: %v", err)
						return
					}
				}
			} else {
				d.mu.Lock()
				if resultCh, ok := d.taskChMap[taskID]; ok {
					resultCh <- mq.Result{
						Command:   "complete",
						Payload:   aggregatedResult,
						Queue:     currentQueue,
						MessageID: taskID,
						Status:    "done",
					}
					delete(d.taskChMap, taskID)
				}
				d.mu.Unlock()
			}
		}
	}
}
