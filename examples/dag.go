package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/oarkflow/mq"
)

func main() {
	dag := NewDAG()
	dag.AddNode("queue1", func(ctx context.Context, task mq.Task) mq.Result {
		log.Printf("Handling task for queue1: %s", string(task.Payload))
		return mq.Result{Payload: []byte(`[{"user_id": 1}, {"user_id": 2}]`), MessageID: task.ID}
	})
	dag.AddNode("queue2", func(ctx context.Context, task mq.Task) mq.Result {
		var item map[string]interface{}
		if err := json.Unmarshal(task.Payload, &item); err != nil {
			return mq.Result{Payload: nil, Error: err, MessageID: task.ID}
		}
		item["salary"] = 12000 // Simulating task logic by adding "salary"
		result, _ := json.Marshal(item)
		log.Printf("Handling task for queue2: %s", string(result))
		return mq.Result{Payload: result, MessageID: task.ID}
	})

	dag.AddEdge("queue1", "queue2")
	dag.AddLoop("queue1", "queue2") // This adds a loop between queue1 and queue2

	go func() {
		time.Sleep(2 * time.Second)
		finalResult := dag.Send([]byte(`{}`)) // sending empty payload to initiate
		log.Printf("Final result received: %s", string(finalResult.Payload))
	}()

	err := dag.Start(context.TODO())
	if err != nil {
		panic(err)
	}
}

// DAG struct to handle tasks and loops
type DAG struct {
	server      *mq.Broker
	nodes       map[string]*mq.Consumer
	edges       map[string][]string
	loopEdges   map[string]string // Handles loop edges
	taskChMap   map[string]chan mq.Result
	loopTaskMap map[string]*loopTaskContext // Map to handle loop tasks
	mu          sync.Mutex
}

// Structure to store the loop task context
type loopTaskContext struct {
	subResultCh chan mq.Result
	totalItems  int
	completed   int
	results     []json.RawMessage
}

// NewDAG initializes the DAG structure with necessary fields
func NewDAG(opts ...mq.Option) *DAG {
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

// PublishTask sends a task to a queue
func (d *DAG) PublishTask(ctx context.Context, payload []byte, queueName string, taskID ...string) (*mq.Task, error) {
	task := mq.Task{
		Payload: payload,
	}
	if len(taskID) > 0 {
		task.ID = taskID[0]
	}
	return d.server.Publish(ctx, task, queueName)
}

// TaskCallback is called when a task completes and decides the next step
func (d *DAG) TaskCallback(ctx context.Context, task *mq.Task) error {
	log.Printf("Callback from queue %s with result: %s", task.CurrentQueue, string(task.Result))

	// Check if this task belongs to a loop
	d.mu.Lock()
	loopCtx, isLoopTask := d.loopTaskMap[task.ID]
	d.mu.Unlock()
	if isLoopTask {
		// Send the sub-task result to the loop's result channel
		loopCtx.subResultCh <- mq.Result{Payload: task.Result, MessageID: task.ID}
	}

	// Handle loopEdges first, if applicable
	if loopNode, exists := d.loopEdges[task.CurrentQueue]; exists {
		// This is a loop node, and we need to handle array processing
		var items []json.RawMessage
		if err := json.Unmarshal(task.Result, &items); err != nil {
			return err
		}

		// Create a loop task context to track the state of this loop
		loopCtx := &loopTaskContext{
			subResultCh: make(chan mq.Result, len(items)), // A channel to collect sub-task results
			totalItems:  len(items),
			results:     make([]json.RawMessage, 0, len(items)),
		}

		// Register the loop context for this task
		d.mu.Lock()
		d.loopTaskMap[task.ID] = loopCtx
		d.mu.Unlock()

		// Publish a sub-task for each item in the array
		for _, item := range items {
			_, err := d.PublishTask(ctx, item, loopNode, task.ID)
			if err != nil {
				return err
			}
		}

		go d.waitForLoopCompletion(ctx, task.ID, task.CurrentQueue)

		return nil
	}

	// Normal edge processing
	edges, exists := d.edges[task.CurrentQueue]
	if exists {
		for _, edge := range edges {
			_, err := d.PublishTask(ctx, task.Result, edge, task.ID)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// waitForLoopCompletion waits until all sub-tasks are processed, aggregates results, and proceeds.
func (d *DAG) waitForLoopCompletion(ctx context.Context, taskID string, currentQueue string) {
	// Get the loop context
	d.mu.Lock()
	loopCtx := d.loopTaskMap[taskID]
	d.mu.Unlock()

	for result := range loopCtx.subResultCh {
		// Collect the result
		loopCtx.results = append(loopCtx.results, result.Payload)
		loopCtx.completed++
		// If all sub-tasks are completed, aggregate results and proceed
		if loopCtx.completed == loopCtx.totalItems {
			close(loopCtx.subResultCh)

			// Aggregate the results
			aggregatedResult, err := json.Marshal(loopCtx.results)
			if err != nil {
				log.Printf("Error aggregating results: %v", err)
				return
			}
			d.mu.Lock()
			if resultCh, ok := d.taskChMap[taskID]; ok {
				result := mq.Result{
					Command:   "complete",
					Payload:   aggregatedResult,
					Queue:     currentQueue,
					MessageID: taskID,
					Status:    "done",
				}
				resultCh <- result
				delete(d.taskChMap, taskID)
			}
			d.mu.Unlock()

			// Remove the loop context
			d.mu.Lock()
			delete(d.loopTaskMap, taskID)
			d.mu.Unlock()

			break
		}
	}
}

// Send sends the initial task and waits for the final result
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
