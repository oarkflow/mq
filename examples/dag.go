package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/oarkflow/mq"
)

func main() {
	dag := NewDAG()

	dag.AddNode("queue1", func(ctx context.Context, task mq.Task) mq.Result {
		log.Printf("Handling task for queue1: %s", string(task.Payload))
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})

	dag.AddNode("queue2", func(ctx context.Context, task mq.Task) mq.Result {
		log.Printf("Handling task for queue2: %s", string(task.Payload))
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})

	dag.AddNode("queue3", func(ctx context.Context, task mq.Task) mq.Result {
		var data map[string]any
		err := json.Unmarshal(task.Payload, &data)
		if err != nil {
			return mq.Result{Error: err}
		}
		data["salary"] = fmt.Sprintf("12000%v", data["user_id"])
		bt, _ := json.Marshal(data)
		log.Printf("Handling task for queue3: %s", string(bt))
		return mq.Result{Payload: bt, MessageID: task.ID}
	})

	dag.AddNode("queue4", func(ctx context.Context, task mq.Task) mq.Result {
		log.Printf("Handling task for queue4: %s", string(task.Payload))
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})

	// Define edges and loops
	dag.AddEdge("queue1", "queue2")
	dag.AddLoop("queue2", "queue3") // Loop through queue3 for each item from queue2
	dag.AddEdge("queue2", "queue4") // After processing queue2 (including loop), continue to queue4

	// Send task payload
	go func() {
		time.Sleep(2 * time.Second)
		finalResult := dag.Send([]byte(`[{"user_id": 1}, {"user_id": 2}]`))
		log.Printf("Final result received: %s", string(finalResult.Payload))
	}()

	err := dag.Start(context.TODO())
	if err != nil {
		panic(err)
	}
}

// DAG struct to handle tasks, edges, and loops
type DAG struct {
	server      *mq.Broker
	nodes       map[string]*mq.Consumer
	edges       map[string][]string
	loopEdges   map[string]string // Tracks loop edges
	taskChMap   map[string]chan mq.Result
	loopTaskMap map[string]*loopTaskContext // Manages loop tasks
	mu          sync.Mutex
}

// Structure to store the loop task context
type loopTaskContext struct {
	subResultCh chan mq.Result
	totalItems  int
	completed   int
	results     []json.RawMessage
}

// NewDAG initializes the DAG structure
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

// PublishTask sends a task to a specific queue
func (d *DAG) PublishTask(ctx context.Context, payload []byte, queueName string, taskID ...string) (*mq.Task, error) {
	task := mq.Task{
		Payload: payload,
	}
	if len(taskID) > 0 {
		task.ID = taskID[0]
	}
	return d.server.Publish(ctx, task, queueName)
}

// TaskCallback processes the completion of a task and continues the flow
func (d *DAG) TaskCallback(ctx context.Context, task *mq.Task) error {
	log.Printf("Callback from queue %s with result: %s", task.CurrentQueue, string(task.Result))

	// Check if the task belongs to a loop
	d.mu.Lock()
	loopCtx, isLoopTask := d.loopTaskMap[task.ID]
	d.mu.Unlock()

	if isLoopTask {
		loopCtx.subResultCh <- mq.Result{Payload: task.Result, MessageID: task.ID}
	}

	// Handle loopEdges first
	if loopNode, exists := d.loopEdges[task.CurrentQueue]; exists {
		var items []json.RawMessage
		if err := json.Unmarshal(task.Result, &items); err != nil {
			return err
		}
		loopCtx := &loopTaskContext{
			subResultCh: make(chan mq.Result, len(items)), // Collect sub-task results
			totalItems:  len(items),
			results:     make([]json.RawMessage, 0, len(items)),
		}
		d.mu.Lock()
		d.loopTaskMap[task.ID] = loopCtx
		d.mu.Unlock()

		// Publish a sub-task for each item
		for _, item := range items {
			_, err := d.PublishTask(ctx, item, loopNode, task.ID)
			if err != nil {
				return err
			}
		}

		// Wait for loop completion
		go d.waitForLoopCompletion(ctx, task.ID, task.CurrentQueue)
		return nil
	}

	// Handle normal edges
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

// waitForLoopCompletion waits for all sub-tasks in a loop to finish, aggregates the results, and proceeds
func (d *DAG) waitForLoopCompletion(ctx context.Context, taskID string, currentQueue string) {
	// Get the loop context
	d.mu.Lock()
	loopCtx := d.loopTaskMap[taskID]
	d.mu.Unlock()

	for result := range loopCtx.subResultCh {
		// Collect results
		loopCtx.results = append(loopCtx.results, result.Payload)
		loopCtx.completed++

		// If all sub-tasks are completed, aggregate results and continue
		if loopCtx.completed == loopCtx.totalItems {
			close(loopCtx.subResultCh)

			// Aggregate results
			aggregatedResult, err := json.Marshal(loopCtx.results)
			if err != nil {
				log.Printf("Error aggregating results: %v", err)
				return
			}

			// Continue flow after loop completion
			d.mu.Lock()
			delete(d.loopTaskMap, taskID)
			d.mu.Unlock()

			// Handle the next nodes in the DAG
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
				// If no further edges, finalize
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
