package main

import (
	"context"
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
		return mq.Result{Payload: []byte(`{"task": 123}`), MessageID: task.ID}
	})
	dag.AddNode("queue2", func(ctx context.Context, task mq.Task) mq.Result {
		log.Printf("Handling task for queue2: %s", string(task.Payload))
		return mq.Result{Payload: []byte(`{"task": 456}`), MessageID: task.ID}
	})
	dag.AddEdge("queue1", "queue2")
	
	// Start DAG processing
	go func() {
		time.Sleep(2 * time.Second)
		finalResult := dag.Send([]byte(`{"task": 1}`))
		log.Printf("Final result received: %s", string(finalResult.Payload))
	}()
	
	err := dag.Start(context.TODO())
	if err != nil {
		panic(err)
	}
}

type DAG struct {
	server    *mq.Broker
	nodes     map[string]*mq.Consumer
	edges     map[string][]string
	taskChMap map[string]chan mq.Result // A map to store result channels for each task
	mu        sync.Mutex                // Mutex to protect the taskChMap
}

func NewDAG(opts ...mq.Option) *DAG {
	d := &DAG{
		nodes:     make(map[string]*mq.Consumer),
		edges:     make(map[string][]string),
		taskChMap: make(map[string]chan mq.Result),
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

func (d *DAG) Start(ctx context.Context) error {
	for _, con := range d.nodes {
		go con.Consume(ctx)
	}
	return d.server.Start(ctx)
}

func (d *DAG) PublishTask(ctx context.Context, payload []byte, queueName string) (*mq.Task, error) {
	task := mq.Task{
		Payload: payload,
	}
	return d.server.Publish(ctx, task, queueName)
}

// TaskCallback is the function triggered after each task completion.
func (d *DAG) TaskCallback(ctx context.Context, task *mq.Task) error {
	log.Printf("Callback from queue %s with result: %s", task.CurrentQueue, string(task.Result))
	edges, exists := d.edges[task.CurrentQueue]
	if !exists {
		// Lock and send the result to the specific task channel
		d.mu.Lock()
		fmt.Println(d.taskChMap, task.ID)
		for _, resultCh := range d.taskChMap {
			result := mq.Result{
				Command:   "complete",
				Payload:   task.Result,
				Queue:     task.CurrentQueue,
				MessageID: task.ID,
				Status:    "done",
			}
			resultCh <- result
			delete(d.taskChMap, task.ID) // Clean up the channel
		}
		d.mu.Unlock()
		return nil
	}
	
	// Forward the task to the next node(s)
	for _, edge := range edges {
		_, err := d.PublishTask(ctx, task.Result, edge)
		if err != nil {
			return err
		}
	}
	return nil
}

// Send sends the task and waits for the final result.
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
