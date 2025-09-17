package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// LoggingMiddleware logs the start and end of task processing
func LoggingMiddleware(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("Middleware: Starting processing for node %s, task %s", task.Topic, task.ID)
	start := time.Now()

	// For middleware, we return a successful result to continue to next middleware/processor
	// The actual processing will happen after all middlewares
	result := mq.Result{
		Status:  mq.Completed,
		Ctx:     ctx,
		Payload: task.Payload, // Pass through the payload
	}

	log.Printf("Middleware: Completed in %v", time.Since(start))
	return result
}

// ValidationMiddleware validates the task payload
func ValidationMiddleware(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("ValidationMiddleware: Validating payload for node %s", task.Topic)

	// Check if payload is empty
	if len(task.Payload) == 0 {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("empty payload not allowed"),
			Ctx:    ctx,
		}
	}

	log.Printf("ValidationMiddleware: Payload validation passed")
	return mq.Result{
		Status:  mq.Completed,
		Ctx:     ctx,
		Payload: task.Payload,
	}
}

// TimingMiddleware measures execution time
func TimingMiddleware(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("TimingMiddleware: Starting timing for node %s", task.Topic)

	// Add timing info to context
	ctx = context.WithValue(ctx, "start_time", time.Now())

	return mq.Result{
		Status:  mq.Completed,
		Ctx:     ctx,
		Payload: task.Payload,
	}
}

// Example processor that simulates some work
type ExampleProcessor struct {
	dag.Operation
}

func (p *ExampleProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("Processor: Processing task %s on node %s", task.ID, task.Topic)

	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)

	// Check if timing middleware was used
	if startTime, ok := ctx.Value("start_time").(time.Time); ok {
		duration := time.Since(startTime)
		log.Printf("Processor: Task completed in %v", duration)
	}

	result := fmt.Sprintf("Processed: %s", string(task.Payload))
	return mq.Result{
		Status:  mq.Completed,
		Payload: []byte(result),
		Ctx:     ctx,
	}
}

func main() {
	// Create a new DAG
	flow := dag.NewDAG("Middleware Example", "middleware-example", func(taskID string, result mq.Result) {
		log.Printf("Final result for task %s: %s", taskID, string(result.Payload))
	})

	// Add nodes
	flow.AddNode(dag.Function, "Process A", "process_a", &ExampleProcessor{Operation: dag.Operation{Type: dag.Function}}, true)
	flow.AddNode(dag.Function, "Process B", "process_b", &ExampleProcessor{Operation: dag.Operation{Type: dag.Function}})
	flow.AddNode(dag.Function, "Process C", "process_c", &ExampleProcessor{Operation: dag.Operation{Type: dag.Function}})

	// Add edges
	flow.AddEdge(dag.Simple, "A to B", "process_a", "process_b")
	flow.AddEdge(dag.Simple, "B to C", "process_b", "process_c")

	// Add global middlewares that apply to all nodes
	flow.Use(LoggingMiddleware, ValidationMiddleware)

	// Add node-specific middlewares
	flow.UseNodeMiddlewares(
		dag.NodeMiddleware{
			Node:        "process_a",
			Middlewares: []mq.Handler{TimingMiddleware},
		},
		dag.NodeMiddleware{
			Node:        "process_b",
			Middlewares: []mq.Handler{TimingMiddleware},
		},
	)

	if flow.Error != nil {
		panic(flow.Error)
	}

	// Test the DAG with middleware
	data := []byte(`{"message": "Hello from middleware example"}`)
	log.Printf("Starting DAG processing with payload: %s", string(data))

	result := flow.Process(context.Background(), data)
	if result.Error != nil {
		log.Printf("DAG processing failed: %v", result.Error)
	} else {
		log.Printf("DAG processing completed successfully: %s", string(result.Payload))
	}
}
