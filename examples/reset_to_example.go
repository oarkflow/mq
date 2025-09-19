package main

import (
	"context"
	"fmt"
	"log"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// ResetToExample demonstrates the ResetTo functionality
type ResetToExample struct {
	dag.Operation
}

func (r *ResetToExample) Process(ctx context.Context, task *mq.Task) mq.Result {
	payload := string(task.Payload)
	log.Printf("Processing node %s with payload: %s", task.Topic, payload)

	// Simulate some processing logic
	if task.Topic == "step1" {
		// For step1, we'll return a result that resets to step2
		return mq.Result{
			Status:  mq.Completed,
			Payload: json.RawMessage(`{"message": "Step 1 completed, resetting to step2"}`),
			Ctx:     ctx,
			TaskID:  task.ID,
			Topic:   task.Topic,
			ResetTo: "step2", // Reset to step2
		}
	} else if task.Topic == "step2" {
		// For step2, we'll return a result that resets to the previous page node
		return mq.Result{
			Status:  mq.Completed,
			Payload: json.RawMessage(`{"message": "Step 2 completed, resetting to back"}`),
			Ctx:     ctx,
			TaskID:  task.ID,
			Topic:   task.Topic,
			ResetTo: "back", // Reset to previous page node
		}
	} else if task.Topic == "step3" {
		// Final step
		return mq.Result{
			Status:  mq.Completed,
			Payload: json.RawMessage(`{"message": "Step 3 completed - final result"}`),
			Ctx:     ctx,
			TaskID:  task.ID,
			Topic:   task.Topic,
		}
	}

	return mq.Result{
		Status: mq.Failed,
		Error:  fmt.Errorf("unknown step: %s", task.Topic),
		Ctx:    ctx,
		TaskID: task.ID,
		Topic:  task.Topic,
	}
}

func runResetToExample() {
	// Create a DAG with ResetTo functionality
	flow := dag.NewDAG("ResetTo Example", "reset-to-example", func(taskID string, result mq.Result) {
		log.Printf("Final result for task %s: %s", taskID, string(result.Payload))
	})

	// Add nodes
	flow.AddNode(dag.Function, "Step 1", "step1", &ResetToExample{}, true)
	flow.AddNode(dag.Page, "Step 2", "step2", &ResetToExample{})
	flow.AddNode(dag.Page, "Step 3", "step3", &ResetToExample{})

	// Add edges
	flow.AddEdge(dag.Simple, "Step 1 to Step 2", "step1", "step2")
	flow.AddEdge(dag.Simple, "Step 2 to Step 3", "step2", "step3")

	// Validate the DAG
	if err := flow.Validate(); err != nil {
		log.Fatalf("DAG validation failed: %v", err)
	}

	// Process a task
	data := json.RawMessage(`{"initial": "data"}`)
	log.Println("Starting DAG processing...")
	result := flow.Process(context.Background(), data)

	if result.Error != nil {
		log.Printf("Processing failed: %v", result.Error)
	} else {
		log.Printf("Processing completed successfully: %s", string(result.Payload))
	}
}

func main() {
	runResetToExample()
}
