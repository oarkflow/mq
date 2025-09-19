package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	dagstorage "github.com/oarkflow/mq/dag/storage"
)

// RecoveryProcessor demonstrates a simple processor for recovery example
type RecoveryProcessor struct {
	nodeName string
}

func (p *RecoveryProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("Processing task %s in node %s", task.ID, p.nodeName)

	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)

	return mq.Result{
		Payload: task.Payload,
		Status:  mq.Completed,
		Ctx:     ctx,
		TaskID:  task.ID,
	}
}

func (p *RecoveryProcessor) Consume(ctx context.Context) error { return nil }
func (p *RecoveryProcessor) Pause(ctx context.Context) error   { return nil }
func (p *RecoveryProcessor) Resume(ctx context.Context) error  { return nil }
func (p *RecoveryProcessor) Stop(ctx context.Context) error    { return nil }
func (p *RecoveryProcessor) Close() error                      { return nil }
func (p *RecoveryProcessor) GetKey() string                    { return p.nodeName }
func (p *RecoveryProcessor) SetKey(key string)                 { p.nodeName = key }
func (p *RecoveryProcessor) GetType() string                   { return "recovery" }
func (p *RecoveryProcessor) SetConfig(payload dag.Payload)     {}
func (p *RecoveryProcessor) SetTags(tags ...string)            {}
func (p *RecoveryProcessor) GetTags() []string                 { return nil }

func demonstrateTaskRecovery() {
	ctx := context.Background()

	// Create a DAG with 5 nodes (simulating a complex workflow)
	dagInstance := dag.NewDAG("complex-workflow", "workflow-1", func(taskID string, result mq.Result) {
		log.Printf("Workflow completed for task: %s", taskID)
	})

	// Configure memory storage for this example
	dagInstance.ConfigureMemoryStorage()

	// Add nodes to simulate a complex workflow
	nodes := []string{"start", "validate", "process", "enrich", "finalize"}

	for _, nodeName := range nodes {
		dagInstance.AddNode(dag.Function, nodeName, fmt.Sprintf("Node %s", nodeName), &RecoveryProcessor{nodeName: nodeName}, true)
	}

	// Connect the nodes in sequence
	for i := 0; i < len(nodes)-1; i++ {
		dagInstance.AddEdge(dag.Simple, fmt.Sprintf("Connect %s to %s", nodes[i], nodes[i+1]), nodes[i], nodes[i+1])
	}

	// Simulate a task that was running and got interrupted
	runningTask := &dagstorage.PersistentTask{
		ID:              "interrupted-task-123",
		DAGID:           "workflow-1",
		NodeID:          "start",      // Original starting node
		CurrentNodeID:   "process",    // Task was processing this node when interrupted
		SubDAGPath:      "",           // No sub-dags in this example
		ProcessingState: "processing", // Was actively processing
		Status:          dagstorage.TaskStatusRunning,
		Payload:         json.RawMessage(`{"user_id": 12345, "action": "process_data"}`),
		CreatedAt:       time.Now().Add(-10 * time.Minute), // Started 10 minutes ago
		UpdatedAt:       time.Now().Add(-2 * time.Minute),  // Last updated 2 minutes ago
	}

	// Save the interrupted task to storage
	err := dagInstance.GetTaskStorage().SaveTask(ctx, runningTask)
	if err != nil {
		log.Fatal("Failed to save interrupted task:", err)
	}

	log.Println("✅ Simulated an interrupted task that was processing 'process' node")

	// Simulate system restart - recover tasks
	log.Println("🔄 Simulating system restart...")
	time.Sleep(1 * time.Second) // Simulate restart delay

	log.Println("🚀 Starting task recovery...")
	err = dagInstance.RecoverTasks(ctx)
	if err != nil {
		log.Fatal("Failed to recover tasks:", err)
	}

	log.Println("✅ Task recovery completed successfully!")

	// Verify the task was recovered
	recoveredTasks, err := dagInstance.GetTaskStorage().GetResumableTasks(ctx, "workflow-1")
	if err != nil {
		log.Fatal("Failed to get recovered tasks:", err)
	}

	log.Printf("📊 Found %d recovered tasks", len(recoveredTasks))
	for _, task := range recoveredTasks {
		log.Printf("🔄 Recovered task: %s, Current Node: %s, Status: %s",
			task.ID, task.CurrentNodeID, task.Status)
	}

	log.Println("🎉 Task recovery demonstration completed!")
	log.Println("💡 In a real scenario, the recovered task would continue processing from the 'process' node")
}

func mai7n() {
	fmt.Println("=== DAG Task Recovery Example ===")
	demonstrateTaskRecovery()
}
