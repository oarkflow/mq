package dag

import (
	"context"
	"testing"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	dagstorage "github.com/oarkflow/mq/dag/storage"
)

func TestDAGWithMemoryStorage(t *testing.T) {
	// Create a new DAG
	dag := NewDAG("test-dag", "test-key", func(taskID string, result mq.Result) {
		t.Logf("Task completed: %s", taskID)
	})

	// Configure memory storage
	dag.ConfigureMemoryStorage()

	// Verify storage is configured
	if dag.GetTaskStorage() == nil {
		t.Fatal("Task storage should be configured")
	}

	// Create a simple task
	ctx := context.Background()
	payload := json.RawMessage(`{"test": "data"}`)

	// Test task storage directly
	task := &dagstorage.PersistentTask{
		ID:        "test-task-1",
		DAGID:     "test-key",
		NodeID:    "test-node",
		Status:    dagstorage.TaskStatusPending,
		Payload:   payload,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Save task
	err := dag.GetTaskStorage().SaveTask(ctx, task)
	if err != nil {
		t.Fatalf("Failed to save task: %v", err)
	}

	// Retrieve task
	retrieved, err := dag.GetTaskStorage().GetTask(ctx, "test-task-1")
	if err != nil {
		t.Fatalf("Failed to retrieve task: %v", err)
	}

	if retrieved.ID != "test-task-1" {
		t.Errorf("Expected task ID 'test-task-1', got '%s'", retrieved.ID)
	}

	if retrieved.DAGID != "test-key" {
		t.Errorf("Expected DAG ID 'test-key', got '%s'", retrieved.DAGID)
	}

	// Test DAG isolation - tasks from different DAG should not be accessible
	// (This would require a more complex test with multiple DAGs)

	// Test activity logging
	logEntry := &dagstorage.TaskActivityLog{
		TaskID:    "test-task-1",
		DAGID:     "test-key",
		NodeID:    "test-node",
		Action:    "test_action",
		Message:   "Test activity",
		Level:     "info",
		CreatedAt: time.Now(),
	}

	err = dag.GetTaskStorage().LogActivity(ctx, logEntry)
	if err != nil {
		t.Fatalf("Failed to log activity: %v", err)
	}

	// Retrieve activity logs
	logs, err := dag.GetTaskStorage().GetActivityLogs(ctx, "test-task-1", 10, 0)
	if err != nil {
		t.Fatalf("Failed to retrieve activity logs: %v", err)
	}

	if len(logs) != 1 {
		t.Errorf("Expected 1 activity log, got %d", len(logs))
	}

	if len(logs) > 0 && logs[0].Action != "test_action" {
		t.Errorf("Expected action 'test_action', got '%s'", logs[0].Action)
	}
}

func TestDAGTaskRecovery(t *testing.T) {
	// Create a new DAG
	dag := NewDAG("recovery-test", "recovery-key", func(taskID string, result mq.Result) {
		t.Logf("Task completed: %s", taskID)
	})

	// Configure memory storage
	dag.ConfigureMemoryStorage()

	// Add a test node to the DAG
	testNode := &Node{
		ID:        "test-node",
		Label:     "Test Node",
		processor: &TestProcessor{},
	}
	dag.nodes.Set("test-node", testNode)

	ctx := context.Background()

	// Create and save a task
	task := &dagstorage.PersistentTask{
		ID:              "recovery-task-1",
		DAGID:           "recovery-key",
		NodeID:          "test-node",
		CurrentNodeID:   "test-node",
		SubDAGPath:      "",
		ProcessingState: "processing",
		Status:          dagstorage.TaskStatusRunning,
		Payload:         json.RawMessage(`{"test": "recovery data"}`),
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}

	// Save the task
	err := dag.GetTaskStorage().SaveTask(ctx, task)
	if err != nil {
		t.Fatalf("Failed to save task: %v", err)
	}

	// Verify task was saved with recovery information
	retrieved, err := dag.GetTaskStorage().GetTask(ctx, "recovery-task-1")
	if err != nil {
		t.Fatalf("Failed to retrieve task: %v", err)
	}

	if retrieved.CurrentNodeID != "test-node" {
		t.Errorf("Expected current node 'test-node', got '%s'", retrieved.CurrentNodeID)
	}

	if retrieved.ProcessingState != "processing" {
		t.Errorf("Expected processing state 'processing', got '%s'", retrieved.ProcessingState)
	}

	// Test recovery functionality
	err = dag.RecoverTasks(ctx)
	if err != nil {
		t.Fatalf("Failed to recover tasks: %v", err)
	}

	// Verify that the task manager was created for recovery
	manager, exists := dag.taskManager.Get("recovery-task-1")
	if !exists {
		t.Fatal("Task manager should have been created during recovery")
	}

	if manager == nil {
		t.Fatal("Task manager should not be nil")
	}
}

// TestProcessor is a simple processor for testing
type TestProcessor struct {
	key  string
	tags []string
}

func (p *TestProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{
		Payload: task.Payload,
		Status:  mq.Completed,
		Ctx:     ctx,
		TaskID:  task.ID,
	}
}

func (p *TestProcessor) Consume(ctx context.Context) error {
	return nil
}

func (p *TestProcessor) Pause(ctx context.Context) error {
	return nil
}

func (p *TestProcessor) Resume(ctx context.Context) error {
	return nil
}

func (p *TestProcessor) Stop(ctx context.Context) error {
	return nil
}

func (p *TestProcessor) Close() error {
	return nil
}

func (p *TestProcessor) GetKey() string {
	return p.key
}

func (p *TestProcessor) SetKey(key string) {
	p.key = key
}

func (p *TestProcessor) GetType() string {
	return "test"
}

func (p *TestProcessor) SetConfig(payload Payload) {
	// No-op for test
}

func (p *TestProcessor) SetTags(tags ...string) {
	p.tags = tags
}

func (p *TestProcessor) GetTags() []string {
	return p.tags
}

func TestDAGSubDAGRecovery(t *testing.T) {
	// Create a DAG representing a complex workflow with sub-dags
	dag := NewDAG("complex-dag", "complex-key", func(taskID string, result mq.Result) {
		t.Logf("Complex task completed: %s", taskID)
	})

	// Configure memory storage
	dag.ConfigureMemoryStorage()

	ctx := context.Background()

	// Simulate a task that was in the middle of processing in sub-dag 3, node D
	task := &dagstorage.PersistentTask{
		ID:              "complex-task-1",
		DAGID:           "complex-key",
		NodeID:          "start-node",
		CurrentNodeID:   "node-d",
		SubDAGPath:      "subdag3",
		ProcessingState: "processing",
		Status:          dagstorage.TaskStatusRunning,
		Payload:         json.RawMessage(`{"complex": "workflow data"}`),
		CreatedAt:       time.Now().Add(-5 * time.Minute), // Simulate task that started 5 minutes ago
		UpdatedAt:       time.Now(),
	}

	// Save the task
	err := dag.GetTaskStorage().SaveTask(ctx, task)
	if err != nil {
		t.Fatalf("Failed to save complex task: %v", err)
	}

	// Test that we can retrieve resumable tasks
	resumableTasks, err := dag.GetTaskStorage().GetResumableTasks(ctx, "complex-key")
	if err != nil {
		t.Fatalf("Failed to get resumable tasks: %v", err)
	}

	if len(resumableTasks) != 1 {
		t.Errorf("Expected 1 resumable task, got %d", len(resumableTasks))
	}

	if len(resumableTasks) > 0 {
		rt := resumableTasks[0]
		if rt.CurrentNodeID != "node-d" {
			t.Errorf("Expected resumable task current node 'node-d', got '%s'", rt.CurrentNodeID)
		}
		if rt.SubDAGPath != "subdag3" {
			t.Errorf("Expected resumable task sub-dag path 'subdag3', got '%s'", rt.SubDAGPath)
		}
	}
}
