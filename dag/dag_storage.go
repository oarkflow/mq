package dag

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"

	dagstorage "github.com/oarkflow/mq/dag/storage"
)

// SetTaskStorage sets the task storage for persistence
func (d *DAG) SetTaskStorage(storage dagstorage.TaskStorage) {
	d.taskStorage = storage
}

// GetTaskStorage returns the current task storage
func (d *DAG) GetTaskStorage() dagstorage.TaskStorage {
	return d.taskStorage
}

// GetTasks retrieves tasks for this DAG with optional status filtering
func (d *DAG) GetTasks(ctx context.Context, status *dagstorage.TaskStatus, limit int, offset int) ([]*dagstorage.PersistentTask, error) {
	if d.taskStorage == nil {
		return nil, fmt.Errorf("task storage not configured")
	}

	if status != nil {
		return d.taskStorage.GetTasksByStatus(ctx, d.key, *status)
	}
	return d.taskStorage.GetTasksByDAG(ctx, d.key, limit, offset)
}

// GetTaskActivityLogs retrieves activity logs for this DAG
func (d *DAG) GetTaskActivityLogs(ctx context.Context, limit int, offset int) ([]*dagstorage.TaskActivityLog, error) {
	if d.taskStorage == nil {
		return nil, fmt.Errorf("task storage not configured")
	}

	return d.taskStorage.GetActivityLogsByDAG(ctx, d.key, limit, offset)
}

// RecoverTasks loads and resumes pending/running tasks from storage
func (d *DAG) RecoverTasks(ctx context.Context) error {
	if d.taskStorage == nil {
		return fmt.Errorf("task storage not configured")
	}

	d.Logger().Info("Starting task recovery", logger.Field{Key: "dagID", Value: d.key})

	// Get all resumable tasks for this DAG
	resumableTasks, err := d.taskStorage.GetResumableTasks(ctx, d.key)
	if err != nil {
		return fmt.Errorf("failed to get resumable tasks: %w", err)
	}

	d.Logger().Info("Found tasks to recover", logger.Field{Key: "count", Value: len(resumableTasks)})

	// Resume each task from its last known position
	for _, task := range resumableTasks {
		if err := d.resumeTaskFromStorage(ctx, task); err != nil {
			d.Logger().Error("Failed to resume task",
				logger.Field{Key: "taskID", Value: task.ID},
				logger.Field{Key: "error", Value: err.Error()})
			continue
		}
		d.Logger().Info("Successfully resumed task",
			logger.Field{Key: "taskID", Value: task.ID},
			logger.Field{Key: "currentNode", Value: task.CurrentNodeID})
	}

	return nil
}

// resumeTaskFromStorage resumes a task from its stored position
func (d *DAG) resumeTaskFromStorage(ctx context.Context, task *dagstorage.PersistentTask) error {
	// Determine the node to resume from
	resumeNodeID := task.CurrentNodeID
	if resumeNodeID == "" {
		resumeNodeID = task.NodeID // Fallback to original node
	}

	// Check if the node exists (but don't use the variable)
	_, exists := d.nodes.Get(resumeNodeID)
	if !exists {
		return fmt.Errorf("resume node %s not found in DAG", resumeNodeID)
	}

	// Create a new task manager for this task if it doesn't exist
	manager, exists := d.taskManager.Get(task.ID)
	if !exists {
		resultCh := make(chan mq.Result, 1)
		manager = NewTaskManager(d, task.ID, resultCh, d.iteratorNodes.Clone(), d.taskStorage)
		d.taskManager.Set(task.ID, manager)
	}

	// Resume the task from the stored position using TaskManager's ProcessTask
	if task.Status == dagstorage.TaskStatusPending {
		// Re-enqueue the task
		manager.ProcessTask(ctx, resumeNodeID, task.Payload)
	} else if task.Status == dagstorage.TaskStatusRunning {
		// Task was in progress, resume from current node
		manager.ProcessTask(ctx, resumeNodeID, task.Payload)
	}

	return nil
}

// ConfigureMemoryStorage configures the DAG to use in-memory storage
func (d *DAG) ConfigureMemoryStorage() {
	d.taskStorage = dagstorage.NewMemoryTaskStorage()
}

// ConfigurePostgresStorage configures the DAG to use PostgreSQL storage
func (d *DAG) ConfigurePostgresStorage(dsn string, opts ...dagstorage.StorageOption) error {
	config := &dagstorage.TaskStorageConfig{
		Type:            "postgres",
		DSN:             dsn,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}

	// Apply options
	for _, opt := range opts {
		opt(config)
	}

	storage, err := dagstorage.NewSQLTaskStorage(config)
	if err != nil {
		return fmt.Errorf("failed to create postgres storage: %w", err)
	}

	d.taskStorage = storage
	return nil
}

// ConfigureSQLiteStorage configures the DAG to use SQLite storage
func (d *DAG) ConfigureSQLiteStorage(dbPath string, opts ...dagstorage.StorageOption) error {
	config := &dagstorage.TaskStorageConfig{
		Type:            "sqlite",
		DSN:             dbPath,
		MaxOpenConns:    1, // SQLite works best with single connection
		MaxIdleConns:    1,
		ConnMaxLifetime: 0, // No limit for SQLite
	}

	// Apply options
	for _, opt := range opts {
		opt(config)
	}

	storage, err := dagstorage.NewSQLTaskStorage(config)
	if err != nil {
		return fmt.Errorf("failed to create sqlite storage: %w", err)
	}

	d.taskStorage = storage
	return nil
}
