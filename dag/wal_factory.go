package dag

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/mq/dag/storage"
	"github.com/oarkflow/mq/dag/wal"
	"github.com/oarkflow/mq/logger"
)

// WALEnabledStorageFactory creates WAL-enabled storage instances
type WALEnabledStorageFactory struct {
	logger logger.Logger
}

// NewWALEnabledStorageFactory creates a new WAL-enabled storage factory
func NewWALEnabledStorageFactory(logger logger.Logger) *WALEnabledStorageFactory {
	return &WALEnabledStorageFactory{
		logger: logger,
	}
}

// CreateMemoryStorage creates a WAL-enabled memory storage
func (f *WALEnabledStorageFactory) CreateMemoryStorage(walConfig *wal.WALConfig) (storage.TaskStorage, *wal.WALManager, error) {
	if walConfig == nil {
		walConfig = wal.DefaultWALConfig()
	}

	// Create underlying memory storage
	memoryStorage := storage.NewMemoryTaskStorage()

	// For memory storage, we'll use an in-memory SQLite database for WAL
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create in-memory database: %w", err)
	}

	// Create WAL storage implementation
	walStorage := wal.NewWALStorage(memoryStorage, db, walConfig)

	// Initialize WAL tables
	if err := walStorage.InitializeTables(context.Background()); err != nil {
		return nil, nil, fmt.Errorf("failed to initialize WAL tables: %w", err)
	}

	// Create WAL manager
	walManager := wal.NewWALManager(walConfig, walStorage)

	// Create WAL-enabled storage wrapper
	walEnabledStorage := &WALEnabledStorageWrapper{
		underlying: memoryStorage,
		walManager: walManager,
	}

	return walEnabledStorage, walManager, nil
}

// CreateSQLStorage creates a WAL-enabled SQL storage
func (f *WALEnabledStorageFactory) CreateSQLStorage(config *storage.TaskStorageConfig, walConfig *wal.WALConfig) (storage.TaskStorage, *wal.WALManager, error) {
	if config == nil {
		config = &storage.TaskStorageConfig{
			Type: "postgres", // Default to postgres
		}
	}

	if walConfig == nil {
		walConfig = wal.DefaultWALConfig()
	}

	// Create underlying SQL storage
	sqlStorage, err := storage.NewSQLTaskStorage(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create SQL storage: %w", err)
	}

	// Get the database connection from the SQL storage
	db := sqlStorage.GetDB() // We'll need to add this method to SQLTaskStorage

	// Create WAL storage implementation
	walStorage := wal.NewWALStorage(sqlStorage, db, walConfig)

	// Initialize WAL tables
	if err := walStorage.InitializeTables(context.Background()); err != nil {
		return nil, nil, fmt.Errorf("failed to initialize WAL tables: %w", err)
	}

	// Create WAL manager
	walManager := wal.NewWALManager(walConfig, walStorage)

	// Create WAL-enabled storage wrapper
	walEnabledStorage := &WALEnabledStorageWrapper{
		underlying: sqlStorage,
		walManager: walManager,
	}

	return walEnabledStorage, walManager, nil
}

// WALEnabledStorageWrapper wraps any TaskStorage with WAL functionality
type WALEnabledStorageWrapper struct {
	underlying storage.TaskStorage
	walManager *wal.WALManager
}

// SaveTask saves a task through WAL
func (w *WALEnabledStorageWrapper) SaveTask(ctx context.Context, task *storage.PersistentTask) error {
	// Serialize task to JSON for WAL
	taskData, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Write to WAL first
	if err := w.walManager.WriteEntry(ctx, wal.WALEntryTypeTaskUpdate, taskData, map[string]any{
		"task_id": task.ID,
		"dag_id":  task.DAGID,
	}); err != nil {
		return fmt.Errorf("failed to write task to WAL: %w", err)
	}

	// Delegate to underlying storage (will be applied during flush)
	return w.underlying.SaveTask(ctx, task)
}

// LogActivity logs activity through WAL
func (w *WALEnabledStorageWrapper) LogActivity(ctx context.Context, logEntry *storage.TaskActivityLog) error {
	// Serialize log to JSON for WAL
	logData, err := json.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal activity log: %w", err)
	}

	// Write to WAL first
	if err := w.walManager.WriteEntry(ctx, wal.WALEntryTypeActivityLog, logData, map[string]any{
		"task_id": logEntry.TaskID,
		"dag_id":  logEntry.DAGID,
		"action":  logEntry.Action,
	}); err != nil {
		return fmt.Errorf("failed to write activity log to WAL: %w", err)
	}

	// Delegate to underlying storage (will be applied during flush)
	return w.underlying.LogActivity(ctx, logEntry)
}

// GetTask retrieves a task
func (w *WALEnabledStorageWrapper) GetTask(ctx context.Context, taskID string) (*storage.PersistentTask, error) {
	return w.underlying.GetTask(ctx, taskID)
}

// GetTasksByDAG retrieves tasks by DAG ID
func (w *WALEnabledStorageWrapper) GetTasksByDAG(ctx context.Context, dagID string, limit int, offset int) ([]*storage.PersistentTask, error) {
	return w.underlying.GetTasksByDAG(ctx, dagID, limit, offset)
}

// GetTasksByStatus retrieves tasks by status
func (w *WALEnabledStorageWrapper) GetTasksByStatus(ctx context.Context, dagID string, status storage.TaskStatus) ([]*storage.PersistentTask, error) {
	return w.underlying.GetTasksByStatus(ctx, dagID, status)
}

// UpdateTaskStatus updates task status
func (w *WALEnabledStorageWrapper) UpdateTaskStatus(ctx context.Context, taskID string, status storage.TaskStatus, errorMsg string) error {
	return w.underlying.UpdateTaskStatus(ctx, taskID, status, errorMsg)
}

// DeleteTask deletes a task
func (w *WALEnabledStorageWrapper) DeleteTask(ctx context.Context, taskID string) error {
	return w.underlying.DeleteTask(ctx, taskID)
}

// DeleteTasksByDAG deletes tasks by DAG ID
func (w *WALEnabledStorageWrapper) DeleteTasksByDAG(ctx context.Context, dagID string) error {
	return w.underlying.DeleteTasksByDAG(ctx, dagID)
}

// GetActivityLogs retrieves activity logs
func (w *WALEnabledStorageWrapper) GetActivityLogs(ctx context.Context, taskID string, limit int, offset int) ([]*storage.TaskActivityLog, error) {
	return w.underlying.GetActivityLogs(ctx, taskID, limit, offset)
}

// GetActivityLogsByDAG retrieves activity logs by DAG ID
func (w *WALEnabledStorageWrapper) GetActivityLogsByDAG(ctx context.Context, dagID string, limit int, offset int) ([]*storage.TaskActivityLog, error) {
	return w.underlying.GetActivityLogsByDAG(ctx, dagID, limit, offset)
}

// SaveTasks saves multiple tasks
func (w *WALEnabledStorageWrapper) SaveTasks(ctx context.Context, tasks []*storage.PersistentTask) error {
	return w.underlying.SaveTasks(ctx, tasks)
}

// GetPendingTasks retrieves pending tasks
func (w *WALEnabledStorageWrapper) GetPendingTasks(ctx context.Context, dagID string, limit int) ([]*storage.PersistentTask, error) {
	return w.underlying.GetPendingTasks(ctx, dagID, limit)
}

// GetResumableTasks retrieves resumable tasks
func (w *WALEnabledStorageWrapper) GetResumableTasks(ctx context.Context, dagID string) ([]*storage.PersistentTask, error) {
	return w.underlying.GetResumableTasks(ctx, dagID)
}

// CleanupOldTasks cleans up old tasks
func (w *WALEnabledStorageWrapper) CleanupOldTasks(ctx context.Context, dagID string, olderThan time.Time) error {
	return w.underlying.CleanupOldTasks(ctx, dagID, olderThan)
}

// CleanupOldActivityLogs cleans up old activity logs
func (w *WALEnabledStorageWrapper) CleanupOldActivityLogs(ctx context.Context, dagID string, olderThan time.Time) error {
	return w.underlying.CleanupOldActivityLogs(ctx, dagID, olderThan)
}

// Ping checks storage connectivity
func (w *WALEnabledStorageWrapper) Ping(ctx context.Context) error {
	return w.underlying.Ping(ctx)
}

// Close closes the storage
func (w *WALEnabledStorageWrapper) Close() error {
	return w.underlying.Close()
}

// Flush forces an immediate flush of the WAL buffer
func (w *WALEnabledStorageWrapper) Flush(ctx context.Context) error {
	return w.walManager.Flush(ctx)
}

// Shutdown gracefully shuts down the WAL-enabled storage
func (w *WALEnabledStorageWrapper) Shutdown(ctx context.Context) error {
	// Flush any remaining entries
	if err := w.walManager.Flush(ctx); err != nil {
		// Log error but continue with shutdown
	}

	// Shutdown WAL manager
	if err := w.walManager.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown WAL manager: %w", err)
	}

	return nil
}

// GetWALMetrics returns WAL performance metrics
func (w *WALEnabledStorageWrapper) GetWALMetrics() wal.WALMetrics {
	return w.walManager.GetMetrics()
}
