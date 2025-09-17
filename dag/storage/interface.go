package storage

import (
	"context"
	"time"

	"github.com/oarkflow/json"
)

// TaskStatus represents the status of a task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// PersistentTask represents a task that can be stored persistently
type PersistentTask struct {
	ID              string          `json:"id" db:"id"`
	DAGID           string          `json:"dag_id" db:"dag_id"`
	NodeID          string          `json:"node_id" db:"node_id"`
	CurrentNodeID   string          `json:"current_node_id" db:"current_node_id"`   // Node where task is currently processing
	SubDAGPath      string          `json:"sub_dag_path" db:"sub_dag_path"`         // Path through nested DAGs (e.g., "subdag1.subdag2")
	ProcessingState string          `json:"processing_state" db:"processing_state"` // Current processing state (pending, processing, waiting, etc.)
	Payload         json.RawMessage `json:"payload" db:"payload"`
	Status          TaskStatus      `json:"status" db:"status"`
	CreatedAt       time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at" db:"updated_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty" db:"started_at"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty" db:"completed_at"`
	Error           string          `json:"error,omitempty" db:"error"`
	RetryCount      int             `json:"retry_count" db:"retry_count"`
	MaxRetries      int             `json:"max_retries" db:"max_retries"`
	Priority        int             `json:"priority" db:"priority"`
}

// TaskActivityLog represents an activity log entry for a task
type TaskActivityLog struct {
	ID        string          `json:"id" db:"id"`
	TaskID    string          `json:"task_id" db:"task_id"`
	DAGID     string          `json:"dag_id" db:"dag_id"`
	NodeID    string          `json:"node_id" db:"node_id"`
	Action    string          `json:"action" db:"action"`
	Message   string          `json:"message" db:"message"`
	Data      json.RawMessage `json:"data,omitempty" db:"data"`
	Level     string          `json:"level" db:"level"`
	CreatedAt time.Time       `json:"created_at" db:"created_at"`
}

// TaskStorage defines the interface for task storage operations
type TaskStorage interface {
	// Task operations
	SaveTask(ctx context.Context, task *PersistentTask) error
	GetTask(ctx context.Context, taskID string) (*PersistentTask, error)
	GetTasksByDAG(ctx context.Context, dagID string, limit int, offset int) ([]*PersistentTask, error)
	GetTasksByStatus(ctx context.Context, dagID string, status TaskStatus) ([]*PersistentTask, error)
	UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus, errorMsg string) error
	DeleteTask(ctx context.Context, taskID string) error
	DeleteTasksByDAG(ctx context.Context, dagID string) error

	// Activity logging
	LogActivity(ctx context.Context, log *TaskActivityLog) error
	GetActivityLogs(ctx context.Context, taskID string, limit int, offset int) ([]*TaskActivityLog, error)
	GetActivityLogsByDAG(ctx context.Context, dagID string, limit int, offset int) ([]*TaskActivityLog, error)

	// Batch operations
	SaveTasks(ctx context.Context, tasks []*PersistentTask) error
	GetPendingTasks(ctx context.Context, dagID string, limit int) ([]*PersistentTask, error)

	// Recovery operations
	GetResumableTasks(ctx context.Context, dagID string) ([]*PersistentTask, error)

	// Cleanup operations
	CleanupOldTasks(ctx context.Context, dagID string, olderThan time.Time) error
	CleanupOldActivityLogs(ctx context.Context, dagID string, olderThan time.Time) error

	// Health check
	Ping(ctx context.Context) error
	Close() error
}

// TaskStorageConfig holds configuration for task storage
type TaskStorageConfig struct {
	Type            string        // "memory", "postgres", "sqlite"
	DSN             string        // Database connection string
	MaxOpenConns    int           // Maximum open connections
	MaxIdleConns    int           // Maximum idle connections
	ConnMaxLifetime time.Duration // Connection max lifetime
}

// StorageOption is a function that configures TaskStorageConfig
type StorageOption func(*TaskStorageConfig)

// WithMaxOpenConns sets the maximum number of open connections
func WithMaxOpenConns(maxOpen int) StorageOption {
	return func(config *TaskStorageConfig) {
		config.MaxOpenConns = maxOpen
	}
}

// WithMaxIdleConns sets the maximum number of idle connections
func WithMaxIdleConns(maxIdle int) StorageOption {
	return func(config *TaskStorageConfig) {
		config.MaxIdleConns = maxIdle
	}
}

// WithConnMaxLifetime sets the maximum lifetime of connections
func WithConnMaxLifetime(lifetime time.Duration) StorageOption {
	return func(config *TaskStorageConfig) {
		config.ConnMaxLifetime = lifetime
	}
}
