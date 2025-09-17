package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"           // PostgreSQL driver
	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"github.com/oarkflow/json"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/xid/wuid"
)

// SQLTaskStorage implements TaskStorage using SQL databases
type SQLTaskStorage struct {
	db     *squealx.DB
	config *TaskStorageConfig
}

// NewSQLTaskStorage creates a new SQL-based task storage
func NewSQLTaskStorage(config *TaskStorageConfig) (*SQLTaskStorage, error) {
	db, err := squealx.Open(config.Type, config.DSN, "task-storage")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns)
	}
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns)
	}
	if config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(config.ConnMaxLifetime)
	}

	storage := &SQLTaskStorage{
		db:     db,
		config: config,
	}

	// Create tables
	if err := storage.createTables(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return storage, nil
}

// createTables creates the necessary database tables
func (s *SQLTaskStorage) createTables(ctx context.Context) error {
	tasksTable := `
		CREATE TABLE IF NOT EXISTS dag_tasks (
			id TEXT PRIMARY KEY,
			dag_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			current_node_id TEXT,
			sub_dag_path TEXT,
			processing_state TEXT,
			payload TEXT,
			status TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			started_at TIMESTAMP,
			completed_at TIMESTAMP,
			error TEXT,
			retry_count INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 3,
			priority INTEGER DEFAULT 0
		)`

	activityLogsTable := `
		CREATE TABLE IF NOT EXISTS dag_task_activity_logs (
			id TEXT PRIMARY KEY,
			task_id TEXT NOT NULL,
			dag_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			action TEXT NOT NULL,
			message TEXT,
			data TEXT,
			level TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			FOREIGN KEY (task_id) REFERENCES dag_tasks(id) ON DELETE CASCADE
		)`

	// Create indexes for better performance
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_dag_tasks_dag_id ON dag_tasks(dag_id)`,
		`CREATE INDEX IF NOT EXISTS idx_dag_tasks_status ON dag_tasks(status)`,
		`CREATE INDEX IF NOT EXISTS idx_dag_tasks_created_at ON dag_tasks(created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_activity_logs_task_id ON dag_task_activity_logs(task_id)`,
		`CREATE INDEX IF NOT EXISTS idx_activity_logs_dag_id ON dag_task_activity_logs(dag_id)`,
		`CREATE INDEX IF NOT EXISTS idx_activity_logs_created_at ON dag_task_activity_logs(created_at)`,
	}

	// Execute table creation
	if _, err := s.db.ExecContext(ctx, tasksTable); err != nil {
		return fmt.Errorf("failed to create tasks table: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, activityLogsTable); err != nil {
		return fmt.Errorf("failed to create activity logs table: %w", err)
	}

	// Execute index creation
	for _, index := range indexes {
		if _, err := s.db.ExecContext(ctx, index); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// SaveTask saves a task to the database
func (s *SQLTaskStorage) SaveTask(ctx context.Context, task *PersistentTask) error {
	if task.ID == "" {
		task.ID = wuid.New().String()
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	task.UpdatedAt = time.Now()

	query := `
		INSERT INTO dag_tasks (id, dag_id, node_id, current_node_id, sub_dag_path, processing_state,
		                      payload, status, created_at, updated_at, started_at, completed_at,
		                      error, retry_count, max_retries, priority)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			node_id = excluded.node_id,
			current_node_id = excluded.current_node_id,
			sub_dag_path = excluded.sub_dag_path,
			processing_state = excluded.processing_state,
			payload = excluded.payload,
			status = excluded.status,
			updated_at = excluded.updated_at,
			started_at = excluded.started_at,
			completed_at = excluded.completed_at,
			error = excluded.error,
			retry_count = excluded.retry_count,
			max_retries = excluded.max_retries,
			priority = excluded.priority`

	_, err := s.db.ExecContext(ctx, s.placeholderQuery(query),
		task.ID, task.DAGID, task.NodeID, task.CurrentNodeID, task.SubDAGPath, task.ProcessingState,
		string(task.Payload), task.Status, task.CreatedAt, task.UpdatedAt, task.StartedAt, task.CompletedAt,
		task.Error, task.RetryCount, task.MaxRetries, task.Priority)

	return err
}

// GetTask retrieves a task by ID
func (s *SQLTaskStorage) GetTask(ctx context.Context, taskID string) (*PersistentTask, error) {
	query := `
		SELECT id, dag_id, node_id, current_node_id, sub_dag_path, processing_state,
		       payload, status, created_at, updated_at, started_at, completed_at,
		       error, retry_count, max_retries, priority
		FROM dag_tasks WHERE id = ?`

	var task PersistentTask
	var payload sql.NullString
	var currentNodeID, subDAGPath, processingState sql.NullString
	var startedAt, completedAt sql.NullTime
	var error sql.NullString

	err := s.db.QueryRowContext(ctx, query, taskID).Scan(
		&task.ID, &task.DAGID, &task.NodeID, &currentNodeID, &subDAGPath, &processingState,
		&payload, &task.Status, &task.CreatedAt, &task.UpdatedAt, &startedAt, &completedAt,
		&error, &task.RetryCount, &task.MaxRetries, &task.Priority)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("task not found: %s", taskID)
		}
		return nil, err
	}

	// Handle nullable fields
	if currentNodeID.Valid {
		task.CurrentNodeID = currentNodeID.String
	}
	if subDAGPath.Valid {
		task.SubDAGPath = subDAGPath.String
	}
	if processingState.Valid {
		task.ProcessingState = processingState.String
	}

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("task not found: %s", taskID)
		}
		return nil, err
	}

	if payload.Valid {
		task.Payload = []byte(payload.String)
	}
	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}
	if error.Valid {
		task.Error = error.String
	}

	return &task, nil
}

// GetTasksByDAG retrieves tasks for a specific DAG
func (s *SQLTaskStorage) GetTasksByDAG(ctx context.Context, dagID string, limit int, offset int) ([]*PersistentTask, error) {
	query := `
		SELECT id, dag_id, node_id, payload, status, created_at, updated_at,
		       started_at, completed_at, error, retry_count, max_retries, priority
		FROM dag_tasks
		WHERE dag_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`

	rows, err := s.db.QueryContext(ctx, query, dagID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := make([]*PersistentTask, 0)
	for rows.Next() {
		var task PersistentTask
		var payload sql.NullString
		var startedAt, completedAt sql.NullTime
		var error sql.NullString

		err := rows.Scan(
			&task.ID, &task.DAGID, &task.NodeID, &payload, &task.Status,
			&task.CreatedAt, &task.UpdatedAt, &startedAt, &completedAt,
			&error, &task.RetryCount, &task.MaxRetries, &task.Priority)

		if err != nil {
			return nil, err
		}

		if payload.Valid {
			task.Payload = []byte(payload.String)
		}
		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			task.CompletedAt = &completedAt.Time
		}
		if error.Valid {
			task.Error = error.String
		}

		tasks = append(tasks, &task)
	}

	return tasks, rows.Err()
}

// GetTasksByStatus retrieves tasks by status for a specific DAG
func (s *SQLTaskStorage) GetTasksByStatus(ctx context.Context, dagID string, status TaskStatus) ([]*PersistentTask, error) {
	query := `
		SELECT id, dag_id, node_id, payload, status, created_at, updated_at,
		       started_at, completed_at, error, retry_count, max_retries, priority
		FROM dag_tasks
		WHERE dag_id = ? AND status = ?
		ORDER BY created_at DESC`

	rows, err := s.db.QueryContext(ctx, query, dagID, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := make([]*PersistentTask, 0)
	for rows.Next() {
		var task PersistentTask
		var payload sql.NullString
		var startedAt, completedAt sql.NullTime
		var error sql.NullString

		err := rows.Scan(
			&task.ID, &task.DAGID, &task.NodeID, &payload, &task.Status,
			&task.CreatedAt, &task.UpdatedAt, &startedAt, &completedAt,
			&error, &task.RetryCount, &task.MaxRetries, &task.Priority)

		if err != nil {
			return nil, err
		}

		if payload.Valid {
			task.Payload = []byte(payload.String)
		}
		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			task.CompletedAt = &completedAt.Time
		}
		if error.Valid {
			task.Error = error.String
		}

		tasks = append(tasks, &task)
	}

	return tasks, rows.Err()
}

// UpdateTaskStatus updates the status of a task
func (s *SQLTaskStorage) UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus, errorMsg string) error {
	now := time.Now()
	query := `
		UPDATE dag_tasks
		SET status = ?, updated_at = ?, completed_at = ?, error = ?
		WHERE id = ?`

	_, err := s.db.ExecContext(ctx, query, status, now, now, errorMsg, taskID)
	return err
}

// DeleteTask deletes a task
func (s *SQLTaskStorage) DeleteTask(ctx context.Context, taskID string) error {
	query := `DELETE FROM dag_tasks WHERE id = ?`
	_, err := s.db.ExecContext(ctx, query, taskID)
	return err
}

// DeleteTasksByDAG deletes all tasks for a specific DAG
func (s *SQLTaskStorage) DeleteTasksByDAG(ctx context.Context, dagID string) error {
	query := `DELETE FROM dag_tasks WHERE dag_id = ?`
	_, err := s.db.ExecContext(ctx, query, dagID)
	return err
}

// LogActivity logs an activity for a task
func (s *SQLTaskStorage) LogActivity(ctx context.Context, logEntry *TaskActivityLog) error {
	if logEntry.ID == "" {
		logEntry.ID = wuid.New().String()
	}
	if logEntry.CreatedAt.IsZero() {
		logEntry.CreatedAt = time.Now()
	}

	query := `
		INSERT INTO dag_task_activity_logs (id, task_id, dag_id, node_id, action, message, data, level, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		logEntry.ID, logEntry.TaskID, logEntry.DAGID, logEntry.NodeID,
		logEntry.Action, logEntry.Message, string(logEntry.Data), logEntry.Level, logEntry.CreatedAt)

	return err
}

// GetActivityLogs retrieves activity logs for a task
func (s *SQLTaskStorage) GetActivityLogs(ctx context.Context, taskID string, limit int, offset int) ([]*TaskActivityLog, error) {
	query := `
		SELECT id, task_id, dag_id, node_id, action, message, data, level, created_at
		FROM dag_task_activity_logs
		WHERE task_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`

	rows, err := s.db.QueryContext(ctx, query, taskID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logs := make([]*TaskActivityLog, 0)
	for rows.Next() {
		var log TaskActivityLog
		var message, data sql.NullString

		err := rows.Scan(
			&log.ID, &log.TaskID, &log.DAGID, &log.NodeID, &log.Action,
			&message, &data, &log.Level, &log.CreatedAt)

		if err != nil {
			return nil, err
		}

		if message.Valid {
			log.Message = message.String
		}
		if data.Valid {
			log.Data = []byte(data.String)
		}

		logs = append(logs, &log)
	}

	return logs, rows.Err()
}

// GetActivityLogsByDAG retrieves activity logs for all tasks in a DAG
func (s *SQLTaskStorage) GetActivityLogsByDAG(ctx context.Context, dagID string, limit int, offset int) ([]*TaskActivityLog, error) {
	query := `
		SELECT id, task_id, dag_id, node_id, action, message, data, level, created_at
		FROM dag_task_activity_logs
		WHERE dag_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`

	rows, err := s.db.QueryContext(ctx, query, dagID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logs := make([]*TaskActivityLog, 0)
	for rows.Next() {
		var log TaskActivityLog
		var message, data sql.NullString

		err := rows.Scan(
			&log.ID, &log.TaskID, &log.DAGID, &log.NodeID, &log.Action,
			&message, &data, &log.Level, &log.CreatedAt)

		if err != nil {
			return nil, err
		}

		if message.Valid {
			log.Message = message.String
		}
		if data.Valid {
			log.Data = []byte(data.String)
		}

		logs = append(logs, &log)
	}

	return logs, rows.Err()
}

// SaveTasks saves multiple tasks
func (s *SQLTaskStorage) SaveTasks(ctx context.Context, tasks []*PersistentTask) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, task := range tasks {
		if task.ID == "" {
			task.ID = wuid.New().String()
		}
		if task.CreatedAt.IsZero() {
			task.CreatedAt = time.Now()
		}
		task.UpdatedAt = time.Now()

		query := `
			INSERT INTO dag_tasks (id, dag_id, node_id, payload, status, created_at, updated_at,
			                      started_at, completed_at, error, retry_count, max_retries, priority)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET
				node_id = excluded.node_id,
				payload = excluded.payload,
				status = excluded.status,
				updated_at = excluded.updated_at,
				started_at = excluded.started_at,
				completed_at = excluded.completed_at,
				error = excluded.error,
				retry_count = excluded.retry_count,
				max_retries = excluded.max_retries,
				priority = excluded.priority`

		_, err := tx.ExecContext(ctx, s.placeholderQuery(query),
			task.ID, task.DAGID, task.NodeID, string(task.Payload), task.Status,
			task.CreatedAt, task.UpdatedAt, task.StartedAt, task.CompletedAt,
			task.Error, task.RetryCount, task.MaxRetries, task.Priority)

		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetPendingTasks retrieves pending tasks for a DAG
func (s *SQLTaskStorage) GetPendingTasks(ctx context.Context, dagID string, limit int) ([]*PersistentTask, error) {
	query := `
		SELECT id, dag_id, node_id, payload, status, created_at, updated_at,
		       started_at, completed_at, error, retry_count, max_retries, priority
		FROM dag_tasks
		WHERE dag_id = ? AND status = ?
		ORDER BY priority DESC, created_at ASC
		LIMIT ?`

	rows, err := s.db.QueryContext(ctx, query, dagID, TaskStatusPending, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := make([]*PersistentTask, 0)
	for rows.Next() {
		var task PersistentTask
		var payload sql.NullString
		var startedAt, completedAt sql.NullTime
		var error sql.NullString

		err := rows.Scan(
			&task.ID, &task.DAGID, &task.NodeID, &payload, &task.Status,
			&task.CreatedAt, &task.UpdatedAt, &startedAt, &completedAt,
			&error, &task.RetryCount, &task.MaxRetries, &task.Priority)

		if err != nil {
			return nil, err
		}

		if payload.Valid {
			task.Payload = []byte(payload.String)
		}
		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			task.CompletedAt = &completedAt.Time
		}
		if error.Valid {
			task.Error = error.String
		}

		tasks = append(tasks, &task)
	}

	return tasks, rows.Err()
}

// CleanupOldTasks removes tasks older than the specified time
func (s *SQLTaskStorage) CleanupOldTasks(ctx context.Context, dagID string, olderThan time.Time) error {
	query := `DELETE FROM dag_tasks WHERE dag_id = ? AND created_at < ?`
	_, err := s.db.ExecContext(ctx, query, dagID, olderThan)
	return err
}

// CleanupOldActivityLogs removes activity logs older than the specified time
func (s *SQLTaskStorage) CleanupOldActivityLogs(ctx context.Context, dagID string, olderThan time.Time) error {
	query := `DELETE FROM dag_task_activity_logs WHERE dag_id = ? AND created_at < ?`
	_, err := s.db.ExecContext(ctx, query, dagID, olderThan)
	return err
}

// GetResumableTasks gets tasks that can be resumed (pending or running status)
func (s *SQLTaskStorage) GetResumableTasks(ctx context.Context, dagID string) ([]*PersistentTask, error) {
	query := `
		SELECT id, dag_id, node_id, current_node_id, sub_dag_path, processing_state,
		       payload, status, created_at, updated_at, started_at, completed_at,
		       error, retry_count, max_retries, priority
		FROM dag_tasks
		WHERE dag_id = ? AND status IN (?, ?)
		ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, s.placeholderQuery(query), dagID, TaskStatusPending, TaskStatusRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*PersistentTask
	for rows.Next() {
		var task PersistentTask
		var payload sql.NullString
		var currentNodeID, subDAGPath, processingState sql.NullString
		var startedAt, completedAt sql.NullTime
		var error sql.NullString

		err := rows.Scan(
			&task.ID, &task.DAGID, &task.NodeID, &currentNodeID, &subDAGPath, &processingState,
			&payload, &task.Status, &task.CreatedAt, &task.UpdatedAt, &startedAt, &completedAt,
			&error, &task.RetryCount, &task.MaxRetries, &task.Priority)
		if err != nil {
			return nil, err
		}

		// Handle nullable fields
		if payload.Valid {
			task.Payload = json.RawMessage(payload.String)
		}
		if currentNodeID.Valid {
			task.CurrentNodeID = currentNodeID.String
		}
		if subDAGPath.Valid {
			task.SubDAGPath = subDAGPath.String
		}
		if processingState.Valid {
			task.ProcessingState = processingState.String
		}
		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			task.CompletedAt = &completedAt.Time
		}
		if error.Valid {
			task.Error = error.String
		}

		tasks = append(tasks, &task)
	}

	return tasks, rows.Err()
}

// Ping checks if the database is healthy
func (s *SQLTaskStorage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection
func (s *SQLTaskStorage) Close() error {
	return s.db.Close()
}

// placeholderQuery converts ? placeholders to the appropriate format for the database
func (s *SQLTaskStorage) placeholderQuery(query string) string {
	if s.config.Type == "postgres" {
		return strings.ReplaceAll(query, "?", "$1")
	}
	return query // SQLite uses ?
}

// GetDB returns the underlying database connection
func (s *SQLTaskStorage) GetDB() *sql.DB {
	return s.db.DB()
}
