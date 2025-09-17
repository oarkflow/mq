package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/xid/wuid"
)

// MemoryTaskStorage implements TaskStorage using in-memory storage
type MemoryTaskStorage struct {
	mu           sync.RWMutex
	tasks        map[string]*PersistentTask
	activityLogs map[string][]*TaskActivityLog // taskID -> logs
	dagTasks     map[string][]string           // dagID -> taskIDs
}

// NewMemoryTaskStorage creates a new memory-based task storage
func NewMemoryTaskStorage() *MemoryTaskStorage {
	return &MemoryTaskStorage{
		tasks:        make(map[string]*PersistentTask),
		activityLogs: make(map[string][]*TaskActivityLog),
		dagTasks:     make(map[string][]string),
	}
}

// SaveTask saves a task to memory
func (m *MemoryTaskStorage) SaveTask(ctx context.Context, task *PersistentTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.ID == "" {
		task.ID = wuid.New().String()
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	task.UpdatedAt = time.Now()

	m.tasks[task.ID] = task

	// Add to DAG index
	if _, exists := m.dagTasks[task.DAGID]; !exists {
		m.dagTasks[task.DAGID] = make([]string, 0)
	}
	m.dagTasks[task.DAGID] = append(m.dagTasks[task.DAGID], task.ID)

	return nil
}

// GetTask retrieves a task by ID
func (m *MemoryTaskStorage) GetTask(ctx context.Context, taskID string) (*PersistentTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.tasks[taskID]
	if !exists {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	// Return a copy to prevent external modifications
	taskCopy := *task
	return &taskCopy, nil
}

// GetTasksByDAG retrieves tasks for a specific DAG
func (m *MemoryTaskStorage) GetTasksByDAG(ctx context.Context, dagID string, limit int, offset int) ([]*PersistentTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	taskIDs, exists := m.dagTasks[dagID]
	if !exists {
		return []*PersistentTask{}, nil
	}

	tasks := make([]*PersistentTask, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		if task, exists := m.tasks[taskID]; exists {
			taskCopy := *task
			tasks = append(tasks, &taskCopy)
		}
	}

	// Sort by creation time (newest first)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
	})

	// Apply pagination
	start := offset
	end := offset + limit
	if start > len(tasks) {
		return []*PersistentTask{}, nil
	}
	if end > len(tasks) {
		end = len(tasks)
	}

	return tasks[start:end], nil
}

// GetTasksByStatus retrieves tasks by status for a specific DAG
func (m *MemoryTaskStorage) GetTasksByStatus(ctx context.Context, dagID string, status TaskStatus) ([]*PersistentTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	taskIDs, exists := m.dagTasks[dagID]
	if !exists {
		return []*PersistentTask{}, nil
	}

	tasks := make([]*PersistentTask, 0)
	for _, taskID := range taskIDs {
		if task, exists := m.tasks[taskID]; exists && task.Status == status {
			taskCopy := *task
			tasks = append(tasks, &taskCopy)
		}
	}

	return tasks, nil
}

// UpdateTaskStatus updates the status of a task
func (m *MemoryTaskStorage) UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus, errorMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, exists := m.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	task.Status = status
	task.UpdatedAt = time.Now()

	if status == TaskStatusCompleted || status == TaskStatusFailed {
		now := time.Now()
		task.CompletedAt = &now
	}

	if errorMsg != "" {
		task.Error = errorMsg
	}

	return nil
}

// DeleteTask deletes a task
func (m *MemoryTaskStorage) DeleteTask(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, exists := m.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	delete(m.tasks, taskID)
	delete(m.activityLogs, taskID)

	// Remove from DAG index
	if taskIDs, exists := m.dagTasks[task.DAGID]; exists {
		for i, id := range taskIDs {
			if id == taskID {
				m.dagTasks[task.DAGID] = append(taskIDs[:i], taskIDs[i+1:]...)
				break
			}
		}
	}

	return nil
}

// DeleteTasksByDAG deletes all tasks for a specific DAG
func (m *MemoryTaskStorage) DeleteTasksByDAG(ctx context.Context, dagID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskIDs, exists := m.dagTasks[dagID]
	if !exists {
		return nil
	}

	for _, taskID := range taskIDs {
		delete(m.tasks, taskID)
		delete(m.activityLogs, taskID)
	}

	delete(m.dagTasks, dagID)
	return nil
}

// LogActivity logs an activity for a task
func (m *MemoryTaskStorage) LogActivity(ctx context.Context, logEntry *TaskActivityLog) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if logEntry.ID == "" {
		logEntry.ID = wuid.New().String()
	}
	if logEntry.CreatedAt.IsZero() {
		logEntry.CreatedAt = time.Now()
	}

	if _, exists := m.activityLogs[logEntry.TaskID]; !exists {
		m.activityLogs[logEntry.TaskID] = make([]*TaskActivityLog, 0)
	}

	m.activityLogs[logEntry.TaskID] = append(m.activityLogs[logEntry.TaskID], logEntry)
	return nil
}

// GetActivityLogs retrieves activity logs for a task
func (m *MemoryTaskStorage) GetActivityLogs(ctx context.Context, taskID string, limit int, offset int) ([]*TaskActivityLog, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	logs, exists := m.activityLogs[taskID]
	if !exists {
		return []*TaskActivityLog{}, nil
	}

	// Sort by creation time (newest first)
	sortedLogs := make([]*TaskActivityLog, len(logs))
	copy(sortedLogs, logs)
	sort.Slice(sortedLogs, func(i, j int) bool {
		return sortedLogs[i].CreatedAt.After(sortedLogs[j].CreatedAt)
	})

	// Apply pagination
	start := offset
	end := offset + limit
	if start > len(sortedLogs) {
		return []*TaskActivityLog{}, nil
	}
	if end > len(sortedLogs) {
		end = len(sortedLogs)
	}

	return sortedLogs[start:end], nil
}

// GetActivityLogsByDAG retrieves activity logs for all tasks in a DAG
func (m *MemoryTaskStorage) GetActivityLogsByDAG(ctx context.Context, dagID string, limit int, offset int) ([]*TaskActivityLog, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	taskIDs, exists := m.dagTasks[dagID]
	if !exists {
		return []*TaskActivityLog{}, nil
	}

	allLogs := make([]*TaskActivityLog, 0)
	for _, taskID := range taskIDs {
		if logs, exists := m.activityLogs[taskID]; exists {
			allLogs = append(allLogs, logs...)
		}
	}

	// Sort by creation time (newest first)
	sort.Slice(allLogs, func(i, j int) bool {
		return allLogs[i].CreatedAt.After(allLogs[j].CreatedAt)
	})

	// Apply pagination
	start := offset
	end := offset + limit
	if start > len(allLogs) {
		return []*TaskActivityLog{}, nil
	}
	if end > len(allLogs) {
		end = len(allLogs)
	}

	return allLogs[start:end], nil
}

// SaveTasks saves multiple tasks
func (m *MemoryTaskStorage) SaveTasks(ctx context.Context, tasks []*PersistentTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, task := range tasks {
		if task.ID == "" {
			task.ID = wuid.New().String()
		}
		if task.CreatedAt.IsZero() {
			task.CreatedAt = time.Now()
		}
		task.UpdatedAt = time.Now()

		m.tasks[task.ID] = task

		// Add to DAG index
		if _, exists := m.dagTasks[task.DAGID]; !exists {
			m.dagTasks[task.DAGID] = make([]string, 0)
		}
		m.dagTasks[task.DAGID] = append(m.dagTasks[task.DAGID], task.ID)
	}

	return nil
}

// GetPendingTasks retrieves pending tasks for a DAG
func (m *MemoryTaskStorage) GetPendingTasks(ctx context.Context, dagID string, limit int) ([]*PersistentTask, error) {
	return m.GetTasksByStatus(ctx, dagID, TaskStatusPending)
}

// CleanupOldTasks removes tasks older than the specified time
func (m *MemoryTaskStorage) CleanupOldTasks(ctx context.Context, dagID string, olderThan time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskIDs, exists := m.dagTasks[dagID]
	if !exists {
		return nil
	}

	updatedTaskIDs := make([]string, 0)
	for _, taskID := range taskIDs {
		if task, exists := m.tasks[taskID]; exists {
			if task.CreatedAt.Before(olderThan) {
				delete(m.tasks, taskID)
				delete(m.activityLogs, taskID)
			} else {
				updatedTaskIDs = append(updatedTaskIDs, taskID)
			}
		}
	}

	m.dagTasks[dagID] = updatedTaskIDs
	return nil
}

// CleanupOldActivityLogs removes activity logs older than the specified time
func (m *MemoryTaskStorage) CleanupOldActivityLogs(ctx context.Context, dagID string, olderThan time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskIDs, exists := m.dagTasks[dagID]
	if !exists {
		return nil
	}

	for _, taskID := range taskIDs {
		if logs, exists := m.activityLogs[taskID]; exists {
			updatedLogs := make([]*TaskActivityLog, 0)
			for _, log := range logs {
				if log.CreatedAt.After(olderThan) {
					updatedLogs = append(updatedLogs, log)
				}
			}
			if len(updatedLogs) > 0 {
				m.activityLogs[taskID] = updatedLogs
			} else {
				delete(m.activityLogs, taskID)
			}
		}
	}

	return nil
}

// GetResumableTasks gets tasks that can be resumed (pending or running status)
func (m *MemoryTaskStorage) GetResumableTasks(ctx context.Context, dagID string) ([]*PersistentTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	taskIDs, exists := m.dagTasks[dagID]
	if !exists {
		return []*PersistentTask{}, nil
	}

	var resumableTasks []*PersistentTask
	for _, taskID := range taskIDs {
		if task, exists := m.tasks[taskID]; exists {
			if task.Status == TaskStatusPending || task.Status == TaskStatusRunning {
				// Return a copy to prevent external modifications
				taskCopy := *task
				resumableTasks = append(resumableTasks, &taskCopy)
			}
		}
	}

	return resumableTasks, nil
}

// Ping checks if the storage is healthy
func (m *MemoryTaskStorage) Ping(ctx context.Context) error {
	return nil // Memory storage is always healthy
}

// Close closes the storage (no-op for memory)
func (m *MemoryTaskStorage) Close() error {
	return nil
}
