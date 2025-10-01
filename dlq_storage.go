package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
)

// DLQStorage defines the interface for Dead Letter Queue persistent storage
type DLQStorage interface {
	// Store persists a failed task to the DLQ
	Store(ctx context.Context, task *DLQEntry) error

	// Get retrieves a task from the DLQ by ID
	Get(ctx context.Context, taskID string) (*DLQEntry, error)

	// List returns a paginated list of DLQ entries
	List(ctx context.Context, offset, limit int, filter DLQFilter) ([]*DLQEntry, error)

	// Delete removes a task from the DLQ
	Delete(ctx context.Context, taskID string) error

	// DeleteOlderThan removes entries older than the specified duration
	DeleteOlderThan(ctx context.Context, duration time.Duration) (int, error)

	// Count returns the total number of entries
	Count(ctx context.Context, filter DLQFilter) (int64, error)

	// Close closes the storage
	Close() error
}

// DLQEntry represents a dead letter queue entry
type DLQEntry struct {
	TaskID          string            `json:"task_id"`
	QueueName       string            `json:"queue_name"`
	OriginalPayload json.RawMessage   `json:"original_payload"`
	ErrorMessage    string            `json:"error_message"`
	ErrorType       string            `json:"error_type"`
	FailedAt        time.Time         `json:"failed_at"`
	RetryCount      int               `json:"retry_count"`
	LastRetryAt     time.Time         `json:"last_retry_at,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	Priority        int               `json:"priority"`
	TraceID         string            `json:"trace_id,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
	ReprocessCount  int               `json:"reprocess_count"`
}

// DLQFilter for filtering DLQ entries
type DLQFilter struct {
	QueueName  string
	ErrorType  string
	FromDate   time.Time
	ToDate     time.Time
	MinRetries int
	MaxRetries int
}

// FileDLQStorage implements file-based DLQ storage
type FileDLQStorage struct {
	baseDir string
	mu      sync.RWMutex
	logger  logger.Logger
	index   map[string]*DLQEntry // In-memory index for fast lookups
}

// NewFileDLQStorage creates a new file-based DLQ storage
func NewFileDLQStorage(baseDir string, log logger.Logger) (*FileDLQStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create DLQ directory: %w", err)
	}

	storage := &FileDLQStorage{
		baseDir: baseDir,
		logger:  log,
		index:   make(map[string]*DLQEntry),
	}

	// Load existing entries into index
	if err := storage.loadIndex(); err != nil {
		return nil, fmt.Errorf("failed to load DLQ index: %w", err)
	}

	return storage, nil
}

// Store persists a DLQ entry to disk
func (f *FileDLQStorage) Store(ctx context.Context, entry *DLQEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Serialize entry
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ entry: %w", err)
	}

	// Create file path based on queue name and task ID
	queueDir := filepath.Join(f.baseDir, entry.QueueName)
	if err := os.MkdirAll(queueDir, 0755); err != nil {
		return fmt.Errorf("failed to create queue directory: %w", err)
	}

	filePath := filepath.Join(queueDir, fmt.Sprintf("%s.json", entry.TaskID))

	// Write atomically using temp file
	tempPath := filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write DLQ entry: %w", err)
	}

	if err := os.Rename(tempPath, filePath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename DLQ entry: %w", err)
	}

	// Update index
	f.index[entry.TaskID] = entry

	f.logger.Info("DLQ entry persisted",
		logger.Field{Key: "taskID", Value: entry.TaskID},
		logger.Field{Key: "queue", Value: entry.QueueName})

	return nil
}

// Get retrieves a DLQ entry by task ID
func (f *FileDLQStorage) Get(ctx context.Context, taskID string) (*DLQEntry, error) {
	f.mu.RLock()
	entry, exists := f.index[taskID]
	f.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("DLQ entry not found: %s", taskID)
	}

	return entry, nil
}

// List returns paginated DLQ entries
func (f *FileDLQStorage) List(ctx context.Context, offset, limit int, filter DLQFilter) ([]*DLQEntry, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var entries []*DLQEntry
	for _, entry := range f.index {
		if f.matchesFilter(entry, filter) {
			entries = append(entries, entry)
		}
	}

	// Sort by failed_at descending (newest first)
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].FailedAt.Before(entries[j].FailedAt) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	// Apply pagination
	start := offset
	if start >= len(entries) {
		return []*DLQEntry{}, nil
	}

	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}

	return entries[start:end], nil
}

// Delete removes a DLQ entry
func (f *FileDLQStorage) Delete(ctx context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, exists := f.index[taskID]
	if !exists {
		return fmt.Errorf("DLQ entry not found: %s", taskID)
	}

	// Delete file
	filePath := filepath.Join(f.baseDir, entry.QueueName, fmt.Sprintf("%s.json", taskID))
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete DLQ entry file: %w", err)
	}

	// Remove from index
	delete(f.index, taskID)

	f.logger.Info("DLQ entry deleted", logger.Field{Key: "taskID", Value: taskID})

	return nil
}

// DeleteOlderThan removes entries older than specified duration
func (f *FileDLQStorage) DeleteOlderThan(ctx context.Context, duration time.Duration) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	cutoff := time.Now().Add(-duration)
	deleted := 0

	for taskID, entry := range f.index {
		if entry.FailedAt.Before(cutoff) {
			filePath := filepath.Join(f.baseDir, entry.QueueName, fmt.Sprintf("%s.json", taskID))
			if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
				f.logger.Error("Failed to delete old DLQ entry",
					logger.Field{Key: "error", Value: err},
					logger.Field{Key: "taskID", Value: taskID})
				continue
			}
			delete(f.index, taskID)
			deleted++
		}
	}

	f.logger.Info("Deleted old DLQ entries", logger.Field{Key: "count", Value: deleted})

	return deleted, nil
}

// Count returns the total number of DLQ entries
func (f *FileDLQStorage) Count(ctx context.Context, filter DLQFilter) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	count := int64(0)
	for _, entry := range f.index {
		if f.matchesFilter(entry, filter) {
			count++
		}
	}

	return count, nil
}

// Close closes the storage
func (f *FileDLQStorage) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.index = nil
	return nil
}

// loadIndex loads existing DLQ entries into memory index
func (f *FileDLQStorage) loadIndex() error {
	return filepath.Walk(f.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			f.logger.Error("Failed to read DLQ entry",
				logger.Field{Key: "error", Value: err},
				logger.Field{Key: "path", Value: path})
			return nil
		}

		var entry DLQEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			f.logger.Error("Failed to unmarshal DLQ entry",
				logger.Field{Key: "error", Value: err},
				logger.Field{Key: "path", Value: path})
			return nil
		}

		f.index[entry.TaskID] = &entry
		return nil
	})
}

// matchesFilter checks if an entry matches the filter
func (f *FileDLQStorage) matchesFilter(entry *DLQEntry, filter DLQFilter) bool {
	if filter.QueueName != "" && entry.QueueName != filter.QueueName {
		return false
	}
	if filter.ErrorType != "" && entry.ErrorType != filter.ErrorType {
		return false
	}
	if !filter.FromDate.IsZero() && entry.FailedAt.Before(filter.FromDate) {
		return false
	}
	if !filter.ToDate.IsZero() && entry.FailedAt.After(filter.ToDate) {
		return false
	}
	if filter.MinRetries > 0 && entry.RetryCount < filter.MinRetries {
		return false
	}
	if filter.MaxRetries > 0 && entry.RetryCount > filter.MaxRetries {
		return false
	}
	return true
}

// InMemoryDLQStorage implements in-memory DLQ storage (for testing or small scale)
type InMemoryDLQStorage struct {
	entries map[string]*DLQEntry
	mu      sync.RWMutex
}

// NewInMemoryDLQStorage creates a new in-memory DLQ storage
func NewInMemoryDLQStorage() *InMemoryDLQStorage {
	return &InMemoryDLQStorage{
		entries: make(map[string]*DLQEntry),
	}
}

func (m *InMemoryDLQStorage) Store(ctx context.Context, entry *DLQEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[entry.TaskID] = entry
	return nil
}

func (m *InMemoryDLQStorage) Get(ctx context.Context, taskID string) (*DLQEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, exists := m.entries[taskID]
	if !exists {
		return nil, fmt.Errorf("DLQ entry not found: %s", taskID)
	}
	return entry, nil
}

func (m *InMemoryDLQStorage) List(ctx context.Context, offset, limit int, filter DLQFilter) ([]*DLQEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var entries []*DLQEntry
	for _, entry := range m.entries {
		entries = append(entries, entry)
	}

	start := offset
	if start >= len(entries) {
		return []*DLQEntry{}, nil
	}

	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}

	return entries[start:end], nil
}

func (m *InMemoryDLQStorage) Delete(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, taskID)
	return nil
}

func (m *InMemoryDLQStorage) DeleteOlderThan(ctx context.Context, duration time.Duration) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().Add(-duration)
	deleted := 0

	for taskID, entry := range m.entries {
		if entry.FailedAt.Before(cutoff) {
			delete(m.entries, taskID)
			deleted++
		}
	}

	return deleted, nil
}

func (m *InMemoryDLQStorage) Count(ctx context.Context, filter DLQFilter) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.entries)), nil
}

func (m *InMemoryDLQStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = nil
	return nil
}
