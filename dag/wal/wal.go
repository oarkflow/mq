package wal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq/dag/storage"
)

// WALEntryType represents the type of WAL entry
type WALEntryType string

const (
	WALEntryTypeTaskUpdate  WALEntryType = "task_update"
	WALEntryTypeActivityLog WALEntryType = "activity_log"
	WALEntryTypeTaskDelete  WALEntryType = "task_delete"
	WALEntryTypeBatchUpdate WALEntryType = "batch_update"
)

// WALEntry represents a single entry in the Write-Ahead Log
type WALEntry struct {
	ID         string                 `json:"id"`
	Type       WALEntryType           `json:"type"`
	Timestamp  time.Time              `json:"timestamp"`
	SequenceID uint64                 `json:"sequence_id"`
	Data       json.RawMessage        `json:"data"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	Checksum   string                 `json:"checksum"`
}

// WALSegment represents a segment of WAL entries
type WALSegment struct {
	ID         string        `json:"id"`
	StartSeqID uint64        `json:"start_seq_id"`
	EndSeqID   uint64        `json:"end_seq_id"`
	Entries    []WALEntry    `json:"entries"`
	CreatedAt  time.Time     `json:"created_at"`
	FlushedAt  *time.Time    `json:"flushed_at,omitempty"`
	Status     SegmentStatus `json:"status"`
	Checksum   string        `json:"checksum"`
}

// SegmentStatus represents the status of a WAL segment
type SegmentStatus string

const (
	SegmentStatusActive   SegmentStatus = "active"
	SegmentStatusFlushing SegmentStatus = "flushing"
	SegmentStatusFlushed  SegmentStatus = "flushed"
	SegmentStatusFailed   SegmentStatus = "failed"
)

// WALConfig holds configuration for the WAL system
type WALConfig struct {
	// Buffer configuration
	MaxBufferSize   int           `json:"max_buffer_size"`   // Maximum entries in buffer before flush
	FlushInterval   time.Duration `json:"flush_interval"`    // How often to flush buffer
	MaxFlushRetries int           `json:"max_flush_retries"` // Max retries for failed flushes

	// Segment configuration
	MaxSegmentSize   int           `json:"max_segment_size"`  // Maximum entries per segment
	SegmentRetention time.Duration `json:"segment_retention"` // How long to keep flushed segments

	// Performance tuning
	WorkerCount int `json:"worker_count"` // Number of flush workers
	BatchSize   int `json:"batch_size"`   // Batch size for database operations

	// Recovery configuration
	EnableRecovery  bool          `json:"enable_recovery"`  // Enable WAL recovery on startup
	RecoveryTimeout time.Duration `json:"recovery_timeout"` // Timeout for recovery operations

	// Monitoring
	EnableMetrics   bool          `json:"enable_metrics"`   // Enable metrics collection
	MetricsInterval time.Duration `json:"metrics_interval"` // Metrics collection interval
}

// DefaultWALConfig returns default WAL configuration
func DefaultWALConfig() *WALConfig {
	return &WALConfig{
		MaxBufferSize:    1000,
		FlushInterval:    5 * time.Second,
		MaxFlushRetries:  3,
		MaxSegmentSize:   5000,
		SegmentRetention: 24 * time.Hour,
		WorkerCount:      2,
		BatchSize:        100,
		EnableRecovery:   true,
		RecoveryTimeout:  30 * time.Second,
		EnableMetrics:    true,
		MetricsInterval:  10 * time.Second,
	}
}

// WALManager manages the Write-Ahead Log system
type WALManager struct {
	config        *WALConfig
	storage       WALStorage
	buffer        chan WALEntry
	segments      map[string]*WALSegment
	currentSeqID  uint64
	activeSegment *WALSegment

	// Control channels
	flushTrigger chan struct{}
	shutdown     chan struct{}
	done         chan struct{}

	// Workers
	flushWorkers []chan WALSegment

	// Synchronization
	mu sync.RWMutex
	wg sync.WaitGroup

	// Metrics
	metrics *WALMetrics

	// Callbacks
	onFlush    func(segment *WALSegment) error
	onRecovery func(entries []WALEntry) error
}

// WALStorage defines the interface for WAL storage operations
type WALStorage interface {
	// WAL operations
	SaveWALEntry(ctx context.Context, entry *WALEntry) error
	SaveWALEntries(ctx context.Context, entries []WALEntry) error
	SaveWALSegment(ctx context.Context, segment *WALSegment) error

	// Recovery operations
	GetWALSegments(ctx context.Context, since time.Time) ([]WALSegment, error)
	GetUnflushedEntries(ctx context.Context) ([]WALEntry, error)

	// Cleanup operations
	DeleteOldSegments(ctx context.Context, olderThan time.Time) error

	// Task operations (delegated to underlying storage)
	SaveTask(ctx context.Context, task *storage.PersistentTask) error
	LogActivity(ctx context.Context, log *storage.TaskActivityLog) error
}

// WALMetrics holds metrics for the WAL system
type WALMetrics struct {
	EntriesBuffered    int64
	EntriesFlushed     int64
	FlushOperations    int64
	FlushErrors        int64
	RecoveryOperations int64
	RecoveryErrors     int64
	AverageFlushTime   time.Duration
	LastFlushTime      time.Time
}

// NewWALManager creates a new WAL manager
func NewWALManager(config *WALConfig, storage WALStorage) *WALManager {
	if config == nil {
		config = DefaultWALConfig()
	}

	wm := &WALManager{
		config:       config,
		storage:      storage,
		buffer:       make(chan WALEntry, config.MaxBufferSize*2), // Extra capacity for burst
		segments:     make(map[string]*WALSegment),
		flushTrigger: make(chan struct{}, 1),
		shutdown:     make(chan struct{}),
		done:         make(chan struct{}),
		flushWorkers: make([]chan WALSegment, config.WorkerCount),
		metrics:      &WALMetrics{},
	}

	// Initialize flush workers
	for i := 0; i < config.WorkerCount; i++ {
		wm.flushWorkers[i] = make(chan WALSegment, 10)
		wm.wg.Add(1)
		go wm.flushWorker(i, wm.flushWorkers[i])
	}

	// Start main processing loop
	wm.wg.Add(1)
	go wm.processLoop()

	return wm
}

// WriteEntry writes an entry to the WAL
func (wm *WALManager) WriteEntry(ctx context.Context, entryType WALEntryType, data json.RawMessage, metadata map[string]interface{}) error {
	entry := WALEntry{
		ID:         generateID(),
		Type:       entryType,
		Timestamp:  time.Now(),
		SequenceID: atomic.AddUint64(&wm.currentSeqID, 1),
		Data:       data,
		Metadata:   metadata,
		Checksum:   calculateChecksum(data),
	}

	select {
	case wm.buffer <- entry:
		atomic.AddInt64(&wm.metrics.EntriesBuffered, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-wm.shutdown:
		return fmt.Errorf("WAL manager is shutting down")
	default:
		// Buffer is full, trigger immediate flush
		select {
		case wm.flushTrigger <- struct{}{}:
		default:
		}
		return fmt.Errorf("WAL buffer is full")
	}
}

// Flush forces an immediate flush of the WAL buffer
func (wm *WALManager) Flush(ctx context.Context) error {
	select {
	case wm.flushTrigger <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-wm.shutdown:
		return fmt.Errorf("WAL manager is shutting down")
	}
}

// Shutdown gracefully shuts down the WAL manager
func (wm *WALManager) Shutdown(ctx context.Context) error {
	close(wm.shutdown)

	// Wait for processing to complete or context timeout
	select {
	case <-wm.done:
		wm.wg.Wait()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// GetMetrics returns current WAL metrics
func (wm *WALManager) GetMetrics() WALMetrics {
	return *wm.metrics
}

// SetFlushCallback sets the callback to be called after each flush
func (wm *WALManager) SetFlushCallback(callback func(segment *WALSegment) error) {
	wm.onFlush = callback
}

// SetRecoveryCallback sets the callback to be called during recovery
func (wm *WALManager) SetRecoveryCallback(callback func(entries []WALEntry) error) {
	wm.onRecovery = callback
}

// processLoop is the main processing loop for the WAL manager
func (wm *WALManager) processLoop() {
	defer close(wm.done)

	ticker := time.NewTicker(wm.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wm.shutdown:
			wm.flushAllBuffers(context.Background())
			return

		case <-ticker.C:
			wm.triggerFlush()

		case <-wm.flushTrigger:
			wm.triggerFlush()

		case entry := <-wm.buffer:
			wm.addToSegment(entry)
		}
	}
}

// triggerFlush triggers a flush operation
func (wm *WALManager) triggerFlush() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.activeSegment != nil && len(wm.activeSegment.Entries) > 0 {
		segment := wm.activeSegment
		wm.activeSegment = wm.createNewSegment()

		// Send to a worker for processing
		workerIndex := int(segment.StartSeqID) % len(wm.flushWorkers)
		select {
		case wm.flushWorkers[workerIndex] <- *segment:
		default:
			// Worker queue is full, process synchronously
			go wm.flushSegment(context.Background(), segment)
		}
	}
}

// flushWorker processes segments for a specific worker
func (wm *WALManager) flushWorker(workerID int, segmentCh <-chan WALSegment) {
	defer wm.wg.Done()

	for segment := range segmentCh {
		if err := wm.flushSegment(context.Background(), &segment); err != nil {
			// Log error and retry logic could be added here
			atomic.AddInt64(&wm.metrics.FlushErrors, 1)
		}
	}
}

// flushSegment flushes a segment to storage
func (wm *WALManager) flushSegment(ctx context.Context, segment *WALSegment) error {
	start := time.Now()

	// Update segment status
	segment.Status = SegmentStatusFlushing
	now := time.Now()
	segment.FlushedAt = &now

	// Save segment to storage
	if err := wm.storage.SaveWALSegment(ctx, segment); err != nil {
		segment.Status = SegmentStatusFailed
		return fmt.Errorf("failed to save WAL segment: %w", err)
	}

	// Apply entries to underlying storage based on type
	if err := wm.applyEntries(ctx, segment.Entries); err != nil {
		segment.Status = SegmentStatusFailed
		return fmt.Errorf("failed to apply WAL entries: %w", err)
	}

	// Mark segment as flushed
	segment.Status = SegmentStatusFlushed

	// Update metrics
	atomic.AddInt64(&wm.metrics.EntriesFlushed, int64(len(segment.Entries)))
	atomic.AddInt64(&wm.metrics.FlushOperations, 1)
	atomic.StoreInt64((*int64)(&wm.metrics.AverageFlushTime), int64(time.Since(start)))
	wm.metrics.LastFlushTime = time.Now()

	// Call flush callback if set
	if wm.onFlush != nil {
		if err := wm.onFlush(segment); err != nil {
			// Log error but don't fail the flush
		}
	}

	return nil
}

// applyEntries applies WAL entries to the underlying storage
func (wm *WALManager) applyEntries(ctx context.Context, entries []WALEntry) error {
	// Group entries by type for batch processing
	taskUpdates := make([]storage.PersistentTask, 0)
	activityLogs := make([]storage.TaskActivityLog, 0)

	for _, entry := range entries {
		switch entry.Type {
		case WALEntryTypeTaskUpdate:
			var task storage.PersistentTask
			if err := json.Unmarshal(entry.Data, &task); err != nil {
				continue // Skip invalid entries
			}
			taskUpdates = append(taskUpdates, task)

		case WALEntryTypeActivityLog:
			var log storage.TaskActivityLog
			if err := json.Unmarshal(entry.Data, &log); err != nil {
				continue // Skip invalid entries
			}
			activityLogs = append(activityLogs, log)
		}
	}

	// Batch save tasks
	if len(taskUpdates) > 0 {
		for i := 0; i < len(taskUpdates); i += wm.config.BatchSize {
			end := i + wm.config.BatchSize
			if end > len(taskUpdates) {
				end = len(taskUpdates)
			}
			batch := taskUpdates[i:end]

			for _, task := range batch {
				if err := wm.storage.SaveTask(ctx, &task); err != nil {
					return fmt.Errorf("failed to save task %s: %w", task.ID, err)
				}
			}
		}
	}

	// Batch save activity logs
	if len(activityLogs) > 0 {
		for i := 0; i < len(activityLogs); i += wm.config.BatchSize {
			end := i + wm.config.BatchSize
			if end > len(activityLogs) {
				end = len(activityLogs)
			}
			batch := activityLogs[i:end]

			for _, log := range batch {
				if err := wm.storage.LogActivity(ctx, &log); err != nil {
					return fmt.Errorf("failed to save activity log for task %s: %w", log.TaskID, err)
				}
			}
		}
	}

	return nil
}

// addToSegment adds an entry to the current active segment
func (wm *WALManager) addToSegment(entry WALEntry) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Create new segment if needed
	if wm.activeSegment == nil || len(wm.activeSegment.Entries) >= wm.config.MaxSegmentSize {
		wm.activeSegment = wm.createNewSegment()
	}

	wm.activeSegment.Entries = append(wm.activeSegment.Entries, entry)
	wm.activeSegment.EndSeqID = entry.SequenceID
}

// createNewSegment creates a new WAL segment
func (wm *WALManager) createNewSegment() *WALSegment {
	segmentID := generateID()
	startSeqID := atomic.LoadUint64(&wm.currentSeqID) + 1

	segment := &WALSegment{
		ID:         segmentID,
		StartSeqID: startSeqID,
		EndSeqID:   startSeqID,
		Entries:    make([]WALEntry, 0, wm.config.MaxSegmentSize),
		CreatedAt:  time.Now(),
		Status:     SegmentStatusActive,
	}

	wm.segments[segmentID] = segment
	return segment
}

// flushAllBuffers flushes all remaining buffers during shutdown
func (wm *WALManager) flushAllBuffers(ctx context.Context) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.activeSegment != nil && len(wm.activeSegment.Entries) > 0 {
		wm.flushSegment(ctx, wm.activeSegment)
	}
}

// Helper functions
func generateID() string {
	return fmt.Sprintf("wal_%d", time.Now().UnixNano())
}

func calculateChecksum(data []byte) string {
	// Simple checksum implementation - in production, use a proper hash
	sum := 0
	for _, b := range data {
		sum += int(b)
	}
	return fmt.Sprintf("%x", sum)
}
