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

// QueueSnapshot represents a point-in-time snapshot of queue state
type QueueSnapshot struct {
	Timestamp     time.Time                `json:"timestamp"`
	QueueName     string                   `json:"queue_name"`
	PendingTasks  []*Task                  `json:"pending_tasks"`
	ConsumerState map[string]*ConsumerMeta `json:"consumer_state"`
	Metrics       *QueueMetrics            `json:"metrics"`
	Config        *QueueConfig             `json:"config"`
	Version       int                      `json:"version"`
}

// ConsumerMeta represents consumer metadata in snapshot
type ConsumerMeta struct {
	ID            string    `json:"id"`
	State         string    `json:"state"`
	LastActivity  time.Time `json:"last_activity"`
	TasksAssigned int       `json:"tasks_assigned"`
}

// SnapshotManager manages queue snapshots and recovery
type SnapshotManager struct {
	baseDir          string
	broker           *Broker
	snapshotInterval time.Duration
	retentionPeriod  time.Duration
	mu               sync.RWMutex
	logger           logger.Logger
	shutdown         chan struct{}
	wg               sync.WaitGroup
}

// SnapshotConfig holds snapshot configuration
type SnapshotConfig struct {
	BaseDir          string
	SnapshotInterval time.Duration
	RetentionPeriod  time.Duration // How long to keep old snapshots
	Logger           logger.Logger
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager(broker *Broker, config SnapshotConfig) (*SnapshotManager, error) {
	if config.SnapshotInterval == 0 {
		config.SnapshotInterval = 5 * time.Minute
	}
	if config.RetentionPeriod == 0 {
		config.RetentionPeriod = 24 * time.Hour
	}

	if err := os.MkdirAll(config.BaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	sm := &SnapshotManager{
		baseDir:          config.BaseDir,
		broker:           broker,
		snapshotInterval: config.SnapshotInterval,
		retentionPeriod:  config.RetentionPeriod,
		logger:           config.Logger,
		shutdown:         make(chan struct{}),
	}

	// Start periodic snapshot worker
	sm.wg.Add(1)
	go sm.snapshotLoop()

	return sm, nil
}

// snapshotLoop periodically creates snapshots
func (sm *SnapshotManager) snapshotLoop() {
	defer sm.wg.Done()

	ticker := time.NewTicker(sm.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sm.CreateSnapshotAll(context.Background()); err != nil {
				sm.logger.Error("Failed to create periodic snapshot",
					logger.Field{Key: "error", Value: err})
			}

			// Cleanup old snapshots
			if err := sm.CleanupOldSnapshots(); err != nil {
				sm.logger.Error("Failed to cleanup old snapshots",
					logger.Field{Key: "error", Value: err})
			}
		case <-sm.shutdown:
			return
		}
	}
}

// CreateSnapshot creates a snapshot of a specific queue
func (sm *SnapshotManager) CreateSnapshot(ctx context.Context, queueName string) (*QueueSnapshot, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	queue, exists := sm.broker.queues.Get(queueName)
	if !exists {
		return nil, fmt.Errorf("queue not found: %s", queueName)
	}

	// Collect pending tasks
	var pendingTasks []*Task
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Drain channel into slice (non-blocking)
		for {
			select {
			case qt := <-queue.tasks:
				pendingTasks = append(pendingTasks, qt.Task)
				// Put it back
				queue.tasks <- qt
			default:
				goto done
			}
		}
	}

done:
	// Collect consumer state
	consumerState := make(map[string]*ConsumerMeta)
	queue.consumers.ForEach(func(id string, consumer *consumer) bool {
		consumerState[id] = &ConsumerMeta{
			ID:           id,
			State:        string(consumer.state),
			LastActivity: consumer.metrics.LastActivity,
		}
		return true
	})

	snapshot := &QueueSnapshot{
		Timestamp:     time.Now(),
		QueueName:     queueName,
		PendingTasks:  pendingTasks,
		ConsumerState: consumerState,
		Metrics:       queue.metrics,
		Config:        queue.config,
		Version:       1,
	}

	// Persist snapshot to disk
	if err := sm.persistSnapshot(snapshot); err != nil {
		return nil, fmt.Errorf("failed to persist snapshot: %w", err)
	}

	sm.logger.Info("Created queue snapshot",
		logger.Field{Key: "queue", Value: queueName},
		logger.Field{Key: "pendingTasks", Value: len(pendingTasks)},
		logger.Field{Key: "consumers", Value: len(consumerState)})

	return snapshot, nil
}

// CreateSnapshotAll creates snapshots of all queues
func (sm *SnapshotManager) CreateSnapshotAll(ctx context.Context) error {
	var snapshots []*QueueSnapshot
	var mu sync.Mutex
	var wg sync.WaitGroup

	sm.broker.queues.ForEach(func(queueName string, _ *Queue) bool {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			snapshot, err := sm.CreateSnapshot(ctx, name)
			if err != nil {
				sm.logger.Error("Failed to create snapshot",
					logger.Field{Key: "queue", Value: name},
					logger.Field{Key: "error", Value: err})
				return
			}
			mu.Lock()
			snapshots = append(snapshots, snapshot)
			mu.Unlock()
		}(queueName)
		return true
	})

	wg.Wait()

	sm.logger.Info("Created snapshots for all queues",
		logger.Field{Key: "count", Value: len(snapshots)})

	return nil
}

// persistSnapshot writes a snapshot to disk
func (sm *SnapshotManager) persistSnapshot(snapshot *QueueSnapshot) error {
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	queueDir := filepath.Join(sm.baseDir, snapshot.QueueName)
	if err := os.MkdirAll(queueDir, 0755); err != nil {
		return fmt.Errorf("failed to create queue directory: %w", err)
	}

	filename := fmt.Sprintf("snapshot-%d.json", snapshot.Timestamp.UnixNano())
	filePath := filepath.Join(queueDir, filename)

	// Write atomically using temp file
	tempPath := filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	if err := os.Rename(tempPath, filePath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}

	return nil
}

// RestoreFromSnapshot restores a queue from the latest snapshot
func (sm *SnapshotManager) RestoreFromSnapshot(ctx context.Context, queueName string) error {
	snapshot, err := sm.GetLatestSnapshot(queueName)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	if snapshot == nil {
		return fmt.Errorf("no snapshot found for queue: %s", queueName)
	}

	sm.logger.Info("Restoring queue from snapshot",
		logger.Field{Key: "queue", Value: queueName},
		logger.Field{Key: "timestamp", Value: snapshot.Timestamp},
		logger.Field{Key: "tasks", Value: len(snapshot.PendingTasks)})

	// Get or create queue
	queue, exists := sm.broker.queues.Get(queueName)
	if !exists {
		queue = sm.broker.NewQueue(queueName)
	}

	// Restore pending tasks
	restored := 0
	for _, task := range snapshot.PendingTasks {
		select {
		case queue.tasks <- &QueuedTask{Task: task}:
			restored++
		case <-ctx.Done():
			return ctx.Err()
		default:
			sm.logger.Warn("Queue full during restore, task skipped",
				logger.Field{Key: "taskID", Value: task.ID})
		}
	}

	// Restore metrics
	if snapshot.Metrics != nil {
		queue.metrics = snapshot.Metrics
	}

	// Restore config
	if snapshot.Config != nil {
		queue.config = snapshot.Config
	}

	sm.logger.Info("Queue restored from snapshot",
		logger.Field{Key: "queue", Value: queueName},
		logger.Field{Key: "restoredTasks", Value: restored})

	return nil
}

// GetLatestSnapshot retrieves the latest snapshot for a queue
func (sm *SnapshotManager) GetLatestSnapshot(queueName string) (*QueueSnapshot, error) {
	queueDir := filepath.Join(sm.baseDir, queueName)

	files, err := filepath.Glob(filepath.Join(queueDir, "snapshot-*.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	if len(files) == 0 {
		return nil, nil
	}

	// Sort files to get latest (files are named with timestamp)
	latestFile := files[0]
	for _, file := range files {
		if file > latestFile {
			latestFile = file
		}
	}

	// Read and parse snapshot
	data, err := os.ReadFile(latestFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot QueueSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	return &snapshot, nil
}

// ListSnapshots lists all snapshots for a queue
func (sm *SnapshotManager) ListSnapshots(queueName string) ([]*QueueSnapshot, error) {
	queueDir := filepath.Join(sm.baseDir, queueName)

	files, err := filepath.Glob(filepath.Join(queueDir, "snapshot-*.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	snapshots := make([]*QueueSnapshot, 0, len(files))
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			sm.logger.Error("Failed to read snapshot file",
				logger.Field{Key: "file", Value: file},
				logger.Field{Key: "error", Value: err})
			continue
		}

		var snapshot QueueSnapshot
		if err := json.Unmarshal(data, &snapshot); err != nil {
			sm.logger.Error("Failed to unmarshal snapshot",
				logger.Field{Key: "file", Value: file},
				logger.Field{Key: "error", Value: err})
			continue
		}

		snapshots = append(snapshots, &snapshot)
	}

	return snapshots, nil
}

// CleanupOldSnapshots removes snapshots older than retention period
func (sm *SnapshotManager) CleanupOldSnapshots() error {
	cutoff := time.Now().Add(-sm.retentionPeriod)
	removed := 0

	err := filepath.Walk(sm.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		if info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err != nil {
				sm.logger.Error("Failed to remove old snapshot",
					logger.Field{Key: "file", Value: path},
					logger.Field{Key: "error", Value: err})
				return nil
			}
			removed++
		}

		return nil
	})

	if err != nil {
		return err
	}

	if removed > 0 {
		sm.logger.Info("Cleaned up old snapshots",
			logger.Field{Key: "removed", Value: removed})
	}

	return nil
}

// DeleteSnapshot deletes a specific snapshot
func (sm *SnapshotManager) DeleteSnapshot(queueName string, timestamp time.Time) error {
	filename := fmt.Sprintf("snapshot-%d.json", timestamp.UnixNano())
	filePath := filepath.Join(sm.baseDir, queueName, filename)

	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}

	sm.logger.Info("Deleted snapshot",
		logger.Field{Key: "queue", Value: queueName},
		logger.Field{Key: "timestamp", Value: timestamp})

	return nil
}

// GetSnapshotStats returns statistics about snapshots
func (sm *SnapshotManager) GetSnapshotStats() map[string]any {
	totalSnapshots := 0
	var totalSize int64

	filepath.Walk(sm.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".json" {
			totalSnapshots++
			totalSize += info.Size()
		}

		return nil
	})

	return map[string]any{
		"total_snapshots":   totalSnapshots,
		"total_size_bytes":  totalSize,
		"retention_period":  sm.retentionPeriod,
		"snapshot_interval": sm.snapshotInterval,
	}
}

// Shutdown gracefully shuts down the snapshot manager
func (sm *SnapshotManager) Shutdown(ctx context.Context) error {
	close(sm.shutdown)

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(done)
	}()

	// Create final snapshot before shutdown
	if err := sm.CreateSnapshotAll(ctx); err != nil {
		sm.logger.Error("Failed to create final snapshot",
			logger.Field{Key: "error", Value: err})
	}

	select {
	case <-done:
		sm.logger.Info("Snapshot manager shutdown complete")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
