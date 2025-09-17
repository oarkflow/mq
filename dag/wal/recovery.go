package wal

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/mq/logger"
)

// WALRecovery handles crash recovery for WAL
type WALRecovery struct {
	storage WALStorage
	logger  logger.Logger
	config  *WALConfig
}

// WALRecoveryManager manages the recovery process
type WALRecoveryManager struct {
	recovery *WALRecovery
	config   *WALConfig
}

// NewWALRecovery creates a new WAL recovery instance
func NewWALRecovery(storage WALStorage, logger logger.Logger, config *WALConfig) *WALRecovery {
	return &WALRecovery{
		storage: storage,
		logger:  logger,
		config:  config,
	}
}

// NewWALRecoveryManager creates a new WAL recovery manager
func NewWALRecoveryManager(config *WALConfig, storage WALStorage, logger logger.Logger) *WALRecoveryManager {
	return &WALRecoveryManager{
		recovery: NewWALRecovery(storage, logger, config),
		config:   config,
	}
}

// Recover performs crash recovery by replaying unflushed WAL entries
func (r *WALRecovery) Recover(ctx context.Context) error {
	r.logger.Info("Starting WAL recovery process", logger.Field{Key: "component", Value: "wal_recovery"})

	// Get all unflushed entries
	entries, err := r.storage.GetUnflushedEntries(ctx)
	if err != nil {
		return fmt.Errorf("failed to get unflushed entries: %w", err)
	}

	if len(entries) == 0 {
		r.logger.Info("No unflushed entries found, recovery complete", logger.Field{Key: "component", Value: "wal_recovery"})
		return nil
	}

	r.logger.Info("Found unflushed entries to recover", logger.Field{Key: "count", Value: len(entries)}, logger.Field{Key: "component", Value: "wal_recovery"})

	// Validate entries before recovery
	validEntries := make([]WALEntry, 0, len(entries))
	for _, entry := range entries {
		if err := r.validateEntry(&entry); err != nil {
			r.logger.Warn("Skipping invalid entry", logger.Field{Key: "entry_id", Value: entry.ID}, logger.Field{Key: "error", Value: err.Error()}, logger.Field{Key: "component", Value: "wal_recovery"})
			continue
		}
		validEntries = append(validEntries, entry)
	}

	if len(validEntries) == 0 {
		r.logger.Info("No valid entries to recover", logger.Field{Key: "component", Value: "wal_recovery"})
		return nil
	}

	// Apply valid entries in order
	appliedCount := 0
	failedCount := 0

	for _, entry := range validEntries {
		if err := r.applyEntry(ctx, &entry); err != nil {
			r.logger.Error("Failed to apply entry", logger.Field{Key: "entry_id", Value: entry.ID}, logger.Field{Key: "error", Value: err.Error()}, logger.Field{Key: "component", Value: "wal_recovery"})
			failedCount++
			continue
		}
		appliedCount++
	}

	r.logger.Info("Recovery complete", logger.Field{Key: "applied", Value: appliedCount}, logger.Field{Key: "failed", Value: failedCount}, logger.Field{Key: "component", Value: "wal_recovery"})
	return nil
}

// validateEntry validates a WAL entry before recovery
func (r *WALRecovery) validateEntry(entry *WALEntry) error {
	if entry.ID == "" {
		return fmt.Errorf("entry ID is empty")
	}

	if entry.Type == "" {
		return fmt.Errorf("entry type is empty")
	}

	if len(entry.Data) == 0 {
		return fmt.Errorf("entry data is empty")
	}

	if entry.Timestamp.IsZero() {
		return fmt.Errorf("entry timestamp is zero")
	}

	// Check if entry is too old (configurable)
	if r.config != nil && r.config.RecoveryTimeout > 0 {
		if time.Since(entry.Timestamp) > r.config.RecoveryTimeout {
			return fmt.Errorf("entry is too old: %v", time.Since(entry.Timestamp))
		}
	}

	return nil
}

// applyEntry applies a single WAL entry to the underlying storage
func (r *WALRecovery) applyEntry(ctx context.Context, entry *WALEntry) error {
	switch entry.Type {
	case WALEntryTypeTaskUpdate:
		return r.applyTaskUpdate(ctx, entry)
	case WALEntryTypeActivityLog:
		return r.applyActivityLog(ctx, entry)
	case WALEntryTypeTaskDelete:
		return r.applyTaskDelete(ctx, entry)
	default:
		return fmt.Errorf("unknown entry type: %s", entry.Type)
	}
}

// applyTaskUpdate applies a task update entry
func (r *WALRecovery) applyTaskUpdate(ctx context.Context, entry *WALEntry) error {
	// Use the underlying storage to save the task
	return r.storage.SaveTask(ctx, nil) // This needs to be implemented properly
}

// applyActivityLog applies an activity log entry
func (r *WALRecovery) applyActivityLog(ctx context.Context, entry *WALEntry) error {
	// Use the underlying storage to log activity
	return r.storage.LogActivity(ctx, nil) // This needs to be implemented properly
}

// applyTaskDelete applies a task delete entry
func (r *WALRecovery) applyTaskDelete(ctx context.Context, entry *WALEntry) error {
	// Task deletion would need to be implemented in the storage interface
	return fmt.Errorf("task delete recovery not implemented")
}

// Cleanup removes old recovery data
func (r *WALRecovery) Cleanup(ctx context.Context, olderThan time.Time) error {
	return r.storage.DeleteOldSegments(ctx, olderThan)
}

// GetRecoveryStats returns recovery statistics
func (r *WALRecovery) GetRecoveryStats(ctx context.Context) (*RecoveryStats, error) {
	entries, err := r.storage.GetUnflushedEntries(ctx)
	if err != nil {
		return nil, err
	}

	return &RecoveryStats{
		TotalEntries:     len(entries),
		PendingEntries:   len(entries),
		LastRecoveryTime: time.Now(),
	}, nil
}

// RecoveryStats contains recovery statistics
type RecoveryStats struct {
	TotalEntries     int
	AppliedEntries   int
	FailedEntries    int
	PendingEntries   int
	LastRecoveryTime time.Time
}

// PerformRecovery performs the full recovery process
func (rm *WALRecoveryManager) PerformRecovery(ctx context.Context) error {
	startTime := time.Now()
	rm.recovery.logger.Info("Starting WAL recovery manager process", logger.Field{Key: "component", Value: "wal_recovery_manager"})

	// Perform recovery
	if err := rm.recovery.Recover(ctx); err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}

	// Cleanup old data if configured
	if rm.config.SegmentRetention > 0 {
		cleanupTime := time.Now().Add(-rm.config.SegmentRetention)
		if err := rm.recovery.Cleanup(ctx, cleanupTime); err != nil {
			rm.recovery.logger.Warn("Failed to cleanup old recovery data", logger.Field{Key: "error", Value: err.Error()}, logger.Field{Key: "component", Value: "wal_recovery_manager"})
		}
	}

	duration := time.Since(startTime)
	rm.recovery.logger.Info("WAL recovery manager completed", logger.Field{Key: "duration", Value: duration.String()}, logger.Field{Key: "component", Value: "wal_recovery_manager"})

	return nil
}

// GetRecoveryStatus returns the current recovery status
func (rm *WALRecoveryManager) GetRecoveryStatus(ctx context.Context) (*RecoveryStatus, error) {
	stats, err := rm.recovery.GetRecoveryStats(ctx)
	if err != nil {
		return nil, err
	}

	return &RecoveryStatus{
		Stats:            *stats,
		Config:           *rm.config,
		IsRecoveryNeeded: stats.PendingEntries > 0,
	}, nil
}

// RecoveryStatus represents the current recovery status
type RecoveryStatus struct {
	Stats            RecoveryStats
	Config           WALConfig
	IsRecoveryNeeded bool
}
