package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/mq/logger"
)

// BrokerEnhancedConfig holds configuration for all enhanced features
type BrokerEnhancedConfig struct {
	// DLQ Configuration
	DLQStoragePath     string
	DLQRetentionPeriod time.Duration
	DLQMaxSize         int

	// WAL Configuration
	WALDirectory    string
	WALMaxFileSize  int64
	WALSyncInterval time.Duration
	WALFsyncOnWrite bool

	// Acknowledgment Configuration
	AckTimeout       time.Duration
	AckMaxRetries    int
	AckCheckInterval time.Duration

	// Worker Health Configuration
	WorkerHealthTimeout time.Duration
	WorkerCheckInterval time.Duration

	// Dynamic Scaling Configuration
	MinWorkers         int
	MaxWorkers         int
	ScaleUpThreshold   float64
	ScaleDownThreshold float64

	// Deduplication Configuration
	DedupWindow          time.Duration
	DedupCleanupInterval time.Duration
	DedupPersistent      bool

	// Flow Control Configuration
	FlowControlStrategy   FlowControlStrategyType
	FlowControlConfigPath string // Path to flow control config file
	FlowControlEnvPrefix  string // Environment variable prefix for flow control
	MaxCredits            int64
	MinCredits            int64
	CreditRefillRate      int64
	CreditRefillInterval  time.Duration
	// Token bucket specific
	TokenBucketCapacity       int64
	TokenBucketRefillRate     int64
	TokenBucketRefillInterval time.Duration
	// Leaky bucket specific
	LeakyBucketCapacity     int64
	LeakyBucketLeakInterval time.Duration
	// Credit-based specific
	CreditBasedMaxCredits     int64
	CreditBasedRefillRate     int64
	CreditBasedRefillInterval time.Duration
	CreditBasedBurstSize      int64
	// Rate limiter specific
	RateLimiterRequestsPerSecond int64
	RateLimiterBurstSize         int64

	// Backpressure Configuration
	QueueDepthThreshold int
	MemoryThreshold     uint64
	ErrorRateThreshold  float64

	// Snapshot Configuration
	SnapshotDirectory string
	SnapshotInterval  time.Duration
	SnapshotRetention time.Duration

	// Tracing Configuration
	TracingEnabled      bool
	TraceRetention      time.Duration
	TraceExportInterval time.Duration

	Logger             logger.Logger
	EnableEnhancements bool // Master switch for all enhancements
}

// EnhancedFeatures holds all enhanced feature managers
type EnhancedFeatures struct {
	ackManager          *AckManager
	walLog              *WriteAheadLog
	dlqStorage          DLQStorage
	dedupManager        *DeduplicationManager
	flowController      *FlowController
	backpressureMonitor *BackpressureMonitor
	snapshotManager     *SnapshotManager
	traceManager        *TraceManager
	lifecycleTracker    *MessageLifecycleTracker
	config              *BrokerEnhancedConfig
	enabled             bool
}

// InitializeEnhancements initializes all enhanced features for a broker
func (b *Broker) InitializeEnhancements(config *BrokerEnhancedConfig) error {
	if config == nil {
		config = DefaultBrokerEnhancedConfig()
	}

	if !config.EnableEnhancements {
		return nil // Enhancements disabled
	}

	features := &EnhancedFeatures{
		config:  config,
		enabled: true,
	}

	// Initialize DLQ Storage
	if config.DLQStoragePath != "" {
		dlqStorage, err := NewFileDLQStorage(config.DLQStoragePath, config.Logger)
		if err != nil {
			return err
		}
		features.dlqStorage = dlqStorage
	} else {
		features.dlqStorage = NewInMemoryDLQStorage()
	}

	// Initialize WAL
	if config.WALDirectory != "" {
		walConfig := WALConfig{
			Directory:    config.WALDirectory,
			MaxFileSize:  config.WALMaxFileSize,
			SyncInterval: config.WALSyncInterval,
			FsyncOnWrite: config.WALFsyncOnWrite,
			Logger:       config.Logger,
		}
		wal, err := NewWriteAheadLog(walConfig)
		if err != nil {
			return err
		}
		features.walLog = wal
	}

	// Initialize Acknowledgment Manager
	ackConfig := AckManagerConfig{
		AckTimeout:    config.AckTimeout,
		MaxRetries:    config.AckMaxRetries,
		CheckInterval: config.AckCheckInterval,
		Logger:        config.Logger,
	}
	features.ackManager = NewAckManager(ackConfig)

	// Setup acknowledgment callbacks
	features.ackManager.SetOnReject(func(pm *PendingMessage) {
		// Move to DLQ
		entry := &DLQEntry{
			TaskID:          pm.Task.ID,
			QueueName:       pm.QueueName,
			OriginalPayload: json.RawMessage(pm.Task.Payload),
			ErrorMessage:    "Max retries exceeded",
			FailedAt:        time.Now(),
			RetryCount:      pm.RetryCount,
		}
		_ = features.dlqStorage.Store(context.Background(), entry)
	})

	features.ackManager.SetOnRedeliver(func(pm *PendingMessage) {
		// Requeue the message
		if queue, exists := b.queues.Get(pm.QueueName); exists {
			select {
			case queue.tasks <- &QueuedTask{Task: pm.Task}:
			default:
				config.Logger.Warn("Failed to requeue message - queue full",
					logger.Field{Key: "taskID", Value: pm.Task.ID})
			}
		}
	})

	// Initialize Deduplication Manager
	dedupConfig := DedupConfig{
		Window:          config.DedupWindow,
		CleanupInterval: config.DedupCleanupInterval,
		Logger:          config.Logger,
	}
	features.dedupManager = NewDeduplicationManager(dedupConfig)

	// Initialize Flow Controller using factory
	factory := NewFlowControllerFactory()

	// Try to load configuration from providers
	var flowConfig FlowControlConfig
	var err error

	// First try file-based configuration
	if config.FlowControlConfigPath != "" {
		fileProvider := NewFileConfigProvider(config.FlowControlConfigPath)
		if loadedConfig, loadErr := fileProvider.GetConfig(); loadErr == nil {
			flowConfig = loadedConfig
		}
	}

	// If no file config, try environment variables
	if flowConfig.Strategy == "" && config.FlowControlEnvPrefix != "" {
		envProvider := NewEnvConfigProvider(config.FlowControlEnvPrefix)
		if loadedConfig, loadErr := envProvider.GetConfig(); loadErr == nil {
			flowConfig = loadedConfig
		}
	}

	// If still no config, use broker config defaults based on strategy
	if flowConfig.Strategy == "" {
		flowConfig = FlowControlConfig{
			Strategy: config.FlowControlStrategy,
			Logger:   config.Logger,
		}

		// Set strategy-specific defaults
		switch config.FlowControlStrategy {
		case StrategyTokenBucket:
			flowConfig.MaxCredits = config.TokenBucketCapacity
			flowConfig.RefillRate = config.TokenBucketRefillRate
			flowConfig.RefillInterval = config.TokenBucketRefillInterval
		case StrategyLeakyBucket:
			flowConfig.MaxCredits = config.LeakyBucketCapacity
			flowConfig.RefillInterval = config.LeakyBucketLeakInterval
		case StrategyCreditBased:
			flowConfig.MaxCredits = config.CreditBasedMaxCredits
			flowConfig.RefillRate = config.CreditBasedRefillRate
			flowConfig.RefillInterval = config.CreditBasedRefillInterval
			flowConfig.BurstSize = config.CreditBasedBurstSize
		case StrategyRateLimiter:
			flowConfig.RefillRate = config.RateLimiterRequestsPerSecond
			flowConfig.BurstSize = config.RateLimiterBurstSize
		default:
			// Fallback to token bucket
			flowConfig.Strategy = StrategyTokenBucket
			flowConfig.MaxCredits = config.MaxCredits
			flowConfig.RefillRate = config.CreditRefillRate
			flowConfig.RefillInterval = config.CreditRefillInterval
		}
	}

	// Ensure logger is set
	flowConfig.Logger = config.Logger

	// Create flow controller using factory
	features.flowController, err = factory.CreateFlowController(flowConfig)
	if err != nil {
		return fmt.Errorf("failed to create flow controller: %w", err)
	}

	// Initialize Backpressure Monitor
	backpressureConfig := BackpressureConfig{
		QueueDepthThreshold: config.QueueDepthThreshold,
		MemoryThreshold:     config.MemoryThreshold,
		ErrorRateThreshold:  config.ErrorRateThreshold,
		Logger:              config.Logger,
	}
	features.backpressureMonitor = NewBackpressureMonitor(backpressureConfig)

	// Initialize Snapshot Manager
	if config.SnapshotDirectory != "" {
		snapshotConfig := SnapshotConfig{
			BaseDir:          config.SnapshotDirectory,
			SnapshotInterval: config.SnapshotInterval,
			RetentionPeriod:  config.SnapshotRetention,
			Logger:           config.Logger,
		}
		snapshotManager, err := NewSnapshotManager(b, snapshotConfig)
		if err != nil {
			return err
		}
		features.snapshotManager = snapshotManager
	}

	// Initialize Tracing
	if config.TracingEnabled {
		traceConfig := TraceConfig{
			Storage:        NewInMemoryTraceStorage(),
			Retention:      config.TraceRetention,
			ExportInterval: config.TraceExportInterval,
			Logger:         config.Logger,
		}
		features.traceManager = NewTraceManager(traceConfig)
		features.lifecycleTracker = NewMessageLifecycleTracker(features.traceManager, config.Logger)
	}

	// Store features in broker (we'll need to add this field to Broker struct)
	b.enhanced = features

	return nil
}

// DefaultBrokerEnhancedConfig returns default configuration
func DefaultBrokerEnhancedConfig() *BrokerEnhancedConfig {
	return &BrokerEnhancedConfig{
		DLQRetentionPeriod:   7 * 24 * time.Hour,
		DLQMaxSize:           10000,
		WALMaxFileSize:       100 * 1024 * 1024,
		WALSyncInterval:      1 * time.Second,
		WALFsyncOnWrite:      false,
		AckTimeout:           30 * time.Second,
		AckMaxRetries:        3,
		AckCheckInterval:     5 * time.Second,
		WorkerHealthTimeout:  30 * time.Second,
		WorkerCheckInterval:  10 * time.Second,
		MinWorkers:           1,
		MaxWorkers:           100,
		ScaleUpThreshold:     0.75,
		ScaleDownThreshold:   0.25,
		DedupWindow:          5 * time.Minute,
		DedupCleanupInterval: 1 * time.Minute,
		// Flow Control defaults (Token Bucket strategy)
		FlowControlStrategy:          StrategyTokenBucket,
		FlowControlConfigPath:        "",
		FlowControlEnvPrefix:         "FLOW_",
		MaxCredits:                   1000,
		MinCredits:                   100,
		CreditRefillRate:             10,
		CreditRefillInterval:         100 * time.Millisecond,
		TokenBucketCapacity:          1000,
		TokenBucketRefillRate:        100,
		TokenBucketRefillInterval:    100 * time.Millisecond,
		LeakyBucketCapacity:          500,
		LeakyBucketLeakInterval:      200 * time.Millisecond,
		CreditBasedMaxCredits:        1000,
		CreditBasedRefillRate:        100,
		CreditBasedRefillInterval:    200 * time.Millisecond,
		CreditBasedBurstSize:         50,
		RateLimiterRequestsPerSecond: 100,
		RateLimiterBurstSize:         200,
		QueueDepthThreshold:          1000,
		MemoryThreshold:              1 * 1024 * 1024 * 1024, // 1GB
		ErrorRateThreshold:           0.5,
		SnapshotInterval:             5 * time.Minute,
		SnapshotRetention:            24 * time.Hour,
		TracingEnabled:               true,
		TraceRetention:               24 * time.Hour,
		TraceExportInterval:          30 * time.Second,
		EnableEnhancements:           true,
	}
}

// EnhancedPublish publishes a message with enhanced features
func (b *Broker) EnhancedPublish(ctx context.Context, task *Task, queueName string) error {
	if b.enhanced == nil || !b.enhanced.enabled {
		// Fall back to regular publish logic
		return b.regularPublish(ctx, task, queueName)
	}

	// Check for duplicates
	if b.enhanced.dedupManager != nil {
		isDuplicate, err := b.enhanced.dedupManager.CheckDuplicate(ctx, task)
		if err != nil {
			return err
		}
		if isDuplicate {
			b.logger.Debug("Duplicate message rejected",
				logger.Field{Key: "taskID", Value: task.ID})
			return nil
		}
	}

	// Acquire flow control credits
	if b.enhanced.flowController != nil {
		if err := b.enhanced.flowController.AcquireCredit(ctx, 1); err != nil {
			return err
		}
		defer b.enhanced.flowController.ReleaseCredit(1)
	}

	// Write to WAL
	if b.enhanced.walLog != nil {
		walEntry := &WALEntry{
			EntryType: WALEntryEnqueue,
			TaskID:    task.ID,
			QueueName: queueName,
			Payload:   json.RawMessage(task.Payload),
		}
		if err := b.enhanced.walLog.WriteEntry(ctx, walEntry); err != nil {
			b.logger.Error("Failed to write WAL entry",
				logger.Field{Key: "error", Value: err})
		}
	}

	// Start tracing
	if b.enhanced.lifecycleTracker != nil {
		b.enhanced.lifecycleTracker.TrackEnqueue(ctx, task, queueName)
	}

	// Continue with regular publish
	return b.regularPublish(ctx, task, queueName)
}

// regularPublish is the standard publish logic
func (b *Broker) regularPublish(ctx context.Context, task *Task, queueName string) error {
	queue, exists := b.queues.Get(queueName)
	if !exists {
		queue = b.NewQueue(queueName)
	}

	// Enqueue task
	select {
	case queue.tasks <- &QueuedTask{Task: task}:
		// Track for acknowledgment if enhanced features enabled
		if b.enhanced != nil && b.enhanced.ackManager != nil {
			_ = b.enhanced.ackManager.TrackMessage(ctx, task, queueName, "")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RecoverFromWAL recovers broker state from WAL
func (b *Broker) RecoverFromWAL(ctx context.Context) error {
	if b.enhanced == nil || b.enhanced.walLog == nil {
		return nil
	}

	b.logger.Info("Starting WAL recovery")

	return b.enhanced.walLog.Replay(func(entry *WALEntry) error {
		switch entry.EntryType {
		case WALEntryEnqueue:
			var task Task
			if err := json.Unmarshal(entry.Payload, &task); err != nil {
				return err
			}

			queue, exists := b.queues.Get(entry.QueueName)
			if !exists {
				queue = b.NewQueue(entry.QueueName)
			}

			select {
			case queue.tasks <- &QueuedTask{Task: &task}:
			default:
				b.logger.Warn("Queue full during recovery",
					logger.Field{Key: "queue", Value: entry.QueueName})
			}

		case WALEntryComplete, WALEntryFailed:
			// Already processed, no action needed
		}

		return nil
	})
}

// RecoverFromSnapshot recovers broker state from snapshots
func (b *Broker) RecoverFromSnapshot(ctx context.Context) error {
	if b.enhanced == nil || b.enhanced.snapshotManager == nil {
		return nil
	}

	b.logger.Info("Starting snapshot recovery")

	// Recover all queues
	var recoveredQueues int
	b.queues.ForEach(func(queueName string, _ *Queue) bool {
		if err := b.enhanced.snapshotManager.RestoreFromSnapshot(ctx, queueName); err != nil {
			b.logger.Error("Failed to restore queue from snapshot",
				logger.Field{Key: "queue", Value: queueName},
				logger.Field{Key: "error", Value: err})
		} else {
			recoveredQueues++
		}
		return true
	})

	b.logger.Info("Snapshot recovery complete",
		logger.Field{Key: "queues", Value: recoveredQueues})

	return nil
}

// GetEnhancedStats returns comprehensive statistics
func (b *Broker) GetEnhancedStats() map[string]interface{} {
	stats := make(map[string]interface{})

	if b.enhanced == nil {
		return stats
	}

	if b.enhanced.ackManager != nil {
		stats["acknowledgments"] = b.enhanced.ackManager.GetStats()
	}

	if b.enhanced.walLog != nil {
		stats["wal"] = b.enhanced.walLog.GetStats()
	}

	if b.enhanced.dedupManager != nil {
		stats["deduplication"] = b.enhanced.dedupManager.GetStats()
	}

	if b.enhanced.flowController != nil {
		stats["flow_control"] = b.enhanced.flowController.GetStats()
	}

	if b.enhanced.snapshotManager != nil {
		stats["snapshots"] = b.enhanced.snapshotManager.GetSnapshotStats()
	}

	if b.enhanced.traceManager != nil {
		stats["tracing"] = b.enhanced.traceManager.GetStats()
	}

	return stats
}

// ShutdownEnhanced gracefully shuts down all enhanced features
func (b *Broker) ShutdownEnhanced(ctx context.Context) error {
	if b.enhanced == nil {
		return nil
	}

	b.logger.Info("Starting enhanced features shutdown")

	// Shutdown components in order
	if b.enhanced.backpressureMonitor != nil {
		b.enhanced.backpressureMonitor.Shutdown()
	}

	if b.enhanced.flowController != nil {
		b.enhanced.flowController.Shutdown()
	}

	if b.enhanced.dedupManager != nil {
		_ = b.enhanced.dedupManager.Shutdown(ctx)
	}

	if b.enhanced.ackManager != nil {
		_ = b.enhanced.ackManager.Shutdown(ctx)
	}

	if b.enhanced.snapshotManager != nil {
		_ = b.enhanced.snapshotManager.Shutdown(ctx)
	}

	if b.enhanced.walLog != nil {
		_ = b.enhanced.walLog.Shutdown(ctx)
	}

	if b.enhanced.traceManager != nil {
		_ = b.enhanced.traceManager.Shutdown(ctx)
	}

	if b.enhanced.dlqStorage != nil {
		_ = b.enhanced.dlqStorage.Close()
	}

	b.logger.Info("Enhanced features shutdown complete")

	return nil
}

// InitializeEnhancedPool initializes pool with enhanced features
func (p *Pool) InitializeEnhancedPool(config *BrokerEnhancedConfig) {
	if config == nil || !config.EnableEnhancements {
		return
	}

	// Add health monitor
	if config.WorkerHealthTimeout > 0 {
		_ = NewWorkerHealthMonitor(p, config.WorkerHealthTimeout, config.WorkerCheckInterval, config.Logger)
	}

	// Add dynamic scaler
	if config.MinWorkers > 0 && config.MaxWorkers > int(p.numOfWorkers) {
		_ = NewDynamicScaler(p, config.MinWorkers, config.MaxWorkers, config.Logger)
	}
}
