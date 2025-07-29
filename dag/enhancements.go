package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// BatchProcessor handles batch processing of tasks
type BatchProcessor struct {
	dag          *DAG
	batchSize    int
	batchTimeout time.Duration
	buffer       []*mq.Task
	bufferMu     sync.Mutex
	flushTimer   *time.Timer
	logger       logger.Logger
	processFunc  func([]*mq.Task) error
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(dag *DAG, batchSize int, batchTimeout time.Duration, logger logger.Logger) *BatchProcessor {
	return &BatchProcessor{
		dag:          dag,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		buffer:       make([]*mq.Task, 0, batchSize),
		logger:       logger,
		stopCh:       make(chan struct{}),
	}
}

// SetProcessFunc sets the function to process batches
func (bp *BatchProcessor) SetProcessFunc(fn func([]*mq.Task) error) {
	bp.processFunc = fn
}

// AddTask adds a task to the batch
func (bp *BatchProcessor) AddTask(task *mq.Task) error {
	bp.bufferMu.Lock()
	defer bp.bufferMu.Unlock()

	bp.buffer = append(bp.buffer, task)

	// Reset timer
	if bp.flushTimer != nil {
		bp.flushTimer.Stop()
	}
	bp.flushTimer = time.AfterFunc(bp.batchTimeout, bp.flushBatch)

	// Check if batch is full
	if len(bp.buffer) >= bp.batchSize {
		bp.flushTimer.Stop()
		go bp.flushBatch()
	}

	return nil
}

// flushBatch processes the current batch
func (bp *BatchProcessor) flushBatch() {
	bp.bufferMu.Lock()
	if len(bp.buffer) == 0 {
		bp.bufferMu.Unlock()
		return
	}

	batch := make([]*mq.Task, len(bp.buffer))
	copy(batch, bp.buffer)
	bp.buffer = bp.buffer[:0] // Reset buffer
	bp.bufferMu.Unlock()

	if bp.processFunc != nil {
		if err := bp.processFunc(batch); err != nil {
			bp.logger.Error("Batch processing failed",
				logger.Field{Key: "batchSize", Value: len(batch)},
				logger.Field{Key: "error", Value: err.Error()},
			)
		} else {
			bp.logger.Info("Batch processed successfully",
				logger.Field{Key: "batchSize", Value: len(batch)},
			)
		}
	}
}

// Stop stops the batch processor
func (bp *BatchProcessor) Stop() {
	close(bp.stopCh)
	bp.flushBatch() // Process remaining tasks
	bp.wg.Wait()
}

// TransactionManager handles transaction-like operations for DAG execution
type TransactionManager struct {
	dag                *DAG
	activeTransactions map[string]*Transaction
	mu                 sync.RWMutex
	logger             logger.Logger
}

// Transaction represents a transactional DAG execution
type Transaction struct {
	ID               string
	TaskID           string
	StartTime        time.Time
	CompletedNodes   []string
	SavePoints       map[string][]byte
	Status           TransactionStatus
	Context          context.Context
	CancelFunc       context.CancelFunc
	RollbackHandlers []RollbackHandler
}

// TransactionStatus represents the status of a transaction
type TransactionStatus int

const (
	TransactionActive TransactionStatus = iota
	TransactionCommitted
	TransactionRolledBack
	TransactionFailed
)

// RollbackHandler defines how to rollback operations
type RollbackHandler interface {
	Rollback(ctx context.Context, savePoint []byte) error
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(dag *DAG, logger logger.Logger) *TransactionManager {
	return &TransactionManager{
		dag:                dag,
		activeTransactions: make(map[string]*Transaction),
		logger:             logger,
	}
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction(taskID string) *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	tx := &Transaction{
		ID:               fmt.Sprintf("tx_%s_%d", taskID, time.Now().UnixNano()),
		TaskID:           taskID,
		StartTime:        time.Now(),
		CompletedNodes:   []string{},
		SavePoints:       make(map[string][]byte),
		Status:           TransactionActive,
		Context:          ctx,
		CancelFunc:       cancel,
		RollbackHandlers: []RollbackHandler{},
	}

	tm.activeTransactions[tx.ID] = tx

	tm.logger.Info("Transaction started",
		logger.Field{Key: "transactionID", Value: tx.ID},
		logger.Field{Key: "taskID", Value: taskID},
	)

	return tx
}

// AddSavePoint adds a save point to the transaction
func (tm *TransactionManager) AddSavePoint(txID, nodeID string, data []byte) error {
	tm.mu.RLock()
	tx, exists := tm.activeTransactions[txID]
	tm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	if tx.Status != TransactionActive {
		return fmt.Errorf("transaction %s is not active", txID)
	}

	tx.SavePoints[nodeID] = data
	tm.logger.Info("Save point added",
		logger.Field{Key: "transactionID", Value: txID},
		logger.Field{Key: "nodeID", Value: nodeID},
	)

	return nil
}

// CommitTransaction commits a transaction
func (tm *TransactionManager) CommitTransaction(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.activeTransactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	if tx.Status != TransactionActive {
		return fmt.Errorf("transaction %s is not active", txID)
	}

	tx.Status = TransactionCommitted
	tx.CancelFunc()
	delete(tm.activeTransactions, txID)

	tm.logger.Info("Transaction committed",
		logger.Field{Key: "transactionID", Value: txID},
		logger.Field{Key: "duration", Value: time.Since(tx.StartTime)},
	)

	return nil
}

// RollbackTransaction rolls back a transaction
func (tm *TransactionManager) RollbackTransaction(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.activeTransactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	if tx.Status != TransactionActive {
		return fmt.Errorf("transaction %s is not active", txID)
	}

	tx.Status = TransactionRolledBack
	tx.CancelFunc()

	// Execute rollback handlers in reverse order
	for i := len(tx.RollbackHandlers) - 1; i >= 0; i-- {
		handler := tx.RollbackHandlers[i]
		if err := handler.Rollback(tx.Context, nil); err != nil {
			tm.logger.Error("Rollback handler failed",
				logger.Field{Key: "transactionID", Value: txID},
				logger.Field{Key: "error", Value: err.Error()},
			)
		}
	}

	delete(tm.activeTransactions, txID)

	tm.logger.Info("Transaction rolled back",
		logger.Field{Key: "transactionID", Value: txID},
		logger.Field{Key: "duration", Value: time.Since(tx.StartTime)},
	)

	return nil
}

// CleanupManager handles cleanup of completed tasks and resources
type CleanupManager struct {
	dag               *DAG
	cleanupInterval   time.Duration
	retentionPeriod   time.Duration
	maxCompletedTasks int
	stopCh            chan struct{}
	logger            logger.Logger
}

// NewCleanupManager creates a new cleanup manager
func NewCleanupManager(dag *DAG, cleanupInterval, retentionPeriod time.Duration, maxCompletedTasks int, logger logger.Logger) *CleanupManager {
	return &CleanupManager{
		dag:               dag,
		cleanupInterval:   cleanupInterval,
		retentionPeriod:   retentionPeriod,
		maxCompletedTasks: maxCompletedTasks,
		stopCh:            make(chan struct{}),
		logger:            logger,
	}
}

// Start begins the cleanup routine
func (cm *CleanupManager) Start(ctx context.Context) {
	go cm.cleanupRoutine(ctx)
	cm.logger.Info("Cleanup manager started",
		logger.Field{Key: "interval", Value: cm.cleanupInterval},
		logger.Field{Key: "retention", Value: cm.retentionPeriod},
	)
}

// Stop stops the cleanup routine
func (cm *CleanupManager) Stop() {
	close(cm.stopCh)
	cm.logger.Info("Cleanup manager stopped")
}

// cleanupRoutine performs periodic cleanup
func (cm *CleanupManager) cleanupRoutine(ctx context.Context) {
	ticker := time.NewTicker(cm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.performCleanup()
		}
	}
}

// performCleanup cleans up old tasks and resources
func (cm *CleanupManager) performCleanup() {
	cleaned := 0
	cutoffTime := time.Now().Add(-cm.retentionPeriod)

	// Clean up old task managers
	var tasksToCleanup []string
	cm.dag.taskManager.ForEach(func(taskID string, manager *TaskManager) bool {
		if manager.createdAt.Before(cutoffTime) {
			tasksToCleanup = append(tasksToCleanup, taskID)
		}
		return true
	})

	for _, taskID := range tasksToCleanup {
		cm.dag.taskManager.Set(taskID, nil)
		cleaned++
	}

	if cleaned > 0 {
		cm.logger.Info("Cleanup completed",
			logger.Field{Key: "cleanedTasks", Value: cleaned},
			logger.Field{Key: "cutoffTime", Value: cutoffTime},
		)
	}
}

// WebhookManager handles webhook notifications
type WebhookManager struct {
	webhooks map[string][]WebhookConfig
	client   HTTPClient
	logger   logger.Logger
	mu       sync.RWMutex
}

// WebhookConfig defines webhook configuration
type WebhookConfig struct {
	URL        string
	Headers    map[string]string
	Timeout    time.Duration
	RetryCount int
	Events     []string // Which events to trigger on
}

// HTTPClient interface for HTTP requests
type HTTPClient interface {
	Post(url string, contentType string, body []byte, headers map[string]string) error
}

// WebhookEvent represents an event to send via webhook
type WebhookEvent struct {
	Type      string      `json:"type"`
	TaskID    string      `json:"task_id,omitempty"`
	NodeID    string      `json:"node_id,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data,omitempty"`
}

// NewWebhookManager creates a new webhook manager
func NewWebhookManager(client HTTPClient, logger logger.Logger) *WebhookManager {
	return &WebhookManager{
		webhooks: make(map[string][]WebhookConfig),
		client:   client,
		logger:   logger,
	}
}

// AddWebhook adds a webhook configuration
func (wm *WebhookManager) AddWebhook(eventType string, config WebhookConfig) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.webhooks[eventType] = append(wm.webhooks[eventType], config)
	wm.logger.Info("Webhook added",
		logger.Field{Key: "eventType", Value: eventType},
		logger.Field{Key: "url", Value: config.URL},
	)
}

// TriggerWebhook sends webhook notifications for an event
func (wm *WebhookManager) TriggerWebhook(event WebhookEvent) {
	wm.mu.RLock()
	configs := wm.webhooks[event.Type]
	wm.mu.RUnlock()

	if len(configs) == 0 {
		return
	}

	data, err := json.Marshal(event)
	if err != nil {
		wm.logger.Error("Failed to marshal webhook event",
			logger.Field{Key: "error", Value: err.Error()},
		)
		return
	}

	for _, config := range configs {
		go wm.sendWebhook(config, data)
	}
}

// sendWebhook sends a single webhook with retry logic
func (wm *WebhookManager) sendWebhook(config WebhookConfig, data []byte) {
	for attempt := 0; attempt <= config.RetryCount; attempt++ {
		err := wm.client.Post(config.URL, "application/json", data, config.Headers)
		if err == nil {
			wm.logger.Info("Webhook sent successfully",
				logger.Field{Key: "url", Value: config.URL},
				logger.Field{Key: "attempt", Value: attempt + 1},
			)
			return
		}

		if attempt < config.RetryCount {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	wm.logger.Error("Webhook failed after all retries",
		logger.Field{Key: "url", Value: config.URL},
		logger.Field{Key: "attempts", Value: config.RetryCount + 1},
	)
}
