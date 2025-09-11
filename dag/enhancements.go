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

	tasks := make([]*mq.Task, len(bp.buffer))
	copy(tasks, bp.buffer)
	bp.buffer = bp.buffer[:0] // Clear buffer
	bp.bufferMu.Unlock()

	if bp.processFunc != nil {
		if err := bp.processFunc(tasks); err != nil {
			bp.logger.Error("Batch processing failed",
				logger.Field{Key: "error", Value: err.Error()},
				logger.Field{Key: "batch_size", Value: len(tasks)},
			)
		} else {
			bp.logger.Info("Batch processed successfully",
				logger.Field{Key: "batch_size", Value: len(tasks)},
			)
		}
	}
}

// Stop stops the batch processor
func (bp *BatchProcessor) Stop() {
	close(bp.stopCh)
	bp.wg.Wait()

	// Flush remaining tasks
	bp.flushBatch()
}

// TransactionManager handles transaction-like operations for DAG execution
type TransactionManager struct {
	dag          *DAG
	transactions map[string]*Transaction
	savePoints   map[string][]SavePoint
	mu           sync.RWMutex
	logger       logger.Logger
}

// Transaction represents a transactional DAG execution
type Transaction struct {
	ID         string                 `json:"id"`
	TaskID     string                 `json:"task_id"`
	Status     TransactionStatus      `json:"status"`
	StartTime  time.Time              `json:"start_time"`
	EndTime    time.Time              `json:"end_time,omitempty"`
	Operations []TransactionOperation `json:"operations"`
	SavePoints []SavePoint            `json:"save_points"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// TransactionStatus represents the status of a transaction
type TransactionStatus string

const (
	TransactionStatusStarted    TransactionStatus = "started"
	TransactionStatusCommitted  TransactionStatus = "committed"
	TransactionStatusRolledBack TransactionStatus = "rolled_back"
	TransactionStatusFailed     TransactionStatus = "failed"
)

// TransactionOperation represents an operation within a transaction
type TransactionOperation struct {
	ID              string                 `json:"id"`
	Type            string                 `json:"type"`
	NodeID          string                 `json:"node_id"`
	Data            map[string]interface{} `json:"data"`
	Timestamp       time.Time              `json:"timestamp"`
	RollbackHandler RollbackHandler        `json:"-"`
}

// SavePoint represents a save point in a transaction
type SavePoint struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Timestamp time.Time              `json:"timestamp"`
	State     map[string]interface{} `json:"state"`
}

// RollbackHandler defines how to rollback operations
type RollbackHandler interface {
	Rollback(operation TransactionOperation) error
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(dag *DAG, logger logger.Logger) *TransactionManager {
	return &TransactionManager{
		dag:          dag,
		transactions: make(map[string]*Transaction),
		savePoints:   make(map[string][]SavePoint),
		logger:       logger,
	}
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction(taskID string) *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx := &Transaction{
		ID:         mq.NewID(),
		TaskID:     taskID,
		Status:     TransactionStatusStarted,
		StartTime:  time.Now(),
		Operations: make([]TransactionOperation, 0),
		SavePoints: make([]SavePoint, 0),
		Metadata:   make(map[string]interface{}),
	}

	tm.transactions[tx.ID] = tx
	if tm.dag.debug {
		tm.logger.Info("Transaction started",
			logger.Field{Key: "transaction_id", Value: tx.ID},
			logger.Field{Key: "task_id", Value: taskID},
		)
	}

	return tx
}

// AddOperation adds an operation to a transaction
func (tm *TransactionManager) AddOperation(txID string, operation TransactionOperation) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	if tx.Status != TransactionStatusStarted {
		return fmt.Errorf("transaction %s is not active", txID)
	}

	operation.ID = mq.NewID()
	operation.Timestamp = time.Now()
	tx.Operations = append(tx.Operations, operation)

	return nil
}

// AddSavePoint adds a save point to the transaction
func (tm *TransactionManager) AddSavePoint(txID, name string, state map[string]interface{}) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	savePoint := SavePoint{
		ID:        mq.NewID(),
		Name:      name,
		Timestamp: time.Now(),
		State:     state,
	}

	tx.SavePoints = append(tx.SavePoints, savePoint)
	tm.savePoints[txID] = tx.SavePoints

	tm.logger.Info("Save point created",
		logger.Field{Key: "transaction_id", Value: txID},
		logger.Field{Key: "save_point_name", Value: name},
	)

	return nil
}

// CommitTransaction commits a transaction
func (tm *TransactionManager) CommitTransaction(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	if tx.Status != TransactionStatusStarted {
		return fmt.Errorf("transaction %s is not active", txID)
	}

	tx.Status = TransactionStatusCommitted
	tx.EndTime = time.Now()

	if tm.dag.debug {
		tm.logger.Info("Transaction committed",
			logger.Field{Key: "transaction_id", Value: txID},
			logger.Field{Key: "operations_count", Value: len(tx.Operations)},
		)
	}

	// Clean up save points
	delete(tm.savePoints, txID)

	return nil
}

// RollbackTransaction rolls back a transaction
func (tm *TransactionManager) RollbackTransaction(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	if tx.Status != TransactionStatusStarted {
		return fmt.Errorf("transaction %s is not active", txID)
	}

	// Rollback operations in reverse order
	for i := len(tx.Operations) - 1; i >= 0; i-- {
		operation := tx.Operations[i]
		if operation.RollbackHandler != nil {
			if err := operation.RollbackHandler.Rollback(operation); err != nil {
				tm.logger.Error("Failed to rollback operation",
					logger.Field{Key: "transaction_id", Value: txID},
					logger.Field{Key: "operation_id", Value: operation.ID},
					logger.Field{Key: "error", Value: err.Error()},
				)
			}
		}
	}

	tx.Status = TransactionStatusRolledBack
	tx.EndTime = time.Now()

	tm.logger.Info("Transaction rolled back",
		logger.Field{Key: "transaction_id", Value: txID},
		logger.Field{Key: "operations_count", Value: len(tx.Operations)},
	)

	// Clean up save points
	delete(tm.savePoints, txID)

	return nil
}

// GetTransaction retrieves a transaction by ID
func (tm *TransactionManager) GetTransaction(txID string) (*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tx, exists := tm.transactions[txID]
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", txID)
	}

	// Return a copy
	txCopy := *tx
	return &txCopy, nil
}

// CleanupManager handles cleanup of completed tasks and resources
type CleanupManager struct {
	dag             *DAG
	cleanupInterval time.Duration
	retentionPeriod time.Duration
	maxEntries      int
	logger          logger.Logger
	stopCh          chan struct{}
	running         bool
	mu              sync.RWMutex
}

// NewCleanupManager creates a new cleanup manager
func NewCleanupManager(dag *DAG, cleanupInterval, retentionPeriod time.Duration, maxEntries int, logger logger.Logger) *CleanupManager {
	return &CleanupManager{
		dag:             dag,
		cleanupInterval: cleanupInterval,
		retentionPeriod: retentionPeriod,
		maxEntries:      maxEntries,
		logger:          logger,
		stopCh:          make(chan struct{}),
	}
}

// Start begins the cleanup routine
func (cm *CleanupManager) Start(ctx context.Context) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.running {
		return
	}

	cm.running = true
	go cm.cleanupRoutine(ctx)

	cm.logger.Info("Cleanup manager started")
}

// Stop stops the cleanup routine
func (cm *CleanupManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.running {
		return
	}

	cm.running = false
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
	cutoff := time.Now().Add(-cm.retentionPeriod)

	// Clean up old task managers
	var toDelete []string
	cm.dag.taskManager.ForEach(func(taskID string, tm *TaskManager) bool {
		if tm.createdAt.Before(cutoff) {
			toDelete = append(toDelete, taskID)
		}
		return true
	})

	for _, taskID := range toDelete {
		if tm, exists := cm.dag.taskManager.Get(taskID); exists {
			tm.Stop()
			cm.dag.taskManager.Del(taskID)
		}
	}

	// Clean up circuit breakers for removed nodes
	cm.dag.circuitBreakersMu.Lock()
	for nodeID := range cm.dag.circuitBreakers {
		if _, exists := cm.dag.nodes.Get(nodeID); !exists {
			delete(cm.dag.circuitBreakers, nodeID)
		}
	}
	cm.dag.circuitBreakersMu.Unlock()

	if len(toDelete) > 0 {
		cm.logger.Info("Cleanup completed",
			logger.Field{Key: "cleaned_tasks", Value: len(toDelete)},
		)
	}
}

// WebhookManager handles webhook notifications
type WebhookManager struct {
	webhooks   map[string][]WebhookConfig
	httpClient HTTPClient
	logger     logger.Logger
	mu         sync.RWMutex
}

// WebhookConfig defines webhook configuration
type WebhookConfig struct {
	URL        string            `json:"url"`
	Headers    map[string]string `json:"headers"`
	Method     string            `json:"method"`
	RetryCount int               `json:"retry_count"`
	Timeout    time.Duration     `json:"timeout"`
	Events     []string          `json:"events"`
}

// HTTPClient interface for HTTP requests
type HTTPClient interface {
	Post(url string, contentType string, body []byte, headers map[string]string) error
}

// WebhookEvent represents an event to send via webhook
type WebhookEvent struct {
	Type      string                 `json:"type"`
	TaskID    string                 `json:"task_id"`
	NodeID    string                 `json:"node_id,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// NewWebhookManager creates a new webhook manager
func NewWebhookManager(httpClient HTTPClient, logger logger.Logger) *WebhookManager {
	return &WebhookManager{
		webhooks:   make(map[string][]WebhookConfig),
		httpClient: httpClient,
		logger:     logger,
	}
}

// AddWebhook adds a webhook configuration
func (wm *WebhookManager) AddWebhook(event string, config WebhookConfig) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if config.Method == "" {
		config.Method = "POST"
	}
	if config.RetryCount == 0 {
		config.RetryCount = 3
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	wm.webhooks[event] = append(wm.webhooks[event], config)

	wm.logger.Info("Webhook added",
		logger.Field{Key: "event", Value: event},
		logger.Field{Key: "url", Value: config.URL},
	)
}

// TriggerWebhook sends webhook notifications for an event
func (wm *WebhookManager) TriggerWebhook(event WebhookEvent) {
	wm.mu.RLock()
	configs, exists := wm.webhooks[event.Type]
	wm.mu.RUnlock()

	if !exists {
		return
	}

	for _, config := range configs {
		// Check if this webhook should handle this event
		if len(config.Events) > 0 {
			found := false
			for _, eventType := range config.Events {
				if eventType == event.Type {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		go wm.sendWebhook(config, event)
	}
}

// sendWebhook sends a single webhook with retry logic
func (wm *WebhookManager) sendWebhook(config WebhookConfig, event WebhookEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		wm.logger.Error("Failed to marshal webhook payload",
			logger.Field{Key: "error", Value: err.Error()},
		)
		return
	}

	for attempt := 0; attempt < config.RetryCount; attempt++ {
		err := wm.httpClient.Post(config.URL, "application/json", payload, config.Headers)
		if err == nil {
			wm.logger.Info("Webhook sent successfully",
				logger.Field{Key: "url", Value: config.URL},
				logger.Field{Key: "event_type", Value: event.Type},
			)
			return
		}

		wm.logger.Warn("Webhook delivery failed",
			logger.Field{Key: "url", Value: config.URL},
			logger.Field{Key: "attempt", Value: attempt + 1},
			logger.Field{Key: "error", Value: err.Error()},
		)

		if attempt < config.RetryCount-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	wm.logger.Error("Webhook delivery failed after all retries",
		logger.Field{Key: "url", Value: config.URL},
		logger.Field{Key: "event_type", Value: event.Type},
	)
}
