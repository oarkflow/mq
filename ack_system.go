package mq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
)

// AckType defines the type of acknowledgment
type AckType int

const (
	// ACK indicates successful processing
	ACK AckType = iota
	// NACK indicates processing failure, message should be requeued
	NACK
	// REJECT indicates message should be moved to DLQ
	REJECT
)

// MessageAck represents an acknowledgment for a message
type MessageAck struct {
	TaskID    string
	AckType   AckType
	Reason    string
	Timestamp time.Time
}

// PendingMessage represents a message awaiting acknowledgment
type PendingMessage struct {
	Task       *Task
	QueueName  string
	SentAt     time.Time
	Deadline   time.Time
	RetryCount int
	ConsumerID string
}

// AckManager manages message acknowledgments and ensures at-least-once delivery
type AckManager struct {
	pending       map[string]*PendingMessage
	mu            sync.RWMutex
	ackTimeout    time.Duration
	maxRetries    int
	redeliverChan chan *PendingMessage
	ackChan       chan MessageAck
	shutdown      chan struct{}
	logger        logger.Logger
	onRedeliver   func(*PendingMessage)
	onAck         func(*PendingMessage)
	onNack        func(*PendingMessage)
	onReject      func(*PendingMessage)
	onTimeout     func(*PendingMessage)
}

// AckManagerConfig holds configuration for AckManager
type AckManagerConfig struct {
	AckTimeout    time.Duration
	MaxRetries    int
	CheckInterval time.Duration
	Logger        logger.Logger
}

// NewAckManager creates a new acknowledgment manager
func NewAckManager(config AckManagerConfig) *AckManager {
	if config.AckTimeout == 0 {
		config.AckTimeout = 30 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.CheckInterval == 0 {
		config.CheckInterval = 5 * time.Second
	}

	am := &AckManager{
		pending:       make(map[string]*PendingMessage),
		ackTimeout:    config.AckTimeout,
		maxRetries:    config.MaxRetries,
		redeliverChan: make(chan *PendingMessage, 1000),
		ackChan:       make(chan MessageAck, 1000),
		shutdown:      make(chan struct{}),
		logger:        config.Logger,
	}

	// Start background workers
	go am.processAcks()
	go am.checkTimeouts(config.CheckInterval)

	return am
}

// TrackMessage adds a message to the pending acknowledgment list
func (am *AckManager) TrackMessage(ctx context.Context, task *Task, queueName, consumerID string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.pending[task.ID]; exists {
		return fmt.Errorf("message already being tracked: %s", task.ID)
	}

	now := time.Now()
	pending := &PendingMessage{
		Task:       task,
		QueueName:  queueName,
		SentAt:     now,
		Deadline:   now.Add(am.ackTimeout),
		RetryCount: task.Retries,
		ConsumerID: consumerID,
	}

	am.pending[task.ID] = pending

	am.logger.Debug("Message tracked for acknowledgment",
		logger.Field{Key: "taskID", Value: task.ID},
		logger.Field{Key: "queue", Value: queueName},
		logger.Field{Key: "consumer", Value: consumerID})

	return nil
}

// Acknowledge processes an acknowledgment for a message
func (am *AckManager) Acknowledge(ctx context.Context, taskID string, ackType AckType, reason string) error {
	ack := MessageAck{
		TaskID:    taskID,
		AckType:   ackType,
		Reason:    reason,
		Timestamp: time.Now(),
	}

	select {
	case am.ackChan <- ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-am.shutdown:
		return fmt.Errorf("ack manager is shutting down")
	}
}

// processAcks processes incoming acknowledgments
func (am *AckManager) processAcks() {
	for {
		select {
		case ack := <-am.ackChan:
			am.handleAck(ack)
		case <-am.shutdown:
			return
		}
	}
}

// handleAck handles a single acknowledgment
func (am *AckManager) handleAck(ack MessageAck) {
	am.mu.Lock()
	pending, exists := am.pending[ack.TaskID]
	if !exists {
		am.mu.Unlock()
		am.logger.Warn("Received ACK for unknown message",
			logger.Field{Key: "taskID", Value: ack.TaskID})
		return
	}

	delete(am.pending, ack.TaskID)
	am.mu.Unlock()

	switch ack.AckType {
	case ACK:
		pending.Task.Status = Completed
		pending.Task.ProcessedAt = time.Now()
		am.logger.Info("Message acknowledged successfully",
			logger.Field{Key: "taskID", Value: ack.TaskID},
			logger.Field{Key: "queue", Value: pending.QueueName})
		if am.onAck != nil {
			am.onAck(pending)
		}

	case NACK:
		pending.RetryCount++
		if pending.RetryCount < am.maxRetries {
			am.logger.Info("Message NACKed, requeuing",
				logger.Field{Key: "taskID", Value: ack.TaskID},
				logger.Field{Key: "retryCount", Value: pending.RetryCount},
				logger.Field{Key: "reason", Value: ack.Reason})
			am.redeliverChan <- pending
			if am.onNack != nil {
				am.onNack(pending)
			}
		} else {
			am.logger.Warn("Message exceeded max retries, rejecting",
				logger.Field{Key: "taskID", Value: ack.TaskID},
				logger.Field{Key: "retries", Value: pending.RetryCount})
			pending.Task.Status = Failed
			if am.onReject != nil {
				am.onReject(pending)
			}
		}

	case REJECT:
		pending.Task.Status = Failed
		am.logger.Warn("Message rejected",
			logger.Field{Key: "taskID", Value: ack.TaskID},
			logger.Field{Key: "reason", Value: ack.Reason})
		if am.onReject != nil {
			am.onReject(pending)
		}
	}
}

// checkTimeouts periodically checks for messages that have exceeded their acknowledgment timeout
func (am *AckManager) checkTimeouts(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			am.processTimeouts()
		case <-am.shutdown:
			return
		}
	}
}

// processTimeouts finds and handles timed-out messages
func (am *AckManager) processTimeouts() {
	now := time.Now()
	am.mu.Lock()

	var timedOut []*PendingMessage
	for taskID, pending := range am.pending {
		if now.After(pending.Deadline) {
			timedOut = append(timedOut, pending)
			delete(am.pending, taskID)
		}
	}

	am.mu.Unlock()

	for _, pending := range timedOut {
		pending.RetryCount++
		if pending.RetryCount < am.maxRetries {
			am.logger.Warn("Message acknowledgment timeout, requeuing",
				logger.Field{Key: "taskID", Value: pending.Task.ID},
				logger.Field{Key: "retryCount", Value: pending.RetryCount},
				logger.Field{Key: "queue", Value: pending.QueueName})
			am.redeliverChan <- pending
			if am.onTimeout != nil {
				am.onTimeout(pending)
			}
		} else {
			am.logger.Error("Message exceeded max retries after timeout",
				logger.Field{Key: "taskID", Value: pending.Task.ID},
				logger.Field{Key: "retries", Value: pending.RetryCount})
			pending.Task.Status = Failed
			if am.onReject != nil {
				am.onReject(pending)
			}
		}
	}
}

// GetRedeliverChannel returns the channel for redelivering messages
func (am *AckManager) GetRedeliverChannel() <-chan *PendingMessage {
	return am.redeliverChan
}

// SetOnRedeliver sets the callback for message redelivery
func (am *AckManager) SetOnRedeliver(fn func(*PendingMessage)) {
	am.onRedeliver = fn
}

// SetOnAck sets the callback for successful acknowledgments
func (am *AckManager) SetOnAck(fn func(*PendingMessage)) {
	am.onAck = fn
}

// SetOnNack sets the callback for negative acknowledgments
func (am *AckManager) SetOnNack(fn func(*PendingMessage)) {
	am.onNack = fn
}

// SetOnReject sets the callback for rejected messages
func (am *AckManager) SetOnReject(fn func(*PendingMessage)) {
	am.onReject = fn
}

// SetOnTimeout sets the callback for timed-out messages
func (am *AckManager) SetOnTimeout(fn func(*PendingMessage)) {
	am.onTimeout = fn
}

// GetPendingCount returns the number of pending acknowledgments
func (am *AckManager) GetPendingCount() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.pending)
}

// GetPendingMessages returns all pending messages (for monitoring/debugging)
func (am *AckManager) GetPendingMessages() []*PendingMessage {
	am.mu.RLock()
	defer am.mu.RUnlock()

	messages := make([]*PendingMessage, 0, len(am.pending))
	for _, msg := range am.pending {
		messages = append(messages, msg)
	}
	return messages
}

// CancelPending removes a message from pending tracking (e.g., consumer disconnected)
func (am *AckManager) CancelPending(taskID string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	if pending, exists := am.pending[taskID]; exists {
		delete(am.pending, taskID)
		am.logger.Debug("Cancelled pending message",
			logger.Field{Key: "taskID", Value: taskID},
			logger.Field{Key: "queue", Value: pending.QueueName})
	}
}

// CancelAllForConsumer removes all pending messages for a specific consumer
func (am *AckManager) CancelAllForConsumer(consumerID string) int {
	am.mu.Lock()
	defer am.mu.Unlock()

	cancelled := 0
	for taskID, pending := range am.pending {
		if pending.ConsumerID == consumerID {
			delete(am.pending, taskID)
			cancelled++
			// Optionally requeue these messages
			go func(p *PendingMessage) {
				am.redeliverChan <- p
			}(pending)
		}
	}

	if cancelled > 0 {
		am.logger.Info("Cancelled pending messages for disconnected consumer",
			logger.Field{Key: "consumerID", Value: consumerID},
			logger.Field{Key: "count", Value: cancelled})
	}

	return cancelled
}

// Shutdown gracefully shuts down the acknowledgment manager
func (am *AckManager) Shutdown(ctx context.Context) error {
	close(am.shutdown)

	// Wait for pending acknowledgments with timeout
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			am.mu.RLock()
			pendingCount := len(am.pending)
			am.mu.RUnlock()
			return fmt.Errorf("shutdown timeout with %d pending messages", pendingCount)
		case <-ticker.C:
			am.mu.RLock()
			pendingCount := len(am.pending)
			am.mu.RUnlock()
			if pendingCount == 0 {
				am.logger.Info("AckManager shutdown complete")
				return nil
			}
		}
	}
}

// GetStats returns statistics about the acknowledgment manager
func (am *AckManager) GetStats() map[string]interface{} {
	am.mu.RLock()
	defer am.mu.RUnlock()

	var oldestPending time.Time
	var totalWaitTime time.Duration
	now := time.Now()

	for _, pending := range am.pending {
		if oldestPending.IsZero() || pending.SentAt.Before(oldestPending) {
			oldestPending = pending.SentAt
		}
		totalWaitTime += now.Sub(pending.SentAt)
	}

	avgWaitTime := time.Duration(0)
	if len(am.pending) > 0 {
		avgWaitTime = totalWaitTime / time.Duration(len(am.pending))
	}

	return map[string]interface{}{
		"pending_count":     len(am.pending),
		"oldest_pending":    oldestPending,
		"avg_wait_time":     avgWaitTime,
		"ack_timeout":       am.ackTimeout,
		"max_retries":       am.maxRetries,
		"redeliver_backlog": len(am.redeliverChan),
		"ack_backlog":       len(am.ackChan),
	}
}
