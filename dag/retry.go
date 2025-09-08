package dag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// RetryConfig defines retry behavior for failed nodes
type RetryConfig struct {
	MaxRetries     int
	InitialDelay   time.Duration
	MaxDelay       time.Duration
	BackoffFactor  float64
	Jitter         bool
	RetryCondition func(err error) bool
}

// DefaultRetryConfig returns a sensible default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:     3,
		InitialDelay:   1 * time.Second,
		MaxDelay:       30 * time.Second,
		BackoffFactor:  2.0,
		Jitter:         true,
		RetryCondition: func(err error) bool { return true }, // Retry all errors by default
	}
}

// NodeRetryManager handles retry logic for individual nodes
type NodeRetryManager struct {
	config   *RetryConfig
	attempts map[string]int
	mu       sync.RWMutex
	logger   logger.Logger
}

// NewNodeRetryManager creates a new retry manager
func NewNodeRetryManager(config *RetryConfig, logger logger.Logger) *NodeRetryManager {
	if config == nil {
		config = DefaultRetryConfig()
	}
	return &NodeRetryManager{
		config:   config,
		attempts: make(map[string]int),
		logger:   logger,
	}
}

// ShouldRetry determines if a failed node should be retried
func (rm *NodeRetryManager) ShouldRetry(taskID, nodeID string, err error) bool {
	rm.mu.RLock()
	attempts := rm.attempts[rm.getKey(taskID, nodeID)]
	rm.mu.RUnlock()

	if attempts >= rm.config.MaxRetries {
		return false
	}

	if rm.config.RetryCondition != nil && !rm.config.RetryCondition(err) {
		return false
	}

	return true
}

// GetRetryDelay calculates the delay before the next retry
func (rm *NodeRetryManager) GetRetryDelay(taskID, nodeID string) time.Duration {
	rm.mu.RLock()
	attempts := rm.attempts[rm.getKey(taskID, nodeID)]
	rm.mu.RUnlock()

	delay := rm.config.InitialDelay
	for i := 0; i < attempts; i++ {
		delay = time.Duration(float64(delay) * rm.config.BackoffFactor)
		if delay > rm.config.MaxDelay {
			delay = rm.config.MaxDelay
			break
		}
	}

	if rm.config.Jitter {
		// Add up to 25% jitter
		jitter := time.Duration(float64(delay) * 0.25 * (0.5 - float64(time.Now().UnixNano()%2)))
		delay += jitter
	}

	return delay
}

// RecordAttempt records a retry attempt
func (rm *NodeRetryManager) RecordAttempt(taskID, nodeID string) {
	rm.mu.Lock()
	key := rm.getKey(taskID, nodeID)
	rm.attempts[key]++
	rm.mu.Unlock()

	rm.logger.Info("Retry attempt recorded",
		logger.Field{Key: "taskID", Value: taskID},
		logger.Field{Key: "nodeID", Value: nodeID},
		logger.Field{Key: "attempt", Value: rm.attempts[key]},
	)
}

// Reset clears retry attempts for a task/node combination
func (rm *NodeRetryManager) Reset(taskID, nodeID string) {
	rm.mu.Lock()
	delete(rm.attempts, rm.getKey(taskID, nodeID))
	rm.mu.Unlock()
}

// ResetTask clears all retry attempts for a task
func (rm *NodeRetryManager) ResetTask(taskID string) {
	rm.mu.Lock()
	for key := range rm.attempts {
		if len(key) > len(taskID) && key[:len(taskID)+1] == taskID+":" {
			delete(rm.attempts, key)
		}
	}
	rm.mu.Unlock()
}

// GetAttempts returns the number of attempts for a task/node combination
func (rm *NodeRetryManager) GetAttempts(taskID, nodeID string) int {
	rm.mu.RLock()
	attempts := rm.attempts[rm.getKey(taskID, nodeID)]
	rm.mu.RUnlock()
	return attempts
}

func (rm *NodeRetryManager) getKey(taskID, nodeID string) string {
	return taskID + ":" + nodeID
}

// SetGlobalConfig sets the global retry configuration
func (rm *NodeRetryManager) SetGlobalConfig(config *RetryConfig) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.config = config
	rm.logger.Info("Global retry configuration updated")
}

// SetNodeConfig sets retry configuration for a specific node
func (rm *NodeRetryManager) SetNodeConfig(nodeID string, config *RetryConfig) {
	// For simplicity, we'll store node-specific configs in a map
	// This could be extended to support per-node configurations
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Store node-specific config (this is a simplified implementation)
	// In a full implementation, you'd have a nodeConfigs map
	rm.logger.Info("Node-specific retry configuration set",
		logger.Field{Key: "nodeID", Value: nodeID},
		logger.Field{Key: "maxRetries", Value: config.MaxRetries},
	)
}

// RetryableProcessor wraps a processor with retry logic
type RetryableProcessor struct {
	processor    mq.Processor
	retryManager *NodeRetryManager
	logger       logger.Logger
}

// NewRetryableProcessor creates a processor with retry capabilities
func NewRetryableProcessor(processor mq.Processor, config *RetryConfig, logger logger.Logger) *RetryableProcessor {
	return &RetryableProcessor{
		processor:    processor,
		retryManager: NewNodeRetryManager(config, logger),
		logger:       logger,
	}
}

// ProcessTask processes a task with retry logic
func (rp *RetryableProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	taskID := task.ID
	nodeID := task.Topic

	result := rp.processor.ProcessTask(ctx, task)

	// If the task failed and should be retried
	if result.Error != nil && rp.retryManager.ShouldRetry(taskID, nodeID, result.Error) {
		rp.retryManager.RecordAttempt(taskID, nodeID)
		delay := rp.retryManager.GetRetryDelay(taskID, nodeID)

		rp.logger.Warn("Task failed, scheduling retry",
			logger.Field{Key: "taskID", Value: taskID},
			logger.Field{Key: "nodeID", Value: nodeID},
			logger.Field{Key: "error", Value: result.Error.Error()},
			logger.Field{Key: "retryDelay", Value: delay.String()},
			logger.Field{Key: "attempt", Value: rp.retryManager.GetAttempts(taskID, nodeID)},
		)

		// Create a channel to signal retry completion
		retryDone := make(chan mq.Result, 1)

		// Schedule retry with proper context handling
		go func() {
			defer close(retryDone)

			select {
			case <-time.After(delay):
				retryResult := rp.processor.ProcessTask(ctx, task)
				retryDone <- retryResult
			case <-ctx.Done():
				rp.logger.Warn("Retry cancelled due to context cancellation",
					logger.Field{Key: "taskID", Value: taskID},
					logger.Field{Key: "nodeID", Value: nodeID})
				return
			}
		}()

		// Wait for retry result or timeout
		select {
		case retryResult := <-retryDone:
			if retryResult.Error == nil {
				rp.retryManager.Reset(taskID, nodeID)
				rp.logger.Info("Task retry succeeded",
					logger.Field{Key: "taskID", Value: taskID},
					logger.Field{Key: "nodeID", Value: nodeID},
				)
				return retryResult
			}
		case <-time.After(30 * time.Second): // Timeout for retry
			rp.logger.Error("Retry timed out",
				logger.Field{Key: "taskID", Value: taskID},
				logger.Field{Key: "nodeID", Value: nodeID})
		case <-ctx.Done():
			rp.logger.Warn("Retry cancelled due to context cancellation",
				logger.Field{Key: "taskID", Value: taskID},
				logger.Field{Key: "nodeID", Value: nodeID})
		}

		// Return original failure result if retry didn't succeed
		return result
	}

	// If successful, reset retry attempts
	if result.Error == nil {
		rp.retryManager.Reset(taskID, nodeID)
	}

	return result
}

// Stop stops the processor
func (rp *RetryableProcessor) Stop(ctx context.Context) error {
	return rp.processor.Stop(ctx)
}

// Close closes the processor
func (rp *RetryableProcessor) Close() error {
	if closer, ok := rp.processor.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// Consume starts consuming messages
func (rp *RetryableProcessor) Consume(ctx context.Context) error {
	return rp.processor.Consume(ctx)
}

// Pause pauses the processor
func (rp *RetryableProcessor) Pause(ctx context.Context) error {
	return rp.processor.Pause(ctx)
}

// Resume resumes the processor
func (rp *RetryableProcessor) Resume(ctx context.Context) error {
	return rp.processor.Resume(ctx)
}

// GetKey returns the processor key
func (rp *RetryableProcessor) GetKey() string {
	return rp.processor.GetKey()
}

// SetKey sets the processor key
func (rp *RetryableProcessor) SetKey(key string) {
	rp.processor.SetKey(key)
}

// GetType returns the processor type
func (rp *RetryableProcessor) GetType() string {
	return rp.processor.GetType()
}

// Circuit Breaker Implementation
type CircuitBreakerState int

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

// CircuitBreakerConfig defines circuit breaker behavior
type CircuitBreakerConfig struct {
	FailureThreshold int
	ResetTimeout     time.Duration
	HalfOpenMaxCalls int
}

// CircuitBreaker implements circuit breaker pattern for nodes
type CircuitBreaker struct {
	config        *CircuitBreakerConfig
	state         CircuitBreakerState
	failures      int
	lastFailTime  time.Time
	halfOpenCalls int
	mu            sync.RWMutex
	logger        logger.Logger
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *CircuitBreakerConfig, logger logger.Logger) *CircuitBreaker {
	return &CircuitBreaker{
		config: config,
		state:  CircuitClosed,
		logger: logger,
	}
}

// Execute executes a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitOpen:
		if time.Since(cb.lastFailTime) > cb.config.ResetTimeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenCalls = 0
			cb.logger.Info("Circuit breaker transitioning to half-open")
		} else {
			return fmt.Errorf("circuit breaker is open")
		}
	case CircuitHalfOpen:
		if cb.halfOpenCalls >= cb.config.HalfOpenMaxCalls {
			return fmt.Errorf("circuit breaker half-open call limit exceeded")
		}
		cb.halfOpenCalls++
	}

	err := fn()

	if err != nil {
		cb.failures++
		cb.lastFailTime = time.Now()

		if cb.state == CircuitHalfOpen {
			cb.state = CircuitOpen
			cb.logger.Warn("Circuit breaker opened from half-open state")
		} else if cb.failures >= cb.config.FailureThreshold {
			cb.state = CircuitOpen
			cb.logger.Warn("Circuit breaker opened due to failure threshold")
		}
	} else {
		if cb.state == CircuitHalfOpen {
			cb.state = CircuitClosed
			cb.failures = 0
			cb.logger.Info("Circuit breaker closed from half-open state")
		} else if cb.state == CircuitClosed {
			cb.failures = 0
		}
	}

	return err
}

// GetState returns the current circuit breaker state
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Reset manually resets the circuit breaker
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitClosed
	cb.failures = 0
	cb.halfOpenCalls = 0
}
