package mq

import (
	"time"
)

// New type definitions for enhancements
type ThresholdConfig struct {
	HighMemory    int64         // e.g. in bytes
	LongExecution time.Duration // e.g. warning if task execution exceeds
}

type MetricsRegistry interface {
	Register(metricName string, value interface{})
	// ...other methods as needed...
}

type CircuitBreakerConfig struct {
	Enabled          bool
	FailureThreshold int
	ResetTimeout     time.Duration
}

type PoolOption func(*Pool)

func WithTaskQueueSize(size int) PoolOption {
	return func(p *Pool) {
		// Initialize the task queue with the specified size
		p.taskQueue = make(PriorityQueue, 0, size)
	}
}

func WithTaskTimeout(t time.Duration) PoolOption {
	return func(p *Pool) {
		p.timeout = t
	}
}

func WithCompletionCallback(callback func()) PoolOption {
	return func(p *Pool) {
		p.completionCallback = callback
	}
}

func WithMaxMemoryLoad(maxMemoryLoad int64) PoolOption {
	return func(p *Pool) {
		p.maxMemoryLoad = maxMemoryLoad
	}
}

func WithBatchSize(batchSize int) PoolOption {
	return func(p *Pool) {
		p.batchSize = batchSize
	}
}

func WithHandler(handler Handler) PoolOption {
	return func(p *Pool) {
		p.handler = handler
	}
}

func WithPoolCallback(callback Callback) PoolOption {
	return func(p *Pool) {
		p.callback = callback
	}
}

func WithTaskStorage(storage TaskStorage) PoolOption {
	return func(p *Pool) {
		p.taskStorage = storage
	}
}

// New option functions:

func WithWarningThresholds(thresholds ThresholdConfig) PoolOption {
	return func(p *Pool) {
		p.thresholds = thresholds
	}
}

func WithDiagnostics(enabled bool) PoolOption {
	return func(p *Pool) {
		p.diagnosticsEnabled = enabled
	}
}

func WithMetricsRegistry(registry MetricsRegistry) PoolOption {
	return func(p *Pool) {
		p.metricsRegistry = registry
	}
}

func WithCircuitBreaker(config CircuitBreakerConfig) PoolOption {
	return func(p *Pool) {
		p.circuitBreaker = config
	}
}

func WithGracefulShutdown(timeout time.Duration) PoolOption {
	return func(p *Pool) {
		p.gracefulShutdownTimeout = timeout
	}
}
