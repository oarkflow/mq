package mq

import (
	"time"
)

// New type definitions for enhancements
type ThresholdConfig struct {
	HighMemory    int64
	LongExecution time.Duration
}

type MetricsRegistry interface {
	Register(metricName string, value interface{})
	Increment(metricName string)
	Get(metricName string) interface{}
}

type CircuitBreakerConfig struct {
	Enabled          bool
	FailureThreshold int
	ResetTimeout     time.Duration
}

type PoolOption func(*Pool)

func WithTaskQueueSize(size int) PoolOption {
	return func(p *Pool) {
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

func WithHealthServicePort(port int) PoolOption {
	return func(p *Pool) {
		p.port = port
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

func WithPlugin(plugin Plugin) PoolOption {
	return func(p *Pool) {
		p.plugins = append(p.plugins, plugin)
	}
}
