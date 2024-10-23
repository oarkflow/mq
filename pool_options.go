package mq

import (
	"time"
)

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
