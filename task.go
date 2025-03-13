package mq

import (
	"time"

	"github.com/oarkflow/json"
)

type Task struct {
	CreatedAt   time.Time       `json:"created_at"`
	ProcessedAt time.Time       `json:"processed_at"`
	Expiry      time.Time       `json:"expiry"`
	Error       error           `json:"error"`
	ID          string          `json:"id"`
	Topic       string          `json:"topic"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	dag         any
}

func (t *Task) GetFlow() any {
	return t.dag
}

func NewTask(id string, payload json.RawMessage, nodeKey string, opts ...TaskOption) *Task {
	if id == "" {
		id = NewID()
	}
	task := &Task{ID: id, Payload: payload, Topic: nodeKey, CreatedAt: time.Now()}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

// TaskOption defines a function type for setting options.
type TaskOption func(*Task)

func WithDAG(dag any) TaskOption {
	return func(opts *Task) {
		opts.dag = dag
	}
}
