package mq

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

type Queue struct {
	consumers storage.IMap[string, *consumer]
	tasks     chan *QueuedTask // channel to hold tasks
	name      string
}

func newQueue(name string, queueSize int) *Queue {
	return &Queue{
		name:      name,
		consumers: memory.New[string, *consumer](),
		tasks:     make(chan *QueuedTask, queueSize), // buffer size for tasks
	}
}

type QueueTask struct {
	ctx        context.Context
	payload    *Task
	priority   int
	retryCount int
	index      int
}

type PriorityQueue []*QueueTask

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	task := x.(*QueueTask)
	task.index = n
	*pq = append(*pq, task)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	task := old[n-1]
	task.index = -1
	*pq = old[0 : n-1]
	return task
}

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
	// new deduplication field
	DedupKey string `json:"dedup_key,omitempty"`
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

// new TaskOption for deduplication:
func WithDedupKey(key string) TaskOption {
	return func(t *Task) {
		t.DedupKey = key
	}
}

// Add advanced dead-letter queue management
func (b *Broker) ReprocessDLQ(queueName string) error {
	dlqName := queueName + "_dlq"
	dlq, ok := b.deadLetter.Get(dlqName)
	if !ok {
		return fmt.Errorf("dead-letter queue %s does not exist", dlqName)
	}
	for {
		select {
		case task := <-dlq.tasks:
			b.NewQueue(queueName).tasks <- task
		default:
			return nil
		}
	}
}
