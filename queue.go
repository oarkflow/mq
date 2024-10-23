package mq

import (
	"context"

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

func (b *Broker) NewQueue(name string) *Queue {
	q := &Queue{
		name:      name,
		tasks:     make(chan *QueuedTask, b.opts.queueSize),
		consumers: memory.New[string, *consumer](),
	}
	b.queues.Set(name, q)

	// Create DLQ for the queue
	dlq := &Queue{
		name:      name + "_dlq",
		tasks:     make(chan *QueuedTask, b.opts.queueSize),
		consumers: memory.New[string, *consumer](),
	}
	b.deadLetter.Set(name, dlq)
	ctx := context.Background()
	go b.dispatchWorker(ctx, q)
	go b.dispatchWorker(ctx, dlq)
	return q
}

type QueueTask struct {
	ctx      context.Context
	payload  *Task
	priority int
	index    int // The index in the heap
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
	old[n-1] = nil  // avoid memory leak
	task.index = -1 // for safety
	*pq = old[0 : n-1]
	return task
}
