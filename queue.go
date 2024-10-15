package mq

import (
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

func (b *Broker) NewQueue(qName string) *Queue {
	q, ok := b.queues.Get(qName)
	if ok {
		return q
	}
	q = newQueue(qName, b.opts.queueSize)
	b.queues.Set(qName, q)
	go b.dispatchWorker(q)
	return q
}
