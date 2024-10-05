package v2

import (
	"github.com/oarkflow/xsync"
)

type Queue struct {
	name      string
	consumers xsync.IMap[string, *consumer]
	tasks     chan *QueuedTask // channel to hold tasks
}

func newQueue(name string, queueSize int) *Queue {
	return &Queue{
		name:      name,
		consumers: xsync.NewMap[string, *consumer](),
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
