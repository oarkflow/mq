package v2

import (
	"github.com/oarkflow/xsync"
)

type Queue struct {
	name      string
	consumers xsync.IMap[string, *consumer]
}

func newQueue(name string) *Queue {
	return &Queue{
		name:      name,
		consumers: xsync.NewMap[string, *consumer](),
	}
}

func (b *Broker) NewQueue(qName string) *Queue {
	q, ok := b.queues.Get(qName)
	if ok {
		return q
	}
	q = newQueue(qName)
	b.queues.Set(qName, q)
	return q
}
