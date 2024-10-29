package dag

import (
	"sync"

	"github.com/oarkflow/mq"
)

type Operations struct {
	mu       *sync.RWMutex
	Handlers map[string]func(string) mq.Processor
}

var ops = &Operations{mu: &sync.RWMutex{}, Handlers: make(map[string]func(string) mq.Processor)}

func AddHandler(key string, handler func(string) mq.Processor) {
	ops.mu.Lock()
	ops.Handlers[key] = handler
	ops.mu.Unlock()
}

func GetHandler(key string) func(string) mq.Processor {
	return ops.Handlers[key]
}

func AvailableHandlers() []string {
	var op []string
	for opt := range ops.Handlers {
		op = append(op, opt)
	}
	return op
}
