package v2

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

type List struct {
	mu       *sync.RWMutex
	Handlers map[string]*DAG
}

var dags = &List{mu: &sync.RWMutex{}, Handlers: make(map[string]*DAG)}

func AddDAG(key string, handler *DAG) {
	dags.mu.Lock()
	dags.Handlers[key] = handler
	dags.mu.Unlock()
}

func GetDAG(key string) *DAG {
	return dags.Handlers[key]
}

func ClearDAG() {
	dags.mu.Lock()
	clear(dags.Handlers)
	dags.mu.Unlock()
	dags.Handlers = make(map[string]*DAG)
}

func AvailableDAG() []string {
	var op []string
	for opt := range dags.Handlers {
		op = append(op, opt)
	}
	return op
}
