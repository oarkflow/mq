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

type Handler struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

func AvailableHandlers() []Handler {
	var op []Handler
	for opt, data := range ops.Handlers {
		handler, ok := data("").(Processor)
		if !ok {
			continue
		}
		h := Handler{
			Name: opt,
			Tags: handler.GetTags(),
		}
		op = append(op, h)
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

// HasPageNode checks if the DAG contains any Page nodes
func (d *DAG) HasPageNode() bool {
	return d.hasPageNode
}

// ContainsPageNodes iterates through all nodes to check if any are Page nodes
// This method provides an alternative way to check for Page nodes by examining
// the actual nodes rather than relying on the cached hasPageNode field
func (d *DAG) ContainsPageNodes() bool {
	var hasPage bool
	d.nodes.ForEach(func(_ string, node *Node) bool {
		if node.NodeType == Page {
			hasPage = true
			return false // Stop iteration when found
		}
		return true // Continue iteration
	})
	return hasPage
}
