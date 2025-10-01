package v2

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------- Public API Interfaces ----------------------

type Processor func(ctx context.Context, in any) (any, error)

type Node interface {
	ID() string
	Start(ctx context.Context, in <-chan any) <-chan any
}

type Pipeline interface {
	Start(ctx context.Context, inputs <-chan any) (<-chan any, error)
}

// ---------------------- Processor Registry ----------------------

var procRegistry = map[string]Processor{}

func RegisterProcessor(name string, p Processor) {
	procRegistry[name] = p
}

func GetProcessor(name string) (Processor, bool) {
	p, ok := procRegistry[name]
	return p, ok
}

// ---------------------- Ring Buffer (SPSC lock-free) ----------------------

type RingBuffer struct {
	buf  []any
	mask uint64
	head uint64
	tail uint64
}

func NewRingBuffer(size uint64) *RingBuffer {
	if size == 0 || (size&(size-1)) != 0 {
		panic("ring size must be power of two")
	}
	return &RingBuffer{buf: make([]any, size), mask: size - 1}
}

func (r *RingBuffer) Push(v any) bool {
	t := atomic.LoadUint64(&r.tail)
	h := atomic.LoadUint64(&r.head)
	if t-h == uint64(len(r.buf)) {
		return false
	}
	r.buf[t&r.mask] = v
	atomic.AddUint64(&r.tail, 1)
	return true
}

func (r *RingBuffer) Pop() (any, bool) {
	h := atomic.LoadUint64(&r.head)
	t := atomic.LoadUint64(&r.tail)
	if t == h {
		return nil, false
	}
	v := r.buf[h&r.mask]
	atomic.AddUint64(&r.head, 1)
	return v, true
}

// ---------------------- Node Implementations ----------------------

type ChannelNode struct {
	id        string
	processor Processor
	buf       int
	workers   int
}

func NewChannelNode(id string, proc Processor, buf int, workers int) *ChannelNode {
	if buf <= 0 {
		buf = 64
	}
	if workers <= 0 {
		workers = 1
	}
	return &ChannelNode{id: id, processor: proc, buf: buf, workers: workers}
}

func (c *ChannelNode) ID() string { return c.id }

func (c *ChannelNode) Start(ctx context.Context, in <-chan any) <-chan any {
	out := make(chan any, c.buf)
	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
					res, err := c.processor(ctx, v)
					if err != nil {
						fmt.Fprintf(os.Stderr, "processor %s error: %v\n", c.id, err)
						continue
					}
					select {
					case out <- res:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

type PageNode struct {
	id        string
	processor Processor
	buf       int
	workers   int
}

func NewPageNode(id string, proc Processor, buf int, workers int) *PageNode {
	if buf <= 0 {
		buf = 64
	}
	if workers <= 0 {
		workers = 1
	}
	return &PageNode{id: id, processor: proc, buf: buf, workers: workers}
}

func (c *PageNode) ID() string { return c.id }

func (c *PageNode) Start(ctx context.Context, in <-chan any) <-chan any {
	out := make(chan any, c.buf)
	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
					res, err := c.processor(ctx, v)
					if err != nil {
						fmt.Fprintf(os.Stderr, "processor %s error: %v\n", c.id, err)
						continue
					}
					select {
					case out <- res:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

type RingNode struct {
	id        string
	processor Processor
	size      uint64
}

func NewRingNode(id string, proc Processor, size uint64) *RingNode {
	if size == 0 {
		size = 1024
	}
	n := uint64(1)
	for n < size {
		n <<= 1
	}
	return &RingNode{id: id, processor: proc, size: n}
}

func (r *RingNode) ID() string { return r.id }

func (r *RingNode) Start(ctx context.Context, in <-chan any) <-chan any {
	out := make(chan any, 64)
	ring := NewRingBuffer(r.size)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}
				for !ring.Push(v) {
					time.Sleep(time.Microsecond)
					select {
					case <-ctx.Done():
						return
					default:
					}
				}
			}
		}
	}()
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				// process remaining items in ring
				for {
					v, ok := ring.Pop()
					if !ok {
						return
					}
					res, err := r.processor(ctx, v)
					if err != nil {
						fmt.Fprintf(os.Stderr, "processor %s error: %v\n", r.id, err)
						continue
					}
					select {
					case out <- res:
					case <-ctx.Done():
						return
					}
				}
			default:
				v, ok := ring.Pop()
				if !ok {
					time.Sleep(time.Microsecond)
					continue
				}
				res, err := r.processor(ctx, v)
				if err != nil {
					fmt.Fprintf(os.Stderr, "processor %s error: %v\n", r.id, err)
					continue
				}
				select {
				case out <- res:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// ---------------------- DAG Pipeline ----------------------

type NodeSpec struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Processor string `json:"processor"`
	Buf       int    `json:"buf,omitempty"`
	Workers   int    `json:"workers,omitempty"`
	RingSize  uint64 `json:"ring_size,omitempty"`
}

type EdgeSpec struct {
	Source  string   `json:"source"`
	Targets []string `json:"targets"`
	Type    string   `json:"type,omitempty"`
}

type PipelineSpec struct {
	Nodes      []NodeSpec                   `json:"nodes"`
	Edges      []EdgeSpec                   `json:"edges"`
	EntryIDs   []string                     `json:"entry_ids,omitempty"`
	Conditions map[string]map[string]string `json:"conditions,omitempty"`
}

type DAGPipeline struct {
	nodes      map[string]Node
	edges      map[string][]EdgeSpec
	rev        map[string][]string
	entry      []string
	conditions map[string]map[string]string
}

func NewDAGPipeline() *DAGPipeline {
	return &DAGPipeline{
		nodes:      map[string]Node{},
		edges:      map[string][]EdgeSpec{},
		rev:        map[string][]string{},
		conditions: map[string]map[string]string{},
	}
}

func (d *DAGPipeline) AddNode(n Node) {
	d.nodes[n.ID()] = n
}

func (d *DAGPipeline) AddEdge(from string, tos []string, typ string) {
	if typ == "" {
		typ = "simple"
	}
	e := EdgeSpec{Source: from, Targets: tos, Type: typ}
	d.edges[from] = append(d.edges[from], e)
	for _, to := range tos {
		d.rev[to] = append(d.rev[to], from)
	}
}

func (d *DAGPipeline) AddCondition(id string, cond map[string]string) {
	d.conditions[id] = cond
	for _, to := range cond {
		d.rev[to] = append(d.rev[to], id)
	}
}

func (d *DAGPipeline) Start(ctx context.Context, inputs <-chan any) (<-chan any, error) {
	nCh := map[string]chan any{}
	outCh := map[string]<-chan any{}
	wgMap := map[string]*sync.WaitGroup{}
	for id := range d.nodes {
		nCh[id] = make(chan any, 128)
		wgMap[id] = &sync.WaitGroup{}
	}
	if len(d.entry) == 0 {
		for id := range d.nodes {
			if len(d.rev[id]) == 0 {
				d.entry = append(d.entry, id)
			}
		}
	}
	for id, node := range d.nodes {
		in := nCh[id]
		out := node.Start(ctx, in)
		outCh[id] = out
		if cond, ok := d.conditions[id]; ok {
			go func(o <-chan any, cond map[string]string) {
				for v := range o {
					if m, ok := v.(map[string]any); ok {
						if status, ok := m["condition_status"].(string); ok {
							if target, ok := cond[status]; ok {
								wgMap[target].Add(1)
								go func(c chan any, v any, wg *sync.WaitGroup) {
									defer wg.Done()
									select {
									case c <- v:
									case <-ctx.Done():
									}
								}(nCh[target], v, wgMap[target])
							}
						}
					}
				}
			}(out, cond)
		} else {
			for _, e := range d.edges[id] {
				for _, dep := range e.Targets {
					if e.Type == "iterator" {
						go func(o <-chan any, c chan any, wg *sync.WaitGroup) {
							for v := range o {
								if arr, ok := v.([]any); ok {
									for _, item := range arr {
										wg.Add(1)
										go func(item any) {
											defer wg.Done()
											select {
											case c <- item:
											case <-ctx.Done():
											}
										}(item)
									}
								}
							}
						}(out, nCh[dep], wgMap[dep])
					} else {
						wgMap[dep].Add(1)
						go func(o <-chan any, c chan any, wg *sync.WaitGroup) {
							defer wg.Done()
							for v := range o {
								select {
								case c <- v:
								case <-ctx.Done():
									return
								}
							}
						}(out, nCh[dep], wgMap[dep])
					}
				}
			}
		}
	}
	for _, id := range d.entry {
		wgMap[id].Add(1)
	}
	go func() {
		defer func() {
			for _, id := range d.entry {
				wgMap[id].Done()
			}
		}()
		for v := range inputs {
			for _, id := range d.entry {
				select {
				case nCh[id] <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	for id, wg := range wgMap {
		go func(id string, wg *sync.WaitGroup, ch chan any) {
			time.Sleep(time.Millisecond)
			wg.Wait()
			close(ch)
		}(id, wg, nCh[id])
	}
	finalOut := make(chan any, 128)
	var wg sync.WaitGroup
	for id := range d.nodes {
		if len(d.edges[id]) == 0 && len(d.conditions[id]) == 0 {
			wg.Add(1)
			go func(o <-chan any) {
				defer wg.Done()
				for v := range o {
					select {
					case finalOut <- v:
					case <-ctx.Done():
						return
					}
				}
			}(outCh[id])
		}
	}
	go func() {
		wg.Wait()
		close(finalOut)
	}()
	return finalOut, nil
}

func BuildDAGFromSpec(spec PipelineSpec) (*DAGPipeline, error) {
	d := NewDAGPipeline()
	for _, ns := range spec.Nodes {
		proc, ok := GetProcessor(ns.Processor)
		if !ok {
			return nil, fmt.Errorf("processor %s not registered", ns.Processor)
		}
		var node Node
		switch ns.Type {
		case "channel":
			node = NewChannelNode(ns.ID, proc, ns.Buf, ns.Workers)
		case "ring":
			node = NewRingNode(ns.ID, proc, ns.RingSize)
		case "page":
			node = NewPageNode(ns.ID, proc, ns.Buf, ns.Workers)
		default:
			return nil, fmt.Errorf("unknown node type %s", ns.Type)
		}
		d.AddNode(node)
	}
	for _, e := range spec.Edges {
		if _, ok := d.nodes[e.Source]; !ok {
			return nil, fmt.Errorf("edge source %s not found", e.Source)
		}
		for _, tgt := range e.Targets {
			if _, ok := d.nodes[tgt]; !ok {
				return nil, fmt.Errorf("edge target %s not found", tgt)
			}
		}
		d.AddEdge(e.Source, e.Targets, e.Type)
	}
	if len(spec.EntryIDs) > 0 {
		d.entry = spec.EntryIDs
	}
	if spec.Conditions != nil {
		for id, cond := range spec.Conditions {
			d.AddCondition(id, cond)
		}
	}
	return d, nil
}
