package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Task struct {
	ID          string          `json:"id"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	ProcessedAt time.Time       `json:"processed_at"`
	Status      string          `json:"status"`
	Error       error           `json:"error"`
}

type Result struct {
	Payload   json.RawMessage `json:"payload"`
	Queue     string          `json:"queue"`
	MessageID string          `json:"message_id"`
	Error     error           `json:"error,omitempty"`
	Status    string          `json:"status"`
}

const (
	SimpleEdge = iota
	LoopEdge
	ConditionEdge
)

type Edge struct {
	edgeType   int
	to         string
	conditions map[string]string
}

type Node struct {
	key     string
	handler func(context.Context, Task) Result
	edges   []Edge
}

type RadixTrie struct {
	children map[rune]*RadixTrie
	node     *Node
	mu       sync.RWMutex
}

func NewRadixTrie() *RadixTrie {
	return &RadixTrie{
		children: make(map[rune]*RadixTrie),
	}
}

func (trie *RadixTrie) Insert(key string, node *Node) {
	trie.mu.Lock()
	defer trie.mu.Unlock()

	current := trie
	for _, char := range key {
		if _, exists := current.children[char]; !exists {
			current.children[char] = NewRadixTrie()
		}
		current = current.children[char]
	}
	current.node = node
}

func (trie *RadixTrie) Search(key string) (*Node, bool) {
	trie.mu.RLock()
	defer trie.mu.RUnlock()
	current := trie
	for _, char := range key {
		if _, exists := current.children[char]; !exists {
			return nil, false
		}
		current = current.children[char]
	}
	if current.node != nil {
		return current.node, true
	}
	return nil, false
}

type DAG struct {
	trie *RadixTrie
	mu   sync.RWMutex
}

func NewDAG() *DAG {
	return &DAG{
		trie: NewRadixTrie(),
	}
}

func (d *DAG) AddNode(key string, handler func(context.Context, Task) Result, isRoot ...bool) {
	node := &Node{key: key, handler: handler}
	d.trie.Insert(key, node)
}

func (d *DAG) AddEdge(fromKey string, toKey string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	node, exists := d.trie.Search(fromKey)
	if !exists {
		fmt.Printf("Node %s not found to add edge.\n", fromKey)
		return
	}
	edge := Edge{edgeType: SimpleEdge, to: toKey}
	node.edges = append(node.edges, edge)
}

func (d *DAG) AddLoop(fromKey string, toKey string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	node, exists := d.trie.Search(fromKey)
	if !exists {
		fmt.Printf("Node %s not found to add loop edge.\n", fromKey)
		return
	}
	edge := Edge{edgeType: LoopEdge, to: toKey}
	node.edges = append(node.edges, edge)
}

func (d *DAG) AddCondition(fromKey string, conditions map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	node, exists := d.trie.Search(fromKey)
	if !exists {
		fmt.Printf("Node %s not found to add condition edge.\n", fromKey)
		return
	}
	edge := Edge{edgeType: ConditionEdge, conditions: conditions}
	node.edges = append(node.edges, edge)
}

type ProcessCallback func(ctx context.Context, key string, result Result) string

func (d *DAG) ProcessTask(ctx context.Context, key string, task Task) {
	node, exists := d.trie.Search(key)
	if !exists {
		fmt.Printf("Node %s not found.\n", key)
		return
	}
	result := node.handler(ctx, task)
	nextKey := d.callback(ctx, key, result)
	if nextKey != "" {
		d.ProcessTask(ctx, nextKey, task)
	}
}

func (d *DAG) ProcessLoop(ctx context.Context, key string, task Task) {
	_, exists := d.trie.Search(key)
	if !exists {
		fmt.Printf("Node %s not found.\n", key)
		return
	}
	var items []json.RawMessage
	err := json.Unmarshal(task.Payload, &items)
	if err != nil {
		fmt.Printf("Error unmarshaling payload as slice: %v\n", err)
		return
	}
	for _, item := range items {
		newTask := Task{
			ID:      task.ID,
			Payload: item,
		}

		d.ProcessTask(ctx, key, newTask)
	}
}

func (d *DAG) callback(ctx context.Context, currentKey string, result Result) string {
	fmt.Printf("Callback received result from %s: %s\n", currentKey, string(result.Payload))
	node, exists := d.trie.Search(currentKey)
	if !exists {
		return ""
	}
	for _, edge := range node.edges {
		switch edge.edgeType {
		case SimpleEdge:
			return edge.to
		case LoopEdge:

			d.ProcessLoop(ctx, edge.to, Task{Payload: result.Payload})
			return ""
		case ConditionEdge:
			if nextKey, conditionMet := edge.conditions[result.Status]; conditionMet {
				return nextKey
			}
		}
	}
	return ""
}

func Node1(ctx context.Context, task Task) Result {
	return Result{Payload: task.Payload, MessageID: task.ID}
}

func Node2(ctx context.Context, task Task) Result {
	return Result{Payload: task.Payload, MessageID: task.ID}
}

func Node3(ctx context.Context, task Task) Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return Result{Error: err}
	}
	data["salary"] = fmt.Sprintf("12000%v", data["user_id"])
	bt, _ := json.Marshal(data)
	return Result{Payload: bt, MessageID: task.ID}
}

func Node4(ctx context.Context, task Task) Result {
	var data []map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return Result{Error: err}
	}
	payload := map[string]any{"storage": data}
	bt, _ := json.Marshal(payload)
	return Result{Payload: bt, MessageID: task.ID}
}

func CheckCondition(ctx context.Context, task Task) Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return Result{Error: err}
	}
	var status string
	if data["user_id"].(float64) == 2 {
		status = "pass"
	} else {
		status = "fail"
	}
	return Result{Status: status, Payload: task.Payload, MessageID: task.ID}
}

func Pass(ctx context.Context, task Task) Result {
	fmt.Println("Pass")
	return Result{Payload: task.Payload}
}

func Fail(ctx context.Context, task Task) Result {
	fmt.Println("Fail")
	return Result{Payload: []byte(`{"test2": "asdsa"}`)}
}

func main() {
	dag := NewDAG()
	dag.AddNode("queue1", Node1, true)
	dag.AddNode("queue2", Node2)
	dag.AddNode("queue3", Node3)
	dag.AddNode("queue4", Node4)
	dag.AddNode("queue5", CheckCondition)
	dag.AddNode("queue6", Pass)
	dag.AddNode("queue7", Fail)
	dag.AddEdge("queue1", "queue2")
	dag.AddEdge("queue2", "queue4")
	dag.AddEdge("queue3", "queue5")
	dag.AddLoop("queue2", "queue3")
	dag.AddCondition("queue5", map[string]string{"pass": "queue6", "fail": "queue7"})
	ctx := context.Background()
	task := Task{
		ID:      "task1",
		Payload: []byte(`[{"user_id": 1}, {"user_id": 2}]`),
	}
	dag.ProcessTask(ctx, "queue1", task)
}
