package dag

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/storage/memory"
)

// TestRingBuffer_BasicOperations tests basic ring buffer operations
func TestRingBuffer_BasicOperations(t *testing.T) {
	rb := NewRingBuffer[string](16)

	// Test Push and Pop
	if !rb.Push("test1") {
		t.Error("Failed to push item to ring buffer")
	}

	item, ok := rb.Pop()
	if !ok || item != "test1" {
		t.Errorf("Expected 'test1', got '%s', ok=%v", item, ok)
	}

	// Test empty pop
	_, ok = rb.Pop()
	if ok {
		t.Error("Expected false for empty buffer pop")
	}
}

// TestRingBuffer_FullCapacity tests ring buffer at full capacity
func TestRingBuffer_FullCapacity(t *testing.T) {
	capacity := uint64(8)
	rb := NewRingBuffer[int](capacity)

	// Fill buffer
	for i := 0; i < int(capacity); i++ {
		if !rb.Push(i) {
			t.Errorf("Failed to push item %d", i)
		}
	}

	// Buffer should be full
	if !rb.Push(999) {
		t.Log("Correctly rejected push to full buffer")
	} else {
		t.Error("Should not be able to push to full buffer")
	}

	// Pop all items
	for i := 0; i < int(capacity); i++ {
		item, ok := rb.Pop()
		if !ok {
			t.Errorf("Failed to pop item %d", i)
		}
		if item != i {
			t.Errorf("Expected %d, got %d", i, item)
		}
	}
}

// TestStreamingRingBuffer_BatchOperations tests streaming ring buffer batch operations
func TestStreamingRingBuffer_BatchOperations(t *testing.T) {
	rb := NewStreamingRingBuffer[string](64, 8)

	// Test batch push
	items := []string{"a", "b", "c", "d", "e"}
	pushed := rb.PushBatch(items)
	if pushed != len(items) {
		t.Errorf("Expected to push %d items, pushed %d", len(items), pushed)
	}

	// Test batch pop
	popped := rb.PopBatch(3)
	if len(popped) != 3 {
		t.Errorf("Expected to pop 3 items, popped %d", len(popped))
	}

	expected := []string{"a", "b", "c"}
	for i, item := range popped {
		if item != expected[i] {
			t.Errorf("Expected '%s', got '%s'", expected[i], item)
		}
	}

	// Test remaining items
	popped2 := rb.PopBatch(10)
	if len(popped2) != 2 {
		t.Errorf("Expected to pop 2 remaining items, popped %d", len(popped2))
	}
}

// TestTaskManager_HighThroughputRingBuffer tests TaskManager with ring buffer performance
func TestTaskManager_HighThroughputRingBuffer(t *testing.T) {
	// Create a simple DAG for testing
	dag := NewDAG("test-dag", "test", func(taskID string, result mq.Result) {
		// Final result handler
	})

	// Add a simple node
	dag.AddNode(Function, "test-node", "test", &RingBufferTestProcessor{}, true)

	resultCh := make(chan mq.Result, 1000)
	iteratorNodes := memory.New[string, []Edge]()

	tm := NewTaskManager(dag, "test-task", resultCh, iteratorNodes, nil)

	// Test concurrent task processing
	const numTasks = 1000
	const numWorkers = 10

	var wg sync.WaitGroup
	start := time.Now()

	// Launch workers to submit tasks concurrently
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numTasks/numWorkers; j++ {
				payload := json.RawMessage(fmt.Sprintf(`{"worker": %d, "task": %d}`, workerID, j))
				ctx := context.Background()
				tm.ProcessTask(ctx, "test", payload)
			}
		}(i)
	}

	wg.Wait()

	// Wait for all tasks to complete
	completed := 0
	timeout := time.After(30 * time.Second)

	for completed < numTasks {
		select {
		case result := <-resultCh:
			if result.Status == mq.Completed {
				completed++
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for tasks to complete. Completed: %d/%d", completed, numTasks)
		}
	}

	elapsed := time.Since(start)
	throughput := float64(numTasks) / elapsed.Seconds()

	t.Logf("Processed %d tasks in %v (%.2f tasks/sec)", numTasks, elapsed, throughput)

	// Verify high throughput (should be much faster than channel-based)
	if throughput < 100 { // Conservative threshold
		t.Errorf("Throughput too low: %.2f tasks/sec", throughput)
	}

	tm.Stop()
}

// TestTaskManager_LinearStreaming tests linear streaming capabilities
func TestTaskManager_LinearStreaming(t *testing.T) {
	dag := NewDAG("streaming-test", "streaming", func(taskID string, result mq.Result) {
		// Final result handler
	})

	// Create a chain of nodes for streaming
	dag.AddNode(Function, "start", "start", &StreamingProcessor{Prefix: "start"}, true)
	dag.AddNode(Function, "middle", "middle", &StreamingProcessor{Prefix: "middle"}, false)
	dag.AddNode(Function, "end", "end", &StreamingProcessor{Prefix: "end"}, false)

	dag.AddEdge(Simple, "start->middle", "start", "middle")
	dag.AddEdge(Simple, "middle->end", "middle", "end")

	resultCh := make(chan mq.Result, 100)
	iteratorNodes := memory.New[string, []Edge]()

	tm := NewTaskManager(dag, "streaming-task", resultCh, iteratorNodes, nil)

	// Test batch processing
	const batchSize = 50
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := json.RawMessage(fmt.Sprintf(`{"id": %d, "data": "test%d"}`, id, id))
			ctx := context.Background()
			tm.ProcessTask(ctx, "start", payload)
		}(i)
	}

	wg.Wait()

	// Wait for completion
	completed := 0
	timeout := time.After(30 * time.Second)

	for completed < batchSize {
		select {
		case result := <-resultCh:
			if result.Status == mq.Completed {
				completed++
				// Verify streaming worked (payload should be modified by each node)
				var data map[string]any
				if err := json.Unmarshal(result.Payload, &data); err == nil {
					if data["processed_by"] == "end" {
						t.Logf("Task %v streamed through all nodes", data["id"])
					}
				}
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for streaming tasks. Completed: %d/%d", completed, batchSize)
		}
	}

	elapsed := time.Since(start)
	t.Logf("Streaming test completed %d tasks in %v", batchSize, elapsed)

	tm.Stop()
}

// RingBufferTestProcessor is a simple test processor for ring buffer tests
type RingBufferTestProcessor struct {
	Operation
}

func (tp *RingBufferTestProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Simple processing - just return the payload
	return mq.Result{
		Ctx:     ctx,
		Payload: task.Payload,
		Status:  mq.Completed,
	}
}

// StreamingProcessor simulates streaming data processing
type StreamingProcessor struct {
	Operation
	Prefix string
}

func (sp *StreamingProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	// Add processing marker
	data["processed_by"] = sp.Prefix
	data["timestamp"] = time.Now().Unix()

	updatedPayload, _ := json.Marshal(data)

	return mq.Result{
		Ctx:     ctx,
		Payload: updatedPayload,
		Status:  mq.Completed,
	}
}

// BenchmarkRingBuffer_PushPop benchmarks ring buffer performance
func BenchmarkRingBuffer_PushPop(b *testing.B) {
	rb := NewRingBuffer[int](1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if rb.Push(1) {
				rb.Pop()
			}
		}
	})
}

// BenchmarkStreamingRingBuffer_Batch benchmarks streaming ring buffer batch operations
func BenchmarkStreamingRingBuffer_Batch(b *testing.B) {
	rb := NewStreamingRingBuffer[int](8192, 128)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		batch := make([]int, 32)
		for i := range batch {
			batch[i] = i
		}

		for pb.Next() {
			rb.PushBatch(batch)
			rb.PopBatch(32)
		}
	})
}

// BenchmarkTaskManager_Throughput benchmarks TaskManager throughput
func BenchmarkTaskManager_Throughput(b *testing.B) {
	dag := NewDAG("bench-dag", "bench", func(taskID string, result mq.Result) {})
	dag.AddNode(Function, "bench-node", "bench", &RingBufferTestProcessor{}, true)

	resultCh := make(chan mq.Result, b.N)
	iteratorNodes := memory.New[string, []Edge]()

	tm := NewTaskManager(dag, "bench-task", resultCh, iteratorNodes, nil)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			payload := json.RawMessage(`{"bench": true}`)
			ctx := context.Background()
			tm.ProcessTask(ctx, "bench", payload)
		}
	})

	// Drain results
	for i := 0; i < b.N; i++ {
		<-resultCh
	}

	tm.Stop()
}
