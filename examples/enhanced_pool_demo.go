// This is a demo showing the enhanced worker pool capabilities
// Run with: go run enhanced_pool_demo.go

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/mq"
)

// DemoHandler demonstrates a simple task handler
func DemoHandler(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Printf("Processing task: %s\n", task.ID)

	// Simulate some work
	time.Sleep(100 * time.Millisecond)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

// DemoCallback demonstrates result processing
func DemoCallback(ctx context.Context, result mq.Result) error {
	if result.Error != nil {
		fmt.Printf("Task %s failed: %v\n", result.TaskID, result.Error)
	} else {
		fmt.Printf("Task %s completed successfully\n", result.TaskID)
	}
	return nil
}

func main() {
	fmt.Println("=== Enhanced Worker Pool Demo ===")

	// Create task storage
	storage := mq.NewMemoryTaskStorage(1 * time.Hour)

	// Configure circuit breaker
	circuitBreaker := mq.CircuitBreakerConfig{
		Enabled:          true,
		FailureThreshold: 5,
		ResetTimeout:     30 * time.Second,
	}

	// Create pool with enhanced configuration
	pool := mq.NewPool(5,
		mq.WithHandler(DemoHandler),
		mq.WithPoolCallback(DemoCallback),
		mq.WithTaskStorage(storage),
		mq.WithBatchSize(3),
		mq.WithMaxMemoryLoad(50*1024*1024), // 50MB
		mq.WithCircuitBreaker(circuitBreaker),
	)

	fmt.Printf("Worker pool created with %d workers\n", 5)

	// Enqueue some tasks
	fmt.Println("\n=== Enqueueing Tasks ===")
	for i := 0; i < 10; i++ {
		task := mq.NewTask(
			fmt.Sprintf("demo-task-%d", i),
			[]byte(fmt.Sprintf(`{"message": "Hello from task %d", "timestamp": "%s"}`, i, time.Now().Format(time.RFC3339))),
			"demo",
		)

		// Add some tasks with higher priority
		priority := 1
		if i%3 == 0 {
			priority = 5 // Higher priority
		}

		err := pool.EnqueueTask(context.Background(), task, priority)
		if err != nil {
			log.Printf("Failed to enqueue task %d: %v", i, err)
		} else {
			fmt.Printf("Enqueued task %s with priority %d\n", task.ID, priority)
		}
	}

	// Monitor progress
	fmt.Println("\n=== Monitoring Progress ===")
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)

		metrics := pool.FormattedMetrics()
		health := pool.GetHealthStatus()

		fmt.Printf("Progress: %d/%d completed, %d errors, Queue: %d, Healthy: %v\n",
			metrics.CompletedTasks,
			metrics.TotalTasks,
			metrics.ErrorCount,
			health.QueueDepth,
			health.IsHealthy,
		)

		if metrics.CompletedTasks >= 10 {
			break
		}
	}

	// Display final metrics
	fmt.Println("\n=== Final Metrics ===")
	finalMetrics := pool.FormattedMetrics()
	fmt.Printf("Total Tasks: %d\n", finalMetrics.TotalTasks)
	fmt.Printf("Completed: %d\n", finalMetrics.CompletedTasks)
	fmt.Printf("Errors: %d\n", finalMetrics.ErrorCount)
	fmt.Printf("Memory Used: %s\n", finalMetrics.CurrentMemoryUsed)
	fmt.Printf("Execution Time: %s\n", finalMetrics.CumulativeExecution)
	fmt.Printf("Average Execution: %s\n", finalMetrics.AverageExecution)

	// Test dynamic worker scaling
	fmt.Println("\n=== Dynamic Scaling Demo ===")
	fmt.Printf("Current workers: %d\n", 5)

	pool.AdjustWorkerCount(8)
	fmt.Printf("Scaled up to: %d workers\n", 8)

	time.Sleep(1 * time.Second)

	pool.AdjustWorkerCount(3)
	fmt.Printf("Scaled down to: %d workers\n", 3)

	// Test health status
	fmt.Println("\n=== Health Status ===")
	health := pool.GetHealthStatus()
	fmt.Printf("Health Status: %+v\n", health)

	// Test DLQ (simulate some failures)
	fmt.Println("\n=== Dead Letter Queue Demo ===")
	dlqTasks := pool.DLQ().Tasks()
	fmt.Printf("Tasks in DLQ: %d\n", len(dlqTasks))

	// Test configuration update
	fmt.Println("\n=== Configuration Update Demo ===")
	currentConfig := pool.GetCurrentConfig()
	fmt.Printf("Current batch size: %d\n", currentConfig.BatchSize)

	newConfig := currentConfig
	newConfig.BatchSize = 5
	newConfig.NumberOfWorkers = 4

	err := pool.UpdateConfig(&newConfig)
	if err != nil {
		log.Printf("Failed to update config: %v", err)
	} else {
		fmt.Printf("Updated batch size to: %d\n", newConfig.BatchSize)
		fmt.Printf("Updated worker count to: %d\n", newConfig.NumberOfWorkers)
	}

	// Graceful shutdown
	fmt.Println("\n=== Graceful Shutdown ===")
	fmt.Println("Shutting down pool...")
	pool.Stop()
	fmt.Println("Pool shutdown completed")

	fmt.Println("\n=== Demo Complete ===")
}
