package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// Comprehensive Consumer Example
// Demonstrates a production-ready consumer with:
// - Worker pool for concurrent processing
// - Error handling and retries
// - Security/authentication
// - Graceful shutdown
// - Statistics tracking

func main() {
	fmt.Println("🚀 Starting Production Message Consumer")
	fmt.Println(strings.Repeat("=", 60))

	// Configure logger
	nullLogger := logger.NewNullLogger()

	// Consumer configuration
	consumerID := "consumer-1"
	brokerAddress := ":9092"

	// Subscribe to multiple queues
	queues := []string{"orders", "payments", "notifications", "analytics", "reports"}

	fmt.Printf("\n📡 Consumer ID: %s\n", consumerID)
	fmt.Printf("📡 Broker Address: %s\n", brokerAddress)
	fmt.Printf("📋 Queues: %v\n", queues)

	// Create consumers for each queue
	var consumers []*mq.Consumer

	for _, queue := range queues {
		consumer := mq.NewConsumer(
			fmt.Sprintf("%s-%s", consumerID, queue), // Consumer ID
			queue,                                   // Queue name (THIS WAS THE BUG!)
			handleTask,                              // Task handler function
			mq.WithBrokerURL(brokerAddress),
			mq.WithWorkerPool(
				100,   // Worker pool size
				4,     // Number of workers
				50000, // Task queue size
			),
			mq.WithLogger(nullLogger),
			// Optional: Enable security
			// mq.WithSecurity(true),
			// mq.WithUsername("consumer"),
			// mq.WithPassword("con123"),
		)
		consumers = append(consumers, consumer)
		fmt.Printf("  ✅ Created consumer for queue: %s\n", queue)
	}

	fmt.Println("\n✅ Consumers created")

	// Start consuming messages
	fmt.Println("\n🔄 Starting message consumption...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start periodic statistics reporting for first consumer with context
	statsCtx, statsCancel := context.WithCancel(context.Background())
	defer statsCancel()
	go reportStatistics(statsCtx, consumers[0])

	// Wait group to track all consumers
	var wg sync.WaitGroup

	// Run all consumers in background
	for _, consumer := range consumers {
		c := consumer // capture for goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Consume(ctx); err != nil {
				log.Printf("❌ Consumer error: %v", err)
			}
		}()
	}

	fmt.Println("✅ All consumers are running")
	fmt.Println("\n⏳ Consuming messages. Press Ctrl+C to shutdown gracefully...")

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	fmt.Println("\n\n🛑 Shutdown signal received...")

	// Stop statistics reporting first
	fmt.Println("  1. Stopping statistics reporting...")
	statsCancel()
	// Give statistics goroutine time to finish its current print cycle
	time.Sleep(100 * time.Millisecond)
	fmt.Println("     ✅ Statistics reporting stopped")

	fmt.Println("  2. Closing consumers (this will stop worker pools)...")
	for i, consumer := range consumers {
		if err := consumer.Close(); err != nil {
			fmt.Printf("❌ Consumer %d close error: %v\n", i, err)
		}
	}
	fmt.Println("     ✅ All consumers closed")

	// Cancel context to stop consumption
	fmt.Println("  3. Cancelling context to stop message processing...")
	cancel()

	// Wait for all Consume() goroutines to finish
	fmt.Println("  4. Waiting for all consumers to finish...")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		fmt.Println("     ✅ All consumers finished")
	case <-time.After(5 * time.Second):
		fmt.Println("     ⚠️  Timeout waiting for consumers to finish")
	}

	fmt.Println("\n✅ Graceful shutdown complete")
	fmt.Println("👋 Consumer stopped")
}

// handleTask processes incoming messages
// This is called by worker pool for each task
func handleTask(ctx context.Context, task *mq.Task) mq.Result {
	startTime := time.Now()

	fmt.Printf("\n📦 Processing Task\n")
	fmt.Printf("  Task ID: %s\n", task.ID)
	fmt.Printf("  Priority: %d\n", task.Priority)

	// Parse task payload
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		fmt.Printf("  ❌ Failed to parse task data: %v\n", err)
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("invalid task data: %w", err),
		}
	}

	// Determine task type
	taskType := "unknown"
	if t, ok := data["type"].(string); ok {
		taskType = t
	}
	fmt.Printf("  Type: %s\n", taskType)

	// Process based on task type
	var err error
	switch taskType {
	case "order":
		err = processOrder(data)
	case "payment":
		err = processPayment(data)
	case "notification":
		err = processNotification(data)
	default:
		err = processGeneric(data)
	}

	// Calculate processing time
	duration := time.Since(startTime)
	fmt.Printf("  ⏱️  Processing time: %v\n", duration)

	// Return result
	if err != nil {
		fmt.Printf("  ❌ Task failed: %v\n", err)

		// Check if error is retryable
		if isRetryableError(err) {
			return mq.Result{
				Status: mq.Failed,
				Error:  err,
			}
		}

		// Permanent failure
		return mq.Result{
			Status: mq.Failed,
			Error:  err,
		}
	}

	fmt.Printf("  ✅ Task completed successfully\n")
	return mq.Result{
		Status: mq.Completed,
	}
}

// processOrder handles order processing tasks
func processOrder(data map[string]interface{}) error {
	fmt.Printf("  📦 Processing order...\n")

	// Extract order details
	orderID := data["order_id"]
	customerID := data["customer_id"]
	amount := data["amount"]

	fmt.Printf("     Order ID: %v\n", orderID)
	fmt.Printf("     Customer ID: %v\n", customerID)
	fmt.Printf("     Amount: $%.2f\n", amount)

	// Simulate order processing
	time.Sleep(500 * time.Millisecond)

	// Simulate occasional transient errors for testing
	if orderID == "ORD-3" {
		return fmt.Errorf("temporary database connection error")
	}

	return nil
}

// processPayment handles payment processing tasks
func processPayment(data map[string]interface{}) error {
	fmt.Printf("  💳 Processing payment...\n")

	paymentID := data["payment_id"]
	orderID := data["order_id"]
	amount := data["amount"]
	method := data["method"]

	fmt.Printf("     Payment ID: %v\n", paymentID)
	fmt.Printf("     Order ID: %v\n", orderID)
	fmt.Printf("     Amount: $%.2f\n", amount)
	fmt.Printf("     Method: %v\n", method)

	// Simulate payment processing
	time.Sleep(1 * time.Second)

	// Validate payment amount
	if amt, ok := amount.(float64); ok && amt < 0 {
		return fmt.Errorf("invalid payment amount: %.2f", amt)
	}

	return nil
}

// processNotification handles notification tasks
func processNotification(data map[string]interface{}) error {
	fmt.Printf("  📧 Processing notification...\n")

	recipient := data["recipient"]
	subject := data["subject"]
	body := data["body"]

	fmt.Printf("     Recipient: %v\n", recipient)
	fmt.Printf("     Subject: %v\n", subject)
	fmt.Printf("     Body length: %d chars\n", len(fmt.Sprint(body)))

	// Simulate sending notification
	time.Sleep(300 * time.Millisecond)

	return nil
}

// processGeneric handles unknown task types
func processGeneric(data map[string]interface{}) error {
	fmt.Printf("  ⚙️  Processing generic task...\n")

	// Just print the data
	for key, value := range data {
		fmt.Printf("     %s: %v\n", key, value)
	}

	time.Sleep(200 * time.Millisecond)
	return nil
}

// isRetryableError determines if an error should trigger a retry
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// List of retryable error patterns
	retryablePatterns := []string{
		"temporary",
		"timeout",
		"connection",
		"network",
		"unavailable",
	}

	for _, pattern := range retryablePatterns {
		if contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// reportStatistics periodically reports consumer statistics
func reportStatistics(ctx context.Context, consumer *mq.Consumer) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, stop reporting
			return
		case <-ticker.C:
			metrics := consumer.Metrics()

			fmt.Println("\n📊 Consumer Statistics:")
			fmt.Println("  " + strings.Repeat("-", 50))
			fmt.Printf("  Consumer ID: %s\n", consumer.GetKey())
			fmt.Printf("  Total Tasks: %d\n", metrics.TotalTasks)
			fmt.Printf("  Completed Tasks: %d\n", metrics.CompletedTasks)
			fmt.Printf("  Failed Tasks: %d\n", metrics.ErrorCount)
			fmt.Printf("  Scheduled Tasks: %d\n", metrics.TotalScheduled)
			fmt.Printf("  Memory Used: %d bytes\n", metrics.TotalMemoryUsed)

			if metrics.TotalTasks > 0 {
				successRate := float64(metrics.CompletedTasks) / float64(metrics.TotalTasks) * 100
				fmt.Printf("  Success Rate: %.1f%%\n", successRate)
			}

			if metrics.TotalTasks > 0 && metrics.ExecutionTime > 0 {
				avgTime := time.Duration(metrics.ExecutionTime/metrics.TotalTasks) * time.Millisecond
				fmt.Printf("  Avg Processing Time: %v\n", avgTime)
			}

			fmt.Println("  " + strings.Repeat("-", 50))
		}
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			findInString(s, substr)))
}

func findInString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
