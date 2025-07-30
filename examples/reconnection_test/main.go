package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/mq"
)

// Simple task handler for testing
func testHandler(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Printf("Processing task: %s\n", task.ID)
	return mq.Result{
		Status:  "SUCCESS",
		Payload: []byte(fmt.Sprintf(`{"message": "Processed task %s"}`, task.ID)),
	}
}

func main() {
	fmt.Println("=== Consumer Reconnection Test ===")
	fmt.Println("This test demonstrates improved reconnection behavior with jitter retry.")
	fmt.Println("Start a broker on :8081 and then stop it to see reconnection attempts.")
	fmt.Println()

	// Create consumer with custom retry configuration
	consumer := mq.NewConsumer(
		"test-consumer",
		"test-queue",
		testHandler,
		mq.WithBrokerURL(":8081"),
		mq.WithMaxRetries(3),               // Limit initial connection retries
		mq.WithInitialDelay(1*time.Second), // Start with 1 second delay
		mq.WithMaxBackoff(30*time.Second),  // Cap at 30 seconds
		mq.WithHTTPApi(true),
		mq.WithWorkerPool(10, 2, 1000),
	)

	// Start consumer in a goroutine so we can observe the behavior
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		fmt.Println("Starting consumer...")
		if err := consumer.Consume(ctx); err != nil {
			log.Printf("Consumer stopped with error: %v", err)
		}
	}()

	// Keep the main thread alive to observe reconnection behavior
	fmt.Println("Consumer started. You can now:")
	fmt.Println("1. Start a broker on :8081")
	fmt.Println("2. Stop the broker to trigger reconnection attempts")
	fmt.Println("3. Restart the broker to see successful reconnection")
	fmt.Println("4. Press Ctrl+C to exit")
	fmt.Println()
	fmt.Println("Watch the logs to see the improved reconnection behavior with:")
	fmt.Println("- Exponential backoff with jitter")
	fmt.Println("- Throttling of rapid reconnection attempts")
	fmt.Println("- Long backoff periods after many failed attempts")
	fmt.Println("- Reset of retry counters on successful connections")

	// Sleep for a long time to observe behavior
	time.Sleep(10 * time.Minute)

	// Graceful shutdown
	fmt.Println("Shutting down consumer...")
	consumer.Close()
	fmt.Println("Test completed.")
}
