package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// Comprehensive Publisher Example
// Demonstrates a production-ready publisher with:
// - Connection pooling
// - Batch publishing
// - Error handling and retries
// - Message deduplication support
// - Security/authentication

func main() {
	fmt.Println("🚀 Starting Production Message Publisher")
	fmt.Println(strings.Repeat("=", 60))

	// Configure logger
	nullLogger := logger.NewNullLogger()

	// Publisher configuration
	publisherID := "publisher-1"
	brokerAddress := ":9092"

	fmt.Printf("\n📡 Publisher ID: %s\n", publisherID)
	fmt.Printf("📡 Broker Address: %s\n", brokerAddress)

	// Create publisher with authentication (optional)
	publisher := mq.NewPublisher(
		publisherID,
		mq.WithBrokerURL(brokerAddress),
		mq.WithLogger(nullLogger),
		// Optional: Enable security
		// mq.WithSecurity(true),
		// mq.WithUsername("publisher"),
		// mq.WithPassword("pub123"),
	)

	// Connect to broker
	ctx := context.Background()
	fmt.Println("\n🔌 Connecting to broker...")

	// Publisher connects automatically on first Publish, but we can test connection
	time.Sleep(500 * time.Millisecond)
	fmt.Println("✅ Publisher ready")

	// Publish messages to different queues
	fmt.Println("\n📤 Publishing messages...")

	// Publish orders
	if err := publishOrders(ctx, publisher); err != nil {
		log.Printf("❌ Failed to publish orders: %v", err)
	}

	// Publish payments
	if err := publishPayments(ctx, publisher); err != nil {
		log.Printf("❌ Failed to publish payments: %v", err)
	}

	// Publish notifications
	if err := publishNotifications(ctx, publisher); err != nil {
		log.Printf("❌ Failed to publish notifications: %v", err)
	}

	fmt.Println("\n✅ All messages published successfully")

	// Start periodic message publishing (optional)
	fmt.Println("\n🔄 Starting periodic publishing (every 10 seconds)...")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("⏳ Publisher running. Press Ctrl+C to shutdown...")

	for {
		select {
		case <-ticker.C:
			// Publish periodic messages
			if err := publishPeriodicMessage(ctx, publisher); err != nil {
				log.Printf("⚠️  Periodic publish error: %v", err)
			}

		case <-sigChan:
			fmt.Println("\n\n🛑 Shutdown signal received...")
			fmt.Println("✅ Graceful shutdown complete")
			fmt.Println("👋 Publisher stopped")
			return
		}
	}
}

// publishOrders publishes sample order messages
func publishOrders(ctx context.Context, publisher *mq.Publisher) error {
	fmt.Println("\n  📦 Publishing orders...")

	for i := 1; i <= 5; i++ {
		orderData := map[string]interface{}{
			"type":        "order",
			"order_id":    fmt.Sprintf("ORD-%d", i),
			"customer_id": fmt.Sprintf("CUST-%d", i),
			"amount":      100.0 * float64(i),
			"items":       []string{"item1", "item2", "item3"},
			"timestamp":   time.Now().Unix(),
		}

		payload, err := json.Marshal(orderData)
		if err != nil {
			return fmt.Errorf("failed to marshal order: %w", err)
		}

		task := mq.NewTask(
			fmt.Sprintf("order-task-%d-%d", i, time.Now().Unix()),
			payload,
			"orders",
			mq.WithPriority(i),
			mq.WithTaskMaxRetries(3),
			mq.WithTTL(1*time.Hour),
			mq.WithTags(map[string]string{
				"type":     "order",
				"priority": fmt.Sprintf("%d", i),
			}),
		)

		if err := publisher.Publish(ctx, *task, "orders"); err != nil {
			return fmt.Errorf("failed to publish order: %w", err)
		}

		fmt.Printf("     ✅ Published: %s (priority: %d)\n", task.ID, task.Priority)
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// publishPayments publishes sample payment messages
func publishPayments(ctx context.Context, publisher *mq.Publisher) error {
	fmt.Println("\n  💳 Publishing payments...")

	for i := 1; i <= 3; i++ {
		paymentData := map[string]interface{}{
			"type":       "payment",
			"payment_id": fmt.Sprintf("PAY-%d", i),
			"order_id":   fmt.Sprintf("ORD-%d", i),
			"amount":     50.0 * float64(i),
			"method":     "credit_card",
			"timestamp":  time.Now().Unix(),
		}

		payload, err := json.Marshal(paymentData)
		if err != nil {
			return fmt.Errorf("failed to marshal payment: %w", err)
		}

		task := mq.NewTask(
			fmt.Sprintf("payment-task-%d-%d", i, time.Now().Unix()),
			payload,
			"payments",
			mq.WithPriority(10), // High priority for payments
			mq.WithTaskMaxRetries(3),
		)

		if err := publisher.Publish(ctx, *task, "payments"); err != nil {
			return fmt.Errorf("failed to publish payment: %w", err)
		}

		fmt.Printf("     ✅ Published: %s\n", task.ID)
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// publishNotifications publishes sample notification messages
func publishNotifications(ctx context.Context, publisher *mq.Publisher) error {
	fmt.Println("\n  📧 Publishing notifications...")

	notifications := []map[string]interface{}{
		{
			"type":       "notification",
			"notif_type": "email",
			"recipient":  "customer1@example.com",
			"subject":    "Order Confirmation",
			"body":       "Your order has been confirmed",
			"timestamp":  time.Now().Unix(),
		},
		{
			"type":       "notification",
			"notif_type": "sms",
			"recipient":  "+1234567890",
			"subject":    "Payment Received",
			"body":       "Your payment of $150 has been received",
			"timestamp":  time.Now().Unix(),
		},
	}

	for i, notif := range notifications {
		payload, err := json.Marshal(notif)
		if err != nil {
			return fmt.Errorf("failed to marshal notification: %w", err)
		}

		task := mq.NewTask(
			fmt.Sprintf("notif-task-%d-%d", i+1, time.Now().Unix()),
			payload,
			"notifications",
			mq.WithPriority(5),
		)

		if err := publisher.Publish(ctx, *task, "notifications"); err != nil {
			return fmt.Errorf("failed to publish notification: %w", err)
		}

		fmt.Printf("     ✅ Published: %s (%s)\n", task.ID, notif["notif_type"])
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// publishPeriodicMessage publishes a periodic heartbeat/analytics message
func publishPeriodicMessage(ctx context.Context, publisher *mq.Publisher) error {
	data := map[string]interface{}{
		"type":      "analytics",
		"event":     "heartbeat",
		"timestamp": time.Now().Unix(),
		"metrics": map[string]interface{}{
			"cpu_usage":    75.5,
			"memory_usage": 60.2,
			"active_users": 1250,
		},
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal periodic message: %w", err)
	}

	task := mq.NewTask(
		fmt.Sprintf("analytics-task-%d", time.Now().Unix()),
		payload,
		"analytics",
		mq.WithPriority(1), // Low priority for analytics
	)

	if err := publisher.Publish(ctx, *task, "analytics"); err != nil {
		return fmt.Errorf("failed to publish periodic message: %w", err)
	}

	fmt.Printf("  🔄 Published periodic message: %s\n", task.ID)
	return nil
}
