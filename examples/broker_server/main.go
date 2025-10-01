package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// Comprehensive Broker Server Example
// This demonstrates a production-ready message broker with all enhanced features enabled

func main() {
	fmt.Println("🚀 Starting Production Message Broker Server")
	fmt.Println("=" + "=" + string(make([]byte, 58)))

	// Configure logger
	nullLogger := logger.NewNullLogger()

	// Configure all enhanced features
	enhancedConfig := &mq.BrokerEnhancedConfig{
		// Dead Letter Queue (DLQ) - Store failed messages
		DLQStoragePath:     "./data/dlq",
		DLQRetentionPeriod: 7 * 24 * time.Hour, // Keep failed messages for 7 days
		DLQMaxSize:         10000,              // Maximum failed messages to store

		// Write-Ahead Log (WAL) - Durability
		WALDirectory:    "./data/wal",
		WALMaxFileSize:  100 * 1024 * 1024, // 100MB per WAL file
		WALSyncInterval: 1 * time.Second,   // Sync to disk every second
		WALFsyncOnWrite: false,             // Async for better performance

		// Message Acknowledgments - At-least-once delivery
		AckTimeout:       30 * time.Second, // Timeout for message acknowledgment
		AckMaxRetries:    3,                // Retry failed messages 3 times
		AckCheckInterval: 5 * time.Second,  // Check for timeouts every 5 seconds

		// Worker Health Monitoring
		WorkerHealthTimeout: 30 * time.Second, // Mark worker unhealthy after 30s
		WorkerCheckInterval: 10 * time.Second, // Check worker health every 10s

		// Dynamic Worker Scaling
		MinWorkers:         2,    // Minimum number of workers per queue
		MaxWorkers:         10,   // Maximum number of workers per queue
		ScaleUpThreshold:   0.75, // Scale up when 75% utilized
		ScaleDownThreshold: 0.25, // Scale down when below 25% utilized

		// Message Deduplication
		DedupWindow:          5 * time.Minute, // Deduplicate within 5-minute window
		DedupCleanupInterval: 1 * time.Minute, // Clean up old entries every minute
		DedupPersistent:      false,           // In-memory only for better performance

		// Flow Control & Backpressure
		MaxCredits:           1000,                   // Maximum flow control credits
		MinCredits:           100,                    // Minimum credits before refill
		CreditRefillRate:     10,                     // Credits to refill
		CreditRefillInterval: 100 * time.Millisecond, // Refill every 100ms

		// Backpressure Thresholds
		QueueDepthThreshold: 1000,                   // Alert when queue has 1000+ messages
		MemoryThreshold:     1 * 1024 * 1024 * 1024, // Alert at 1GB memory usage
		ErrorRateThreshold:  0.5,                    // Alert at 50% error rate

		// Queue Snapshots - Fast recovery
		SnapshotDirectory: "./data/snapshots",
		SnapshotInterval:  5 * time.Minute, // Take snapshot every 5 minutes
		SnapshotRetention: 24 * time.Hour,  // Keep snapshots for 24 hours

		// Distributed Tracing
		TracingEnabled:      true,
		TraceRetention:      24 * time.Hour,   // Keep traces for 24 hours
		TraceExportInterval: 30 * time.Second, // Export traces every 30 seconds

		Logger:             nullLogger,
		EnableEnhancements: true, // Master switch - set to false to disable all enhancements
	}

	// Create broker with standard options
	brokerOptions := []mq.Option{
		mq.WithBrokerURL(":9092"), // Broker listen address
		mq.WithLogger(nullLogger),
	}

	broker := mq.NewBroker(brokerOptions...)

	// Initialize all enhanced features
	fmt.Println("\n📦 Initializing enhanced features...")
	if err := broker.InitializeEnhancements(enhancedConfig); err != nil {
		log.Fatalf("❌ Failed to initialize enhancements: %v", err)
	}
	fmt.Println("✅ Enhanced features initialized")

	// Attempt to recover from previous crash
	fmt.Println("\n🔄 Attempting recovery...")
	ctx := context.Background()

	// Recover from WAL (Write-Ahead Log)
	if err := broker.RecoverFromWAL(ctx); err != nil {
		fmt.Printf("⚠️  WAL recovery: %v (may be first run)\n", err)
	} else {
		fmt.Println("✅ WAL recovery complete")
	}

	// Recover from snapshots
	if err := broker.RecoverFromSnapshot(ctx); err != nil {
		fmt.Printf("⚠️  Snapshot recovery: %v (may be first run)\n", err)
	} else {
		fmt.Println("✅ Snapshot recovery complete")
	}

	// Create queues for different purposes
	fmt.Println("\n📋 Creating queues...")
	queues := []string{
		"orders",        // Order processing
		"payments",      // Payment processing
		"notifications", // Email/SMS notifications
		"analytics",     // Data analytics tasks
		"reports",       // Report generation
	}

	for _, queueName := range queues {
		_ = broker.NewQueue(queueName)
		fmt.Printf("  ✅ Created queue: %s\n", queueName)
	}

	// Start the broker server
	fmt.Println("\n🌐 Starting broker server on :9092...")
	go func() {
		if err := broker.Start(ctx); err != nil {
			log.Fatalf("❌ Failed to start broker: %v", err)
		}
	}()

	// Wait for server to be ready
	time.Sleep(1 * time.Second)
	fmt.Println("✅ Broker server is running")

	// Start periodic stats reporting
	go reportStats(broker)

	// Print usage information
	printUsageInfo()

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("\n⏳ Server running. Press Ctrl+C to shutdown gracefully...")

	<-sigChan

	fmt.Println("\n\n🛑 Shutdown signal received...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Graceful shutdown
	fmt.Println("  1. Shutting down enhanced features...")
	if err := broker.ShutdownEnhanced(shutdownCtx); err != nil {
		fmt.Printf("❌ Enhanced shutdown error: %v\n", err)
	} else {
		fmt.Println("     ✅ Enhanced features shut down")
	}

	fmt.Println("  2. Closing broker...")
	if err := broker.Close(); err != nil {
		fmt.Printf("❌ Broker close error: %v\n", err)
	} else {
		fmt.Println("     ✅ Broker closed")
	}

	fmt.Println("\n✅ Graceful shutdown complete")
	fmt.Println("👋 Broker server stopped")
}

// reportStats periodically reports broker statistics
func reportStats(broker *mq.Broker) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stats := broker.GetEnhancedStats()

		fmt.Println("\n📊 Broker Statistics:")
		fmt.Println("  " + "-" + string(make([]byte, 50)))

		// Acknowledgment stats
		if ackStats, ok := stats["acknowledgments"].(map[string]interface{}); ok {
			fmt.Printf("  📝 Acknowledgments:\n")
			fmt.Printf("     Pending: %v\n", ackStats["pending_count"])
			fmt.Printf("     Redeliver queue: %v\n", ackStats["redeliver_backlog"])
		}

		// WAL stats
		if walStats, ok := stats["wal"].(map[string]interface{}); ok {
			fmt.Printf("  📚 Write-Ahead Log:\n")
			fmt.Printf("     Sequence ID: %v\n", walStats["current_sequence_id"])
			fmt.Printf("     Files: %v\n", walStats["total_files"])
		}

		// Deduplication stats
		if dedupStats, ok := stats["deduplication"].(map[string]interface{}); ok {
			fmt.Printf("  🔍 Deduplication:\n")
			fmt.Printf("     Cache size: %v\n", dedupStats["cache_size"])
			fmt.Printf("     Duplicates blocked: %v\n", dedupStats["total_duplicates"])
		}

		// Flow control stats
		if flowStats, ok := stats["flow_control"].(map[string]interface{}); ok {
			fmt.Printf("  🚦 Flow Control:\n")
			fmt.Printf("     Credits available: %v\n", flowStats["credits"])
			if util, ok := flowStats["utilization"].(float64); ok {
				fmt.Printf("     Utilization: %.1f%%\n", util*100)
			}
		}

		// Snapshot stats
		if snapshotStats, ok := stats["snapshots"].(map[string]interface{}); ok {
			fmt.Printf("  💾 Snapshots:\n")
			fmt.Printf("     Total snapshots: %v\n", snapshotStats["total_snapshots"])
		}

		// Tracing stats
		if traceStats, ok := stats["tracing"].(map[string]interface{}); ok {
			fmt.Printf("  🔬 Tracing:\n")
			fmt.Printf("     Active traces: %v\n", traceStats["active_traces"])
		}

		fmt.Println("  " + "-" + string(make([]byte, 50)))
	}
}

// printUsageInfo prints connection information
func printUsageInfo() {
	fmt.Println("\n" + "=" + "=" + string(make([]byte, 58)))
	fmt.Println("📡 Connection Information")
	fmt.Println("=" + "=" + string(make([]byte, 58)))
	fmt.Println("\nTo connect consumers and publishers to this broker:")
	fmt.Println("\n  Consumer Example:")
	fmt.Println("    consumer := mq.NewConsumer(\"consumer-1\", \":9092\")")
	fmt.Println("    consumer.Subscribe(\"orders\", handler)")
	fmt.Println("\n  Publisher Example:")
	fmt.Println("    publisher := mq.NewPublisher(\"publisher-1\", \":9092\")")
	fmt.Println("    publisher.Publish(task, \"orders\")")
	fmt.Println("\n  Available Queues:")
	fmt.Println("    - orders (order processing)")
	fmt.Println("    - payments (payment processing)")
	fmt.Println("    - notifications (email/sms)")
	fmt.Println("    - analytics (data analytics)")
	fmt.Println("    - reports (report generation)")
	fmt.Println("\n" + "=" + "=" + string(make([]byte, 58)))
}
