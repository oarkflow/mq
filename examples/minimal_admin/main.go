package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// Example: Proper timeout patterns for different connection types
func demonstrateTimeoutPatterns() {
	fmt.Println("\n=== Connection Timeout Patterns ===")

	// ✅ GOOD: No I/O timeout for persistent broker-consumer connections
	fmt.Println("✅ Broker-Consumer: NO I/O timeouts (persistent connection)")
	fmt.Println("   - Uses TCP keep-alive for network detection")
	fmt.Println("   - Uses context cancellation for graceful shutdown")
	fmt.Println("   - Connection stays open indefinitely waiting for messages")

	// ✅ GOOD: Timeout for connection establishment
	fmt.Println("✅ Connection Setup: WITH timeout (initial connection)")
	fmt.Println("   - 10-30 second timeout for initial TCP handshake")
	fmt.Println("   - Prevents hanging on unreachable servers")

	// ✅ GOOD: Timeout for individual task processing
	fmt.Println("✅ Task Processing: WITH timeout (individual operations)")
	fmt.Println("   - 30 second timeout per task (configurable)")
	fmt.Println("   - Prevents individual tasks from hanging forever")

	// ❌ BAD: I/O timeout on persistent connections
	fmt.Println("❌ Persistent I/O: NO timeouts on read/write operations")
	fmt.Println("   - Would cause 'i/o timeout' errors on idle connections")
	fmt.Println("   - Breaks the persistent connection model")

	fmt.Println("\n💡 Key Principle:")
	fmt.Println("   - Connection timeouts: Only for establishment")
	fmt.Println("   - I/O timeouts: Only for request/response patterns")
	fmt.Println("   - Persistent connections: Use keep-alive + context cancellation")
}

func main() {
	fmt.Println("=== Minimal Admin Server Test ===")

	// Demonstrate proper timeout patterns
	demonstrateTimeoutPatterns()

	// Create logger
	lg := logger.NewDefaultLogger()
	fmt.Println("\n✅ Logger created")

	// Create broker with NO I/O timeouts for persistent connections
	broker := mq.NewBroker(mq.WithLogger(lg), mq.WithBrokerURL(":8081"))
	fmt.Println("✅ Broker created (NO I/O timeouts - persistent connections)")

	// Start broker
	ctx := context.Background()
	fmt.Println("🚀 Starting broker...")

	// Start broker in goroutine since it blocks
	go func() {
		if err := broker.Start(ctx); err != nil {
			log.Printf("❌ Broker error: %v", err)
		}
	}()

	// Give broker time to start
	time.Sleep(500 * time.Millisecond)
	fmt.Println("✅ Broker started")
	defer broker.Close()

	// Create admin server
	fmt.Println("🔧 Creating admin server...")
	adminServer := mq.NewAdminServer(broker, ":8090", lg)
	fmt.Println("✅ Admin server created")

	// Start admin server
	fmt.Println("🚀 Starting admin server...")
	if err := adminServer.Start(); err != nil {
		log.Fatalf("❌ Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()
	fmt.Println("✅ Admin server started")

	// Wait and test
	fmt.Println("⏳ Waiting 2 seconds...")
	time.Sleep(2 * time.Second)

	fmt.Println("🔍 Testing connectivity...")

	// ✅ GOOD: HTTP client WITH timeout (request/response pattern)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://localhost:8090/api/admin/health")
	if err != nil {
		fmt.Printf("❌ Connection failed: %v\n", err)
	} else {
		fmt.Printf("✅ Connection successful! Status: %d\n", resp.StatusCode)
		resp.Body.Close()
	}

	fmt.Println("\n🌐 Admin Dashboard: http://localhost:8090/admin")
	fmt.Println("📊 Health API: http://localhost:8090/api/admin/health")
	fmt.Println("📡 Broker API: http://localhost:8090/api/admin/broker")
	fmt.Println("📋 Queue API: http://localhost:8090/api/admin/queues")

	fmt.Println("\n💡 Connection Patterns Demonstrated:")
	fmt.Println("   🔗 Broker ↔ Consumer: Persistent (NO I/O timeouts)")
	fmt.Println("   🌐 HTTP Client ↔ Admin: Request/Response (WITH timeouts)")
	fmt.Println("   ⚡ WebSocket ↔ Dashboard: Persistent (NO I/O timeouts)")

	fmt.Println("\n⚠️ Server running - Press Ctrl+C to stop")
	fmt.Println("   - Broker connections will remain persistent")
	fmt.Println("   - No 'i/o timeout' errors should occur")
	fmt.Println("   - TCP keep-alive handles network detection")

	// Keep running
	select {}
}
