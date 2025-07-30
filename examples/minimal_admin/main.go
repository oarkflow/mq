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

func main() {
	fmt.Println("=== Minimal Admin Server Test ===")

	// Create logger
	lg := logger.NewDefaultLogger()
	fmt.Println("✅ Logger created")

	// Create broker
	broker := mq.NewBroker(mq.WithLogger(lg))
	fmt.Println("✅ Broker created")

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
	fmt.Println("\n⚠️ Server running - Press Ctrl+C to stop")

	// Keep running
	select {}
}
