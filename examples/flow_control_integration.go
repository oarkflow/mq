package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

func main() {
	testFlowControlIntegration()
}

func testFlowControlIntegration() {
	// Create a logger
	l := logger.NewDefaultLogger()

	fmt.Println("Testing Flow Control Factory and Configuration Providers...")

	// Test 1: Factory creates different strategies
	factory := mq.NewFlowControllerFactory()

	strategies := []struct {
		name       string
		createFunc func() *mq.FlowController
	}{
		{"Token Bucket", func() *mq.FlowController {
			return factory.CreateTokenBucketFlowController(100, 10, 100*time.Millisecond, l)
		}},
		{"Leaky Bucket", func() *mq.FlowController {
			return factory.CreateLeakyBucketFlowController(50, 200*time.Millisecond, l)
		}},
		{"Credit Based", func() *mq.FlowController {
			return factory.CreateCreditBasedFlowController(200, 20, 10, 150*time.Millisecond, l)
		}},
		{"Rate Limiter", func() *mq.FlowController {
			return factory.CreateRateLimiterFlowController(50, 100, l)
		}},
	}

	for _, s := range strategies {
		fmt.Printf("\n--- Testing %s ---\n", s.name)
		fc := s.createFunc()
		if fc == nil {
			fmt.Printf("✗ Failed to create %s flow controller\n", s.name)
			continue
		}

		fmt.Printf("✓ Created %s flow controller\n", s.name)
		stats := fc.GetStats()
		fmt.Printf("  Initial stats: %+v\n", stats)

		// Test acquiring credits
		ctx := context.Background()
		err := fc.AcquireCredit(ctx, 5)
		if err != nil {
			fmt.Printf("  ✗ Failed to acquire credits: %v\n", err)
		} else {
			fmt.Printf("  ✓ Successfully acquired 5 credits\n")
			stats = fc.GetStats()
			fmt.Printf("  Stats after acquire: %+v\n", stats)

			// Release credits
			fc.ReleaseCredit(3)
			fmt.Printf("  ✓ Released 3 credits\n")
			stats = fc.GetStats()
			fmt.Printf("  Stats after release: %+v\n", stats)
		}

		fc.Shutdown()
	}

	// Test 2: Configuration providers
	fmt.Println("\n--- Testing Configuration Providers ---")

	// Test environment provider (will likely fail since no env vars set)
	envProvider := mq.NewEnvConfigProvider("TEST_FLOW_")
	envConfig, err := envProvider.GetConfig()
	if err != nil {
		fmt.Printf("✓ Environment config correctly failed (no env vars): %v\n", err)
	} else {
		fmt.Printf("Environment config: %+v\n", envConfig)
	}

	// Test composite provider
	compositeProvider := mq.NewCompositeConfigProvider(envProvider)
	compositeConfig, err := compositeProvider.GetConfig()
	if err != nil {
		fmt.Printf("✓ Composite config correctly failed: %v\n", err)
	} else {
		fmt.Printf("Composite config: %+v\n", compositeConfig)
	}

	// Test 3: Factory with config
	fmt.Println("\n--- Testing Factory with Config ---")
	config := mq.FlowControlConfig{
		Strategy:       mq.StrategyTokenBucket,
		MaxCredits:     100,
		RefillRate:     10,
		RefillInterval: 100 * time.Millisecond,
		Logger:         l,
	}

	fc, err := factory.CreateFlowController(config)
	if err != nil {
		log.Printf("Failed to create flow controller with config: %v", err)
	} else {
		fmt.Printf("✓ Created flow controller with config\n")
		stats := fc.GetStats()
		fmt.Printf("  Config-based stats: %+v\n", stats)
		fc.Shutdown()
	}

	fmt.Println("\nFlow control integration test completed successfully!")
}
