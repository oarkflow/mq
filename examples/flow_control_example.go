package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

func demonstrateFlowControl() {
	// Create a logger
	l := logger.NewDefaultLogger()

	// Example 1: Using the factory to create different flow controllers
	factory := mq.NewFlowControllerFactory()

	// Create a token bucket flow controller
	tokenBucketFC := factory.CreateTokenBucketFlowController(1000, 10, 100*time.Millisecond, l)
	fmt.Println("Created Token Bucket Flow Controller")
	fmt.Printf("Stats: %+v\n", tokenBucketFC.GetStats())

	// Create a leaky bucket flow controller
	leakyBucketFC := factory.CreateLeakyBucketFlowController(500, 200*time.Millisecond, l)
	fmt.Println("Created Leaky Bucket Flow Controller")
	fmt.Printf("Stats: %+v\n", leakyBucketFC.GetStats())

	// Create a credit-based flow controller
	creditBasedFC := factory.CreateCreditBasedFlowController(1000, 100, 5, 200*time.Millisecond, l)
	fmt.Println("Created Credit-Based Flow Controller")
	fmt.Printf("Stats: %+v\n", creditBasedFC.GetStats())

	// Create a rate limiter flow controller
	rateLimiterFC := factory.CreateRateLimiterFlowController(50, 100, l)
	fmt.Println("Created Rate Limiter Flow Controller")
	fmt.Printf("Stats: %+v\n", rateLimiterFC.GetStats())

	// Example 2: Using configuration providers
	fmt.Println("\n--- Configuration Providers ---")

	// Environment-based configuration
	envProvider := mq.NewEnvConfigProvider("FLOW_")
	envConfig, err := envProvider.GetConfig()
	if err != nil {
		log.Printf("Error loading env config: %v", err)
	} else {
		fmt.Printf("Environment Config: %+v\n", envConfig)
	}

	// Composite configuration (environment overrides defaults)
	compositeProvider := mq.NewCompositeConfigProvider(envProvider)
	compositeConfig, err := compositeProvider.GetConfig()
	if err != nil {
		log.Printf("Error loading composite config: %v", err)
	} else {
		compositeFC, err := factory.CreateFlowController(compositeConfig)
		if err != nil {
			log.Printf("Error creating flow controller: %v", err)
		} else {
			fmt.Printf("Composite Config Flow Controller Stats: %+v\n", compositeFC.GetStats())
		}
	}

	// Example 3: Using the flow controllers
	fmt.Println("\n--- Flow Controller Usage ---")

	ctx := context.Background()

	// Test token bucket
	fmt.Println("Testing Token Bucket...")
	for i := 0; i < 5; i++ {
		if err := tokenBucketFC.AcquireCredit(ctx, 50); err != nil {
			fmt.Printf("Token bucket acquire failed: %v\n", err)
		} else {
			fmt.Printf("Token bucket acquired 50 credits, remaining: %d\n", tokenBucketFC.GetAvailableCredits())
			tokenBucketFC.ReleaseCredit(25) // Release some credits
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Test leaky bucket
	fmt.Println("Testing Leaky Bucket...")
	for i := 0; i < 3; i++ {
		if err := leakyBucketFC.AcquireCredit(ctx, 100); err != nil {
			fmt.Printf("Leaky bucket acquire failed: %v\n", err)
		} else {
			fmt.Printf("Leaky bucket acquired 100 credits, remaining: %d\n", leakyBucketFC.GetAvailableCredits())
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Cleanup
	tokenBucketFC.Shutdown()
	leakyBucketFC.Shutdown()
	creditBasedFC.Shutdown()
	rateLimiterFC.Shutdown()

	fmt.Println("Flow control example completed!")
}
