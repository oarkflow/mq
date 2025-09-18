package main

import (
	"log"
	"os"
)

func main() {
	// Check for config file argument
	configPath := "sms-app.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// Create JSON engine
	engine := NewJSONEngine()

	// Load configuration
	if err := engine.LoadConfiguration(configPath); err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Compile configuration
	if err := engine.Compile(); err != nil {
		log.Fatalf("Failed to compile configuration: %v", err)
	}

	// Start server
	if err := engine.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
