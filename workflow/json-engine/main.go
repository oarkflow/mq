package main

import (
	"flag"
	"log"
	"os"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "sms-app.json", "Path to JSON configuration file")
	flag.Parse()

	// If positional args provided, use the first one
	if len(os.Args) > 1 && !flag.Parsed() {
		*configPath = os.Args[1]
	}

	// Create JSON engine
	engine := NewJSONEngine()

	// Load configuration
	if err := engine.LoadConfiguration(*configPath); err != nil {
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
