package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func loadConfiguration(configPath string) (*AppConfiguration, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config AppConfiguration
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse JSON config: %v", err)
	}

	return &config, nil
}

func main() {
	// Parse command line flags
	configPath := flag.String("config", "sms-app.json", "Path to JSON configuration file")
	flag.Parse()

	// If positional args provided, use the first one
	if len(os.Args) > 1 && !flag.Parsed() {
		*configPath = os.Args[1]
	}

	// Load configuration first
	config, err := loadConfiguration(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create JSON engine with configuration
	engine := NewJSONEngine(config)

	// Compile configuration
	if err := engine.Compile(); err != nil {
		log.Fatalf("Failed to compile configuration: %v", err)
	}

	// Start server
	if err := engine.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
