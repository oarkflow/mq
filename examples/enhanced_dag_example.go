package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/mq/dag"
)

// Enhanced DAG Example demonstrates how to use the enhanced DAG system with workflow capabilities
func mai1n() {
	fmt.Println("🚀 Starting Enhanced DAG with Workflow Engine Demo...")

	// Create enhanced DAG configuration
	config := &dag.EnhancedDAGConfig{
		EnableWorkflowEngine:    true,
		MaintainDAGMode:         true,
		AutoMigrateWorkflows:    true,
		EnablePersistence:       true,
		EnableStateManagement:   true,
		EnableAdvancedRetry:     true,
		EnableCircuitBreaker:    true,
		MaxConcurrentExecutions: 100,
		DefaultTimeout:          time.Minute * 30,
		EnableMetrics:           true,
	}

	// Create workflow engine adapter
	adapterConfig := &dag.WorkflowEngineAdapterConfig{
		UseExternalEngine:   false, // Use built-in engine for this example
		EnablePersistence:   true,
		PersistenceType:     "memory",
		EnableStateRecovery: true,
		MaxExecutions:       1000,
	}

	workflowEngine := dag.NewWorkflowEngineAdapter(adapterConfig)
	config.WorkflowEngine = workflowEngine

	// Create enhanced DAG
	enhancedDAG, err := dag.NewEnhancedDAG("workflow-example", "workflow-key", config)
	if err != nil {
		log.Fatalf("Failed to create enhanced DAG: %v", err)
	}

	// Start the enhanced DAG system
	ctx := context.Background()
	if err := enhancedDAG.Start(ctx, ":8080"); err != nil {
		log.Fatalf("Failed to start enhanced DAG: %v", err)
	}

	// Create example workflows
	if err := createExampleWorkflows(ctx, enhancedDAG); err != nil {
		log.Fatalf("Failed to create example workflows: %v", err)
	}

	// Setup Fiber app with workflow API
	app := fiber.New()

	// Register workflow API routes
	workflowAPI := dag.NewWorkflowAPI(enhancedDAG)
	workflowAPI.RegisterWorkflowRoutes(app)

	// Add some basic routes for demonstration
	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "Enhanced DAG with Workflow Engine",
			"version": "1.0.0",
			"features": []string{
				"Workflow Engine Integration",
				"State Management",
				"Persistence",
				"Advanced Retry",
				"Circuit Breaker",
				"Metrics",
			},
		})
	})

	// Demonstrate workflow execution
	go demonstrateWorkflowExecution(ctx, enhancedDAG)

	// Start the HTTP server
	log.Println("Starting server on :3000")
	log.Fatal(app.Listen(":3000"))
}

// createExampleWorkflows creates example workflows to demonstrate capabilities
func createExampleWorkflows(ctx context.Context, enhancedDAG *dag.EnhancedDAG) error {
	// Example 1: Simple Data Processing Workflow
	dataProcessingWorkflow := &dag.WorkflowDefinition{
		ID:          "data-processing-workflow",
		Name:        "Data Processing Pipeline",
		Description: "A workflow that processes data through multiple stages",
		Version:     "1.0.0",
		Status:      dag.WorkflowStatusActive,
		Tags:        []string{"data", "processing", "example"},
		Category:    "data-processing",
		Owner:       "system",
		Nodes: []dag.WorkflowNode{
			{
				ID:          "validate-input",
				Name:        "Validate Input",
				Type:        dag.WorkflowNodeTypeValidator,
				Description: "Validates incoming data",
				Position:    dag.Position{X: 100, Y: 100},
				Config: dag.WorkflowNodeConfig{
					Custom: map[string]any{
						"validation_type": "json",
						"required_fields": []string{"data"},
					},
				},
			},
			{
				ID:          "transform-data",
				Name:        "Transform Data",
				Type:        dag.WorkflowNodeTypeTransform,
				Description: "Transforms and enriches data",
				Position:    dag.Position{X: 300, Y: 100},
				Config: dag.WorkflowNodeConfig{
					TransformType: "json",
					Expression:    "$.data | {processed: true, timestamp: now()}",
				},
			},
			{
				ID:          "store-data",
				Name:        "Store Data",
				Type:        dag.WorkflowNodeTypeStorage,
				Description: "Stores processed data",
				Position:    dag.Position{X: 500, Y: 100},
				Config: dag.WorkflowNodeConfig{
					Custom: map[string]any{
						"storage_type":      "memory",
						"storage_operation": "save",
						"storage_key":       "processed_data",
					},
				},
			},
			{
				ID:          "notify-completion",
				Name:        "Notify Completion",
				Type:        dag.WorkflowNodeTypeNotify,
				Description: "Sends completion notification",
				Position:    dag.Position{X: 700, Y: 100},
				Config: dag.WorkflowNodeConfig{
					Custom: map[string]any{
						"notify_type":             "email",
						"notification_recipients": []string{"admin@example.com"},
						"notification_message":    "Data processing completed",
					},
				},
			},
		},
		Edges: []dag.WorkflowEdge{
			{
				ID:       "edge_1",
				FromNode: "validate-input",
				ToNode:   "transform-data",
				Label:    "Valid Data",
				Priority: 1,
			},
			{
				ID:       "edge_2",
				FromNode: "transform-data",
				ToNode:   "store-data",
				Label:    "Transformed",
				Priority: 1,
			},
			{
				ID:       "edge_3",
				FromNode: "store-data",
				ToNode:   "notify-completion",
				Label:    "Stored",
				Priority: 1,
			},
		},
		Variables: map[string]dag.Variable{
			"input_data": {
				Name:        "input_data",
				Type:        "object",
				Required:    true,
				Description: "Input data to process",
			},
		},
		Config: dag.WorkflowConfig{
			Timeout:       &[]time.Duration{time.Minute * 10}[0],
			MaxRetries:    3,
			Priority:      dag.PriorityMedium,
			Concurrency:   1,
			EnableAudit:   true,
			EnableMetrics: true,
		},
		Metadata: map[string]any{
			"example": true,
			"type":    "data-processing",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		CreatedBy: "example-system",
		UpdatedBy: "example-system",
	}

	if err := enhancedDAG.RegisterWorkflow(ctx, dataProcessingWorkflow); err != nil {
		return fmt.Errorf("failed to register data processing workflow: %w", err)
	}

	// Example 2: API Integration Workflow
	apiWorkflow := &dag.WorkflowDefinition{
		ID:          "api-integration-workflow",
		Name:        "API Integration Pipeline",
		Description: "A workflow that integrates with external APIs",
		Version:     "1.0.0",
		Status:      dag.WorkflowStatusActive,
		Tags:        []string{"api", "integration", "example"},
		Category:    "integration",
		Owner:       "system",
		Nodes: []dag.WorkflowNode{
			{
				ID:          "fetch-data",
				Name:        "Fetch External Data",
				Type:        dag.WorkflowNodeTypeAPI,
				Description: "Fetches data from external API",
				Position:    dag.Position{X: 100, Y: 100},
				Config: dag.WorkflowNodeConfig{
					URL:    "https://api.example.com/data",
					Method: "GET",
					Headers: map[string]string{
						"Authorization": "Bearer token",
						"Content-Type":  "application/json",
					},
				},
			},
			{
				ID:          "process-response",
				Name:        "Process API Response",
				Type:        dag.WorkflowNodeTypeTransform,
				Description: "Processes API response data",
				Position:    dag.Position{X: 300, Y: 100},
				Config: dag.WorkflowNodeConfig{
					TransformType: "json",
					Expression:    "$.response | {id: .id, name: .name, processed_at: now()}",
				},
			},
			{
				ID:          "decision-point",
				Name:        "Check Data Quality",
				Type:        dag.WorkflowNodeTypeDecision,
				Description: "Decides based on data quality",
				Position:    dag.Position{X: 500, Y: 100},
				Config: dag.WorkflowNodeConfig{
					Condition: "$.data.quality > 0.8",
					DecisionRules: []dag.WorkflowDecisionRule{
						{Condition: "quality > 0.8", NextNode: "send-success-email"},
						{Condition: "quality <= 0.8", NextNode: "send-alert-email"},
					},
				},
			},
			{
				ID:          "send-success-email",
				Name:        "Send Success Email",
				Type:        dag.WorkflowNodeTypeEmail,
				Description: "Sends success notification",
				Position:    dag.Position{X: 700, Y: 50},
				Config: dag.WorkflowNodeConfig{
					EmailTo: []string{"success@example.com"},
					Subject: "API Integration Success",
					Body:    "Data integration completed successfully",
				},
			},
			{
				ID:          "send-alert-email",
				Name:        "Send Alert Email",
				Type:        dag.WorkflowNodeTypeEmail,
				Description: "Sends alert notification",
				Position:    dag.Position{X: 700, Y: 150},
				Config: dag.WorkflowNodeConfig{
					EmailTo: []string{"alert@example.com"},
					Subject: "API Integration Alert",
					Body:    "Data quality below threshold",
				},
			},
		},
		Edges: []dag.WorkflowEdge{
			{
				ID:       "edge_1",
				FromNode: "fetch-data",
				ToNode:   "process-response",
				Label:    "Data Fetched",
				Priority: 1,
			},
			{
				ID:       "edge_2",
				FromNode: "process-response",
				ToNode:   "decision-point",
				Label:    "Processed",
				Priority: 1,
			},
			{
				ID:        "edge_3",
				FromNode:  "decision-point",
				ToNode:    "send-success-email",
				Label:     "High Quality",
				Condition: "quality > 0.8",
				Priority:  1,
			},
			{
				ID:        "edge_4",
				FromNode:  "decision-point",
				ToNode:    "send-alert-email",
				Label:     "Low Quality",
				Condition: "quality <= 0.8",
				Priority:  2,
			},
		},
		Variables: map[string]dag.Variable{
			"api_endpoint": {
				Name:         "api_endpoint",
				Type:         "string",
				DefaultValue: "https://api.example.com/data",
				Required:     true,
				Description:  "API endpoint to fetch data from",
			},
		},
		Config: dag.WorkflowConfig{
			Timeout:       &[]time.Duration{time.Minute * 5}[0],
			MaxRetries:    2,
			Priority:      dag.PriorityHigh,
			Concurrency:   1,
			EnableAudit:   true,
			EnableMetrics: true,
		},
		Metadata: map[string]any{
			"example": true,
			"type":    "api-integration",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		CreatedBy: "example-system",
		UpdatedBy: "example-system",
	}

	if err := enhancedDAG.RegisterWorkflow(ctx, apiWorkflow); err != nil {
		return fmt.Errorf("failed to register API workflow: %w", err)
	}

	log.Println("Example workflows created successfully")
	return nil
}

// demonstrateWorkflowExecution shows how to execute workflows programmatically
func demonstrateWorkflowExecution(ctx context.Context, enhancedDAG *dag.EnhancedDAG) {
	// Wait a bit for system to initialize
	time.Sleep(time.Second * 2)

	log.Println("Starting workflow execution demonstration...")

	// Execute the data processing workflow
	input1 := map[string]any{
		"data": map[string]any{
			"id":    "12345",
			"name":  "Sample Data",
			"value": 100,
			"type":  "example",
		},
		"metadata": map[string]any{
			"source": "demo",
		},
	}

	execution1, err := enhancedDAG.ExecuteWorkflow(ctx, "data-processing-workflow", input1)
	if err != nil {
		log.Printf("Failed to execute data processing workflow: %v", err)
		return
	}

	log.Printf("Started data processing workflow execution: %s", execution1.ID)

	// Execute the API integration workflow
	input2 := map[string]any{
		"api_endpoint": "https://jsonplaceholder.typicode.com/posts/1",
		"timeout":      30,
	}

	execution2, err := enhancedDAG.ExecuteWorkflow(ctx, "api-integration-workflow", input2)
	if err != nil {
		log.Printf("Failed to execute API integration workflow: %v", err)
		return
	}

	log.Printf("Started API integration workflow execution: %s", execution2.ID)

	// Monitor executions
	go monitorExecutions(ctx, enhancedDAG, []string{execution1.ID, execution2.ID})
}

// monitorExecutions monitors the progress of workflow executions
func monitorExecutions(ctx context.Context, enhancedDAG *dag.EnhancedDAG, executionIDs []string) {
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()

	completed := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			allCompleted := true

			for _, execID := range executionIDs {
				if completed[execID] {
					continue
				}

				execution, err := enhancedDAG.GetExecution(execID)
				if err != nil {
					log.Printf("Failed to get execution %s: %v", execID, err)
					continue
				}

				log.Printf("Execution %s status: %s", execID, execution.Status)

				if execution.Status == dag.ExecutionStatusCompleted ||
					execution.Status == dag.ExecutionStatusFailed ||
					execution.Status == dag.ExecutionStatusCancelled {
					completed[execID] = true
					log.Printf("Execution %s completed with status: %s", execID, execution.Status)
					if execution.EndTime != nil {
						duration := execution.EndTime.Sub(execution.StartTime)
						log.Printf("Execution %s took: %v", execID, duration)
					}
				} else {
					allCompleted = false
				}
			}

			if allCompleted {
				log.Println("All executions completed!")
				return
			}
		}
	}
}
