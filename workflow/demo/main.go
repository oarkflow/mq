package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"github.com/oarkflow/mq/workflow"
)

func main() {
	fmt.Println("🚀 Starting Complete Workflow Engine Demo...")

	// Create workflow engine with configuration
	config := &workflow.Config{
		MaxWorkers:       10,
		ExecutionTimeout: 30 * time.Minute,
		EnableMetrics:    true,
		EnableAudit:      true,
		EnableTracing:    true,
		LogLevel:         "info",
		Storage: workflow.StorageConfig{
			Type:           "memory",
			MaxConnections: 100,
		},
		Security: workflow.SecurityConfig{
			EnableAuth:     false,
			AllowedOrigins: []string{"*"},
		},
	}

	engine := workflow.NewWorkflowEngine(config)

	// Start the engine
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		log.Fatalf("Failed to start workflow engine: %v", err)
	}
	defer engine.Stop(ctx)

	// Create and register sample workflows
	createSampleWorkflows(ctx, engine)

	// Start HTTP server
	startHTTPServer(engine)
}

func createSampleWorkflows(ctx context.Context, engine *workflow.WorkflowEngine) {
	fmt.Println("📝 Creating sample workflows...")

	// 1. Simple Data Processing Workflow
	dataProcessingWorkflow := &workflow.WorkflowDefinition{
		ID:          "data-processing-workflow",
		Name:        "Data Processing Pipeline",
		Description: "A workflow that processes incoming data through validation, transformation, and storage",
		Version:     "1.0.0",
		Status:      workflow.WorkflowStatusActive,
		Category:    "data-processing",
		Owner:       "demo-user",
		Tags:        []string{"data", "processing", "pipeline"},
		Variables: map[string]workflow.Variable{
			"source_url": {
				Name:         "source_url",
				Type:         "string",
				DefaultValue: "https://api.example.com/data",
				Required:     true,
				Description:  "URL to fetch data from",
			},
			"batch_size": {
				Name:         "batch_size",
				Type:         "integer",
				DefaultValue: 100,
				Required:     false,
				Description:  "Number of records to process in each batch",
			},
		},
		Nodes: []workflow.WorkflowNode{
			{
				ID:          "fetch-data",
				Name:        "Fetch Data",
				Type:        workflow.NodeTypeAPI,
				Description: "Fetch data from external API",
				Config: workflow.NodeConfig{
					URL:    "${source_url}",
					Method: "GET",
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
				},
				Position: workflow.Position{X: 100, Y: 100},
				Timeout:  func() *time.Duration { d := 30 * time.Second; return &d }(),
			},
			{
				ID:          "validate-data",
				Name:        "Validate Data",
				Type:        workflow.NodeTypeTask,
				Description: "Validate the fetched data",
				Config: workflow.NodeConfig{
					Script: "console.log('Validating data:', ${data})",
				},
				Position: workflow.Position{X: 300, Y: 100},
			},
			{
				ID:          "transform-data",
				Name:        "Transform Data",
				Type:        workflow.NodeTypeTransform,
				Description: "Transform data to required format",
				Config: workflow.NodeConfig{
					TransformType: "json_path",
					Expression:    "$.data",
				},
				Position: workflow.Position{X: 500, Y: 100},
			},
			{
				ID:          "check-quality",
				Name:        "Data Quality Check",
				Type:        workflow.NodeTypeDecision,
				Description: "Check if data meets quality standards",
				Config: workflow.NodeConfig{
					Rules: []workflow.Rule{
						{
							Condition: "record_count > 0",
							Output:    "quality_passed",
							NextNode:  "store-data",
						},
						{
							Condition: "record_count == 0",
							Output:    "quality_failed",
							NextNode:  "notify-failure",
						},
					},
				},
				Position: workflow.Position{X: 700, Y: 100},
			},
			{
				ID:          "store-data",
				Name:        "Store Data",
				Type:        workflow.NodeTypeDatabase,
				Description: "Store processed data in database",
				Config: workflow.NodeConfig{
					Query:      "INSERT INTO processed_data (data, created_at) VALUES (?, ?)",
					Connection: "default",
				},
				Position: workflow.Position{X: 900, Y: 50},
			},
			{
				ID:          "notify-failure",
				Name:        "Notify Failure",
				Type:        workflow.NodeTypeEmail,
				Description: "Send notification about data quality failure",
				Config: workflow.NodeConfig{
					To:      []string{"admin@example.com"},
					Subject: "Data Quality Check Failed",
					Body:    "The data processing workflow failed quality checks.",
				},
				Position: workflow.Position{X: 900, Y: 150},
			},
		},
		Edges: []workflow.WorkflowEdge{
			{
				ID:       "fetch-to-validate",
				FromNode: "fetch-data",
				ToNode:   "validate-data",
				Priority: 1,
			},
			{
				ID:       "validate-to-transform",
				FromNode: "validate-data",
				ToNode:   "transform-data",
				Priority: 1,
			},
			{
				ID:       "transform-to-check",
				FromNode: "transform-data",
				ToNode:   "check-quality",
				Priority: 1,
			},
			{
				ID:        "check-to-store",
				FromNode:  "check-quality",
				ToNode:    "store-data",
				Condition: "quality_passed",
				Priority:  1,
			},
			{
				ID:        "check-to-notify",
				FromNode:  "check-quality",
				ToNode:    "notify-failure",
				Condition: "quality_failed",
				Priority:  2,
			},
		},
		Config: workflow.WorkflowConfig{
			Timeout:     func() *time.Duration { d := 10 * time.Minute; return &d }(),
			MaxRetries:  3,
			Priority:    workflow.PriorityMedium,
			Concurrency: 5,
			ErrorHandling: workflow.ErrorHandling{
				OnFailure: "stop",
				MaxErrors: 3,
				Rollback:  false,
			},
		},
	}

	// 2. Approval Workflow
	approvalWorkflow := &workflow.WorkflowDefinition{
		ID:          "approval-workflow",
		Name:        "Document Approval Process",
		Description: "Multi-stage approval workflow for document processing",
		Version:     "1.0.0",
		Status:      workflow.WorkflowStatusActive,
		Category:    "approval",
		Owner:       "demo-user",
		Tags:        []string{"approval", "documents", "review"},
		Nodes: []workflow.WorkflowNode{
			{
				ID:          "initial-review",
				Name:        "Initial Review",
				Type:        workflow.NodeTypeHumanTask,
				Description: "Initial review by team lead",
				Config: workflow.NodeConfig{
					Custom: map[string]interface{}{
						"assignee":    "team-lead",
						"due_date":    "3 days",
						"description": "Please review the document for technical accuracy",
					},
				},
				Position: workflow.Position{X: 100, Y: 100},
			},
			{
				ID:          "check-approval",
				Name:        "Check Approval Status",
				Type:        workflow.NodeTypeDecision,
				Description: "Check if document was approved or rejected",
				Config: workflow.NodeConfig{
					Rules: []workflow.Rule{
						{
							Condition: "status == 'approved'",
							Output:    "approved",
							NextNode:  "manager-review",
						},
						{
							Condition: "status == 'rejected'",
							Output:    "rejected",
							NextNode:  "notify-rejection",
						},
						{
							Condition: "status == 'needs_changes'",
							Output:    "needs_changes",
							NextNode:  "notify-changes",
						},
					},
				},
				Position: workflow.Position{X: 300, Y: 100},
			},
			{
				ID:          "manager-review",
				Name:        "Manager Review",
				Type:        workflow.NodeTypeHumanTask,
				Description: "Final approval by manager",
				Config: workflow.NodeConfig{
					Custom: map[string]interface{}{
						"assignee":    "manager",
						"due_date":    "2 days",
						"description": "Final approval required",
					},
				},
				Position: workflow.Position{X: 500, Y: 50},
			},
			{
				ID:          "final-approval",
				Name:        "Final Approval Check",
				Type:        workflow.NodeTypeDecision,
				Description: "Check final approval status",
				Config: workflow.NodeConfig{
					Rules: []workflow.Rule{
						{
							Condition: "status == 'approved'",
							Output:    "final_approved",
							NextNode:  "publish-document",
						},
						{
							Condition: "status == 'rejected'",
							Output:    "final_rejected",
							NextNode:  "notify-rejection",
						},
					},
				},
				Position: workflow.Position{X: 700, Y: 50},
			},
			{
				ID:          "publish-document",
				Name:        "Publish Document",
				Type:        workflow.NodeTypeTask,
				Description: "Publish approved document",
				Config: workflow.NodeConfig{
					Script: "console.log('Publishing document:', ${document_id})",
				},
				Position: workflow.Position{X: 900, Y: 50},
			},
			{
				ID:          "notify-rejection",
				Name:        "Notify Rejection",
				Type:        workflow.NodeTypeEmail,
				Description: "Send rejection notification",
				Config: workflow.NodeConfig{
					To:      []string{"${author_email}"},
					Subject: "Document Rejected",
					Body:    "Your document has been rejected. Reason: ${rejection_reason}",
				},
				Position: workflow.Position{X: 500, Y: 200},
			},
			{
				ID:          "notify-changes",
				Name:        "Notify Changes Needed",
				Type:        workflow.NodeTypeEmail,
				Description: "Send notification about required changes",
				Config: workflow.NodeConfig{
					To:      []string{"${author_email}"},
					Subject: "Document Changes Required",
					Body:    "Your document needs changes. Details: ${change_details}",
				},
				Position: workflow.Position{X: 300, Y: 200},
			},
		},
		Edges: []workflow.WorkflowEdge{
			{
				ID:       "review-to-check",
				FromNode: "initial-review",
				ToNode:   "check-approval",
				Priority: 1,
			},
			{
				ID:        "check-to-manager",
				FromNode:  "check-approval",
				ToNode:    "manager-review",
				Condition: "approved",
				Priority:  1,
			},
			{
				ID:        "check-to-rejection",
				FromNode:  "check-approval",
				ToNode:    "notify-rejection",
				Condition: "rejected",
				Priority:  2,
			},
			{
				ID:        "check-to-changes",
				FromNode:  "check-approval",
				ToNode:    "notify-changes",
				Condition: "needs_changes",
				Priority:  3,
			},
			{
				ID:       "manager-to-final",
				FromNode: "manager-review",
				ToNode:   "final-approval",
				Priority: 1,
			},
			{
				ID:        "final-to-publish",
				FromNode:  "final-approval",
				ToNode:    "publish-document",
				Condition: "final_approved",
				Priority:  1,
			},
			{
				ID:        "final-to-rejection",
				FromNode:  "final-approval",
				ToNode:    "notify-rejection",
				Condition: "final_rejected",
				Priority:  2,
			},
		},
		Config: workflow.WorkflowConfig{
			Timeout:     func() *time.Duration { d := 7 * 24 * time.Hour; return &d }(), // 7 days
			MaxRetries:  1,
			Priority:    workflow.PriorityHigh,
			Concurrency: 1,
			ErrorHandling: workflow.ErrorHandling{
				OnFailure: "continue",
				MaxErrors: 5,
				Rollback:  false,
			},
		},
	}

	// 3. Complex ETL Workflow
	etlWorkflow := &workflow.WorkflowDefinition{
		ID:          "etl-workflow",
		Name:        "ETL Data Pipeline",
		Description: "Extract, Transform, Load workflow with parallel processing",
		Version:     "1.0.0",
		Status:      workflow.WorkflowStatusActive,
		Category:    "etl",
		Owner:       "data-team",
		Tags:        []string{"etl", "data", "parallel", "batch"},
		Nodes: []workflow.WorkflowNode{
			{
				ID:          "extract-customers",
				Name:        "Extract Customer Data",
				Type:        workflow.NodeTypeDatabase,
				Description: "Extract customer data from source database",
				Config: workflow.NodeConfig{
					Query:      "SELECT * FROM customers WHERE updated_at > ?",
					Connection: "source_db",
				},
				Position: workflow.Position{X: 100, Y: 50},
			},
			{
				ID:          "extract-orders",
				Name:        "Extract Order Data",
				Type:        workflow.NodeTypeDatabase,
				Description: "Extract order data from source database",
				Config: workflow.NodeConfig{
					Query:      "SELECT * FROM orders WHERE created_at > ?",
					Connection: "source_db",
				},
				Position: workflow.Position{X: 100, Y: 150},
			},
			{
				ID:          "transform-customers",
				Name:        "Transform Customer Data",
				Type:        workflow.NodeTypeTransform,
				Description: "Clean and transform customer data",
				Config: workflow.NodeConfig{
					TransformType: "expression",
					Expression:    "standardize_phone(${phone}) AND validate_email(${email})",
				},
				Position: workflow.Position{X: 300, Y: 50},
			},
			{
				ID:          "transform-orders",
				Name:        "Transform Order Data",
				Type:        workflow.NodeTypeTransform,
				Description: "Calculate order metrics and clean data",
				Config: workflow.NodeConfig{
					TransformType: "expression",
					Expression:    "calculate_total(${items}) AND format_date(${order_date})",
				},
				Position: workflow.Position{X: 300, Y: 150},
			},
			{
				ID:          "parallel-validation",
				Name:        "Parallel Data Validation",
				Type:        workflow.NodeTypeParallel,
				Description: "Run validation checks in parallel",
				Config: workflow.NodeConfig{
					Custom: map[string]interface{}{
						"max_parallel": 5,
						"timeout":      "30s",
					},
				},
				Position: workflow.Position{X: 500, Y: 100},
			},
			{
				ID:          "merge-data",
				Name:        "Merge Customer & Order Data",
				Type:        workflow.NodeTypeTask,
				Description: "Join customer and order data",
				Config: workflow.NodeConfig{
					Script: "merge_datasets(${customers}, ${orders})",
				},
				Position: workflow.Position{X: 700, Y: 100},
			},
			{
				ID:          "load-warehouse",
				Name:        "Load to Data Warehouse",
				Type:        workflow.NodeTypeDatabase,
				Description: "Load processed data to warehouse",
				Config: workflow.NodeConfig{
					Query:      "INSERT INTO warehouse.customer_orders SELECT * FROM temp_table",
					Connection: "warehouse_db",
				},
				Position: workflow.Position{X: 900, Y: 100},
			},
			{
				ID:          "send-report",
				Name:        "Send Processing Report",
				Type:        workflow.NodeTypeEmail,
				Description: "Send completion report",
				Config: workflow.NodeConfig{
					To:      []string{"data-team@example.com"},
					Subject: "ETL Pipeline Completed",
					Body:    "ETL pipeline completed successfully. Processed ${record_count} records.",
				},
				Position: workflow.Position{X: 1100, Y: 100},
			},
		},
		Edges: []workflow.WorkflowEdge{
			{
				ID:       "extract-customers-to-transform",
				FromNode: "extract-customers",
				ToNode:   "transform-customers",
				Priority: 1,
			},
			{
				ID:       "extract-orders-to-transform",
				FromNode: "extract-orders",
				ToNode:   "transform-orders",
				Priority: 1,
			},
			{
				ID:       "customers-to-validation",
				FromNode: "transform-customers",
				ToNode:   "parallel-validation",
				Priority: 1,
			},
			{
				ID:       "orders-to-validation",
				FromNode: "transform-orders",
				ToNode:   "parallel-validation",
				Priority: 1,
			},
			{
				ID:       "validation-to-merge",
				FromNode: "parallel-validation",
				ToNode:   "merge-data",
				Priority: 1,
			},
			{
				ID:       "merge-to-load",
				FromNode: "merge-data",
				ToNode:   "load-warehouse",
				Priority: 1,
			},
			{
				ID:       "load-to-report",
				FromNode: "load-warehouse",
				ToNode:   "send-report",
				Priority: 1,
			},
		},
		Config: workflow.WorkflowConfig{
			Timeout:     func() *time.Duration { d := 2 * time.Hour; return &d }(),
			MaxRetries:  2,
			Priority:    workflow.PriorityCritical,
			Concurrency: 10,
			ErrorHandling: workflow.ErrorHandling{
				OnFailure: "retry",
				MaxErrors: 3,
				Rollback:  true,
			},
		},
	}

	// Register all workflows
	workflows := []*workflow.WorkflowDefinition{
		dataProcessingWorkflow,
		approvalWorkflow,
		etlWorkflow,
	}

	for _, wf := range workflows {
		if err := engine.RegisterWorkflow(ctx, wf); err != nil {
			log.Printf("Failed to register workflow %s: %v", wf.Name, err)
		} else {
			fmt.Printf("✅ Registered workflow: %s (ID: %s)\n", wf.Name, wf.ID)
		}
	}

	// Execute sample workflows
	fmt.Println("🏃 Executing sample workflows...")

	// Execute data processing workflow
	dataExecution, err := engine.ExecuteWorkflow(ctx, "data-processing-workflow", map[string]interface{}{
		"source_url":   "https://jsonplaceholder.typicode.com/posts",
		"batch_size":   50,
		"record_count": 100,
	}, &workflow.ExecutionOptions{
		Priority:    workflow.PriorityMedium,
		Owner:       "demo-user",
		TriggeredBy: "demo",
	})
	if err != nil {
		log.Printf("Failed to execute data processing workflow: %v", err)
	} else {
		fmt.Printf("🚀 Started data processing execution: %s\n", dataExecution.ID)
	}

	// Execute approval workflow
	approvalExecution, err := engine.ExecuteWorkflow(ctx, "approval-workflow", map[string]interface{}{
		"document_id":       "DOC-12345",
		"author_email":      "author@example.com",
		"document_title":    "Technical Specification",
		"document_category": "technical",
	}, &workflow.ExecutionOptions{
		Priority:    workflow.PriorityHigh,
		Owner:       "demo-user",
		TriggeredBy: "document-system",
	})
	if err != nil {
		log.Printf("Failed to execute approval workflow: %v", err)
	} else {
		fmt.Printf("🚀 Started approval execution: %s\n", approvalExecution.ID)
	}

	// Execute ETL workflow with delay
	etlExecution, err := engine.ExecuteWorkflow(ctx, "etl-workflow", map[string]interface{}{
		"start_date": "2023-01-01",
		"end_date":   "2023-12-31",
		"table_name": "customer_orders",
	}, &workflow.ExecutionOptions{
		Priority:    workflow.PriorityCritical,
		Owner:       "data-team",
		TriggeredBy: "scheduler",
		Delay:       2 * time.Second, // Start after 2 seconds
	})
	if err != nil {
		log.Printf("Failed to execute ETL workflow: %v", err)
	} else {
		fmt.Printf("🚀 Scheduled ETL execution: %s (starts in 2 seconds)\n", etlExecution.ID)
	}

	// Wait a bit to see some execution progress
	time.Sleep(3 * time.Second)

	// Check execution status
	fmt.Println("📊 Checking execution status...")
	if dataExecution != nil {
		if exec, err := engine.GetExecution(ctx, dataExecution.ID); err == nil {
			fmt.Printf("Data Processing Status: %s\n", exec.Status)
		}
	}
	if approvalExecution != nil {
		if exec, err := engine.GetExecution(ctx, approvalExecution.ID); err == nil {
			fmt.Printf("Approval Workflow Status: %s\n", exec.Status)
		}
	}
	if etlExecution != nil {
		if exec, err := engine.GetExecution(ctx, etlExecution.ID); err == nil {
			fmt.Printf("ETL Workflow Status: %s\n", exec.Status)
		}
	}
}

func startHTTPServer(engine *workflow.WorkflowEngine) {
	fmt.Println("🌐 Starting HTTP server...")

	// Create Fiber app
	app := fiber.New(workflow.CORSConfig())

	// Add middleware
	app.Use(recover.New())
	app.Use(logger.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,HEAD,PUT,DELETE,PATCH,OPTIONS",
		AllowHeaders: "Origin, Content-Type, Accept, Authorization",
	}))

	// Create API handlers
	api := workflow.NewWorkflowAPI(engine)
	api.RegisterRoutes(app)

	// Add demo routes
	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "🚀 Workflow Engine Demo API",
			"version": "1.0.0",
			"endpoints": map[string]string{
				"workflows":       "/api/v1/workflows",
				"executions":      "/api/v1/workflows/executions",
				"health":          "/api/v1/workflows/health",
				"metrics":         "/api/v1/workflows/metrics",
				"demo_workflows":  "/demo/workflows",
				"demo_executions": "/demo/executions",
			},
		})
	})

	// Demo endpoints
	demo := app.Group("/demo")
	demo.Get("/workflows", func(c *fiber.Ctx) error {
		workflows, err := engine.ListWorkflows(c.Context(), &workflow.WorkflowFilter{})
		if err != nil {
			return err
		}
		return c.JSON(fiber.Map{
			"total":     len(workflows),
			"workflows": workflows,
		})
	})

	demo.Get("/executions", func(c *fiber.Ctx) error {
		executions, err := engine.ListExecutions(c.Context(), &workflow.ExecutionFilter{})
		if err != nil {
			return err
		}
		return c.JSON(fiber.Map{
			"total":      len(executions),
			"executions": executions,
		})
	})

	fmt.Println("📱 Demo endpoints available:")
	fmt.Println("   • Main API: http://localhost:3000/")
	fmt.Println("   • Workflows: http://localhost:3000/demo/workflows")
	fmt.Println("   • Executions: http://localhost:3000/demo/executions")
	fmt.Println("   • Health: http://localhost:3000/api/v1/workflows/health")
	fmt.Println("   • Metrics: http://localhost:3000/api/v1/workflows/metrics")
	fmt.Println()
	fmt.Println("🎯 Try these API calls:")
	fmt.Println("   curl http://localhost:3000/demo/workflows")
	fmt.Println("   curl http://localhost:3000/demo/executions")
	fmt.Println("   curl http://localhost:3000/api/v1/workflows/health")
	fmt.Println()

	// Start server
	log.Fatal(app.Listen(":3000"))
}
