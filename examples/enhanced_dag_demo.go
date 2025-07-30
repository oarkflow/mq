package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/logger"
)

// ExampleProcessor demonstrates a custom processor with debugging
type ExampleProcessor struct {
	name string
	tags []string
}

func NewExampleProcessor(name string) *ExampleProcessor {
	return &ExampleProcessor{
		name: name,
		tags: []string{"example", "demo"},
	}
}

func (p *ExampleProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// Add some example processing logic
	var data map[string]interface{}
	if err := task.UnmarshalPayload(&data); err != nil {
		return mq.Result{Error: err}
	}

	// Process the data
	data["processed_by"] = p.name
	data["processed_at"] = time.Now()

	payload, _ := task.MarshalPayload(data)
	return mq.Result{Payload: payload}
}

func (p *ExampleProcessor) SetConfig(payload dag.Payload)     {}
func (p *ExampleProcessor) SetTags(tags ...string)            { p.tags = append(p.tags, tags...) }
func (p *ExampleProcessor) GetTags() []string                 { return p.tags }
func (p *ExampleProcessor) Consume(ctx context.Context) error { return nil }
func (p *ExampleProcessor) Pause(ctx context.Context) error   { return nil }
func (p *ExampleProcessor) Resume(ctx context.Context) error  { return nil }
func (p *ExampleProcessor) Stop(ctx context.Context) error    { return nil }
func (p *ExampleProcessor) Close() error                      { return nil }
func (p *ExampleProcessor) GetType() string                   { return "example" }
func (p *ExampleProcessor) GetKey() string                    { return p.name }
func (p *ExampleProcessor) SetKey(key string)                 { p.name = key }

// CustomActivityHook demonstrates custom activity processing
type CustomActivityHook struct {
	logger logger.Logger
}

func (h *CustomActivityHook) OnActivity(entry dag.ActivityEntry) error {
	// Custom processing of activity entries
	if entry.Level == dag.ActivityLevelError {
		h.logger.Error("Critical activity detected",
			logger.Field{Key: "activity_id", Value: entry.ID},
			logger.Field{Key: "dag_name", Value: entry.DAGName},
			logger.Field{Key: "message", Value: entry.Message},
		)

		// Here you could send notifications, trigger alerts, etc.
	}
	return nil
}

// CustomAlertHandler demonstrates custom alert handling
type CustomAlertHandler struct {
	logger logger.Logger
}

func (h *CustomAlertHandler) HandleAlert(alert dag.Alert) error {
	h.logger.Warn("DAG Alert received",
		logger.Field{Key: "type", Value: alert.Type},
		logger.Field{Key: "severity", Value: alert.Severity},
		logger.Field{Key: "message", Value: alert.Message},
	)

	// Here you could integrate with external alerting systems
	// like Slack, PagerDuty, email, etc.

	return nil
}

func main() {
	// Initialize logger
	log := logger.New(logger.Config{
		Level:  logger.LevelInfo,
		Format: logger.FormatJSON,
	})

	// Create a comprehensive DAG with all enhanced features
	server := mq.NewServer("demo", ":0", log)

	// Create DAG with comprehensive configuration
	dagInstance := dag.NewDAG("production-workflow", "workflow-key", func(ctx context.Context, result mq.Result) {
		log.Info("Workflow completed",
			logger.Field{Key: "result", Value: string(result.Payload)},
		)
	})

	// Initialize all enhanced components
	setupEnhancedDAG(dagInstance, log)

	// Build the workflow
	buildWorkflow(dagInstance, log)

	// Start the server and DAG
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := server.Start(ctx); err != nil {
			log.Error("Server failed to start", logger.Field{Key: "error", Value: err.Error()})
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Start enhanced DAG features
	startEnhancedFeatures(ctx, dagInstance, log)

	// Set up HTTP API for monitoring and management
	setupHTTPAPI(dagInstance, log)

	// Start the HTTP server
	go func() {
		log.Info("Starting HTTP server on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Error("HTTP server failed", logger.Field{Key: "error", Value: err.Error()})
		}
	}()

	// Demonstrate the enhanced features
	demonstrateFeatures(ctx, dagInstance, log)

	// Wait for shutdown signal
	waitForShutdown(ctx, cancel, dagInstance, server, log)
}

func setupEnhancedDAG(dagInstance *dag.DAG, log logger.Logger) {
	// Initialize activity logger with memory persistence
	activityConfig := dag.DefaultActivityLoggerConfig()
	activityConfig.BufferSize = 500
	activityConfig.FlushInterval = 2 * time.Second

	persistence := dag.NewMemoryActivityPersistence()
	dagInstance.InitializeActivityLogger(activityConfig, persistence)

	// Add custom activity hook
	customHook := &CustomActivityHook{logger: log}
	dagInstance.AddActivityHook(customHook)

	// Initialize monitoring with comprehensive configuration
	monitorConfig := dag.MonitoringConfig{
		MetricsInterval:   5 * time.Second,
		EnableHealthCheck: true,
		BufferSize:        1000,
	}

	alertThresholds := &dag.AlertThresholds{
		MaxFailureRate:      0.1, // 10%
		MaxExecutionTime:    30 * time.Second,
		MaxTasksInProgress:  100,
		MinSuccessRate:      0.9, // 90%
		MaxNodeFailures:     5,
		HealthCheckInterval: 10 * time.Second,
	}

	dagInstance.InitializeMonitoring(monitorConfig, alertThresholds)

	// Add custom alert handler
	customAlertHandler := &CustomAlertHandler{logger: log}
	dagInstance.AddAlertHandler(customAlertHandler)

	// Initialize configuration management
	dagInstance.InitializeConfigManager()

	// Set up rate limiting
	dagInstance.InitializeRateLimiter()
	dagInstance.SetRateLimit("validate", 10.0, 5) // 10 req/sec, burst 5
	dagInstance.SetRateLimit("process", 20.0, 10) // 20 req/sec, burst 10
	dagInstance.SetRateLimit("finalize", 5.0, 2)  // 5 req/sec, burst 2

	// Initialize retry management
	retryConfig := &dag.RetryConfig{
		MaxRetries:    3,
		InitialDelay:  1 * time.Second,
		MaxDelay:      10 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        true,
		RetryCondition: func(err error) bool {
			// Custom retry condition - retry on specific errors
			return err != nil && err.Error() != "permanent_failure"
		},
	}
	dagInstance.InitializeRetryManager(retryConfig)

	// Initialize transaction management
	txConfig := dag.TransactionConfig{
		DefaultTimeout:  5 * time.Minute,
		CleanupInterval: 10 * time.Minute,
	}
	dagInstance.InitializeTransactionManager(txConfig)

	// Initialize cleanup management
	cleanupConfig := dag.CleanupConfig{
		Interval:              5 * time.Minute,
		TaskRetentionPeriod:   1 * time.Hour,
		ResultRetentionPeriod: 2 * time.Hour,
		MaxRetainedTasks:      1000,
	}
	dagInstance.InitializeCleanupManager(cleanupConfig)

	// Initialize performance optimizer
	dagInstance.InitializePerformanceOptimizer()

	// Set up webhook manager for external notifications
	httpClient := dag.NewSimpleHTTPClient(30 * time.Second)
	webhookManager := dag.NewWebhookManager(httpClient, log)

	// Add webhook for task completion events
	webhookConfig := dag.WebhookConfig{
		URL:        "https://api.example.com/dag-events", // Replace with actual endpoint
		Headers:    map[string]string{"Authorization": "Bearer your-token"},
		RetryCount: 3,
		Events:     []string{"task_completed", "task_failed", "dag_completed"},
	}
	webhookManager.AddWebhook("task_completed", webhookConfig)
	dagInstance.SetWebhookManager(webhookManager)

	log.Info("Enhanced DAG features initialized successfully")
}

func buildWorkflow(dagInstance *dag.DAG, log logger.Logger) {
	// Create processors for each step
	validator := NewExampleProcessor("validator")
	processor := NewExampleProcessor("processor")
	enricher := NewExampleProcessor("enricher")
	finalizer := NewExampleProcessor("finalizer")

	// Build the workflow with retry configurations
	retryConfig := &dag.RetryConfig{
		MaxRetries:    2,
		InitialDelay:  500 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
	}

	dagInstance.
		AddNodeWithRetry(dag.Function, "Validate Input", "validate", validator, retryConfig, true).
		AddNodeWithRetry(dag.Function, "Process Data", "process", processor, retryConfig).
		AddNodeWithRetry(dag.Function, "Enrich Data", "enrich", enricher, retryConfig).
		AddNodeWithRetry(dag.Function, "Finalize", "finalize", finalizer, retryConfig).
		Connect("validate", "process").
		Connect("process", "enrich").
		Connect("enrich", "finalize")

	// Add conditional connections
	dagInstance.AddCondition("validate", "success", "process")
	dagInstance.AddCondition("validate", "failure", "finalize") // Skip to finalize on validation failure

	// Validate the DAG structure
	if err := dagInstance.ValidateDAG(); err != nil {
		log.Error("DAG validation failed", logger.Field{Key: "error", Value: err.Error()})
		os.Exit(1)
	}

	log.Info("Workflow built and validated successfully")
}

func startEnhancedFeatures(ctx context.Context, dagInstance *dag.DAG, log logger.Logger) {
	// Start monitoring
	dagInstance.StartMonitoring(ctx)

	// Start cleanup manager
	dagInstance.StartCleanup(ctx)

	// Enable batch processing
	dagInstance.SetBatchProcessingEnabled(true)

	log.Info("Enhanced features started")
}

func setupHTTPAPI(dagInstance *dag.DAG, log logger.Logger) {
	// Set up standard DAG handlers
	dagInstance.Handlers(http.DefaultServeMux, "/dag")

	// Set up enhanced API endpoints
	enhancedAPI := dag.NewEnhancedAPIHandler(dagInstance)
	enhancedAPI.RegisterRoutes(http.DefaultServeMux)

	// Custom endpoints for demonstration
	http.HandleFunc("/demo/activities", func(w http.ResponseWriter, r *http.Request) {
		filter := dag.ActivityFilter{
			Limit: 50,
		}

		activities, err := dagInstance.GetActivities(filter)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := dagInstance.GetActivityLogger().(*dag.ActivityLogger).WriteJSON(w, activities); err != nil {
			log.Error("Failed to write activities response", logger.Field{Key: "error", Value: err.Error()})
		}
	})

	http.HandleFunc("/demo/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := dagInstance.GetActivityStats(dag.ActivityFilter{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := dagInstance.GetActivityLogger().(*dag.ActivityLogger).WriteJSON(w, stats); err != nil {
			log.Error("Failed to write stats response", logger.Field{Key: "error", Value: err.Error()})
		}
	})

	log.Info("HTTP API endpoints configured")
}

func demonstrateFeatures(ctx context.Context, dagInstance *dag.DAG, log logger.Logger) {
	log.Info("Demonstrating enhanced DAG features...")

	// 1. Process a successful task
	log.Info("Processing successful task...")
	processTask(ctx, dagInstance, map[string]interface{}{
		"id":   "task-001",
		"data": "valid input data",
		"type": "success",
	}, log)

	// 2. Process a task that will fail
	log.Info("Processing failing task...")
	processTask(ctx, dagInstance, map[string]interface{}{
		"id":   "task-002",
		"data": nil, // This will cause processing issues
		"type": "failure",
	}, log)

	// 3. Process with transaction
	log.Info("Processing with transaction...")
	processWithTransaction(ctx, dagInstance, map[string]interface{}{
		"id":   "task-003",
		"data": "transaction data",
		"type": "transaction",
	}, log)

	// 4. Demonstrate rate limiting
	log.Info("Demonstrating rate limiting...")
	demonstrateRateLimiting(ctx, dagInstance, log)

	// 5. Show monitoring metrics
	time.Sleep(2 * time.Second) // Allow time for metrics to accumulate
	showMetrics(dagInstance, log)

	// 6. Show activity logs
	showActivityLogs(dagInstance, log)
}

func processTask(ctx context.Context, dagInstance *dag.DAG, payload map[string]interface{}, log logger.Logger) {
	// Add context information
	ctx = context.WithValue(ctx, "user_id", "demo-user")
	ctx = context.WithValue(ctx, "session_id", "demo-session")
	ctx = context.WithValue(ctx, "trace_id", mq.NewID())

	result := dagInstance.Process(ctx, payload)
	if result.Error != nil {
		log.Error("Task processing failed",
			logger.Field{Key: "error", Value: result.Error.Error()},
			logger.Field{Key: "payload", Value: payload},
		)
	} else {
		log.Info("Task processed successfully",
			logger.Field{Key: "result_size", Value: len(result.Payload)},
		)
	}
}

func processWithTransaction(ctx context.Context, dagInstance *dag.DAG, payload map[string]interface{}, log logger.Logger) {
	taskID := fmt.Sprintf("tx-%s", mq.NewID())

	// Begin transaction
	tx := dagInstance.BeginTransaction(taskID)
	if tx == nil {
		log.Error("Failed to begin transaction")
		return
	}

	// Add transaction context
	ctx = context.WithValue(ctx, "transaction_id", tx.ID)
	ctx = context.WithValue(ctx, "task_id", taskID)

	// Process the task
	result := dagInstance.Process(ctx, payload)

	// Commit or rollback based on result
	if result.Error != nil {
		if err := dagInstance.RollbackTransaction(tx.ID); err != nil {
			log.Error("Failed to rollback transaction",
				logger.Field{Key: "tx_id", Value: tx.ID},
				logger.Field{Key: "error", Value: err.Error()},
			)
		} else {
			log.Info("Transaction rolled back",
				logger.Field{Key: "tx_id", Value: tx.ID},
			)
		}
	} else {
		if err := dagInstance.CommitTransaction(tx.ID); err != nil {
			log.Error("Failed to commit transaction",
				logger.Field{Key: "tx_id", Value: tx.ID},
				logger.Field{Key: "error", Value: err.Error()},
			)
		} else {
			log.Info("Transaction committed",
				logger.Field{Key: "tx_id", Value: tx.ID},
			)
		}
	}
}

func demonstrateRateLimiting(ctx context.Context, dagInstance *dag.DAG, log logger.Logger) {
	// Try to exceed rate limits
	for i := 0; i < 15; i++ {
		allowed := dagInstance.CheckRateLimit("validate")
		log.Info("Rate limit check",
			logger.Field{Key: "attempt", Value: i + 1},
			logger.Field{Key: "allowed", Value: allowed},
		)

		if allowed {
			processTask(ctx, dagInstance, map[string]interface{}{
				"id":   fmt.Sprintf("rate-test-%d", i),
				"data": "rate limiting test",
			}, log)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func showMetrics(dagInstance *dag.DAG, log logger.Logger) {
	metrics := dagInstance.GetMonitoringMetrics()
	if metrics != nil {
		log.Info("Current DAG Metrics",
			logger.Field{Key: "total_tasks", Value: metrics.TasksTotal},
			logger.Field{Key: "completed_tasks", Value: metrics.TasksCompleted},
			logger.Field{Key: "failed_tasks", Value: metrics.TasksFailed},
			logger.Field{Key: "tasks_in_progress", Value: metrics.TasksInProgress},
			logger.Field{Key: "avg_execution_time", Value: metrics.AverageExecutionTime.String()},
		)

		// Show node-specific metrics
		for nodeID := range map[string]bool{"validate": true, "process": true, "enrich": true, "finalize": true} {
			if nodeStats := dagInstance.GetNodeStats(nodeID); nodeStats != nil {
				log.Info("Node Metrics",
					logger.Field{Key: "node_id", Value: nodeID},
					logger.Field{Key: "executions", Value: nodeStats.TotalExecutions},
					logger.Field{Key: "failures", Value: nodeStats.FailureCount},
					logger.Field{Key: "avg_duration", Value: nodeStats.AverageExecutionTime.String()},
				)
			}
		}
	} else {
		log.Warn("Monitoring metrics not available")
	}
}

func showActivityLogs(dagInstance *dag.DAG, log logger.Logger) {
	// Get recent activities
	filter := dag.ActivityFilter{
		Limit:     10,
		SortBy:    "timestamp",
		SortOrder: "desc",
	}

	activities, err := dagInstance.GetActivities(filter)
	if err != nil {
		log.Error("Failed to get activities", logger.Field{Key: "error", Value: err.Error()})
		return
	}

	log.Info("Recent Activities", logger.Field{Key: "count", Value: len(activities)})
	for _, activity := range activities {
		log.Info("Activity",
			logger.Field{Key: "id", Value: activity.ID},
			logger.Field{Key: "type", Value: string(activity.Type)},
			logger.Field{Key: "level", Value: string(activity.Level)},
			logger.Field{Key: "message", Value: activity.Message},
			logger.Field{Key: "task_id", Value: activity.TaskID},
			logger.Field{Key: "node_id", Value: activity.NodeID},
		)
	}

	// Get activity statistics
	stats, err := dagInstance.GetActivityStats(dag.ActivityFilter{})
	if err != nil {
		log.Error("Failed to get activity stats", logger.Field{Key: "error", Value: err.Error()})
		return
	}

	log.Info("Activity Statistics",
		logger.Field{Key: "total_activities", Value: stats.TotalActivities},
		logger.Field{Key: "success_rate", Value: fmt.Sprintf("%.2f%%", stats.SuccessRate*100)},
		logger.Field{Key: "failure_rate", Value: fmt.Sprintf("%.2f%%", stats.FailureRate*100)},
		logger.Field{Key: "avg_duration", Value: stats.AverageDuration.String()},
	)
}

func waitForShutdown(ctx context.Context, cancel context.CancelFunc, dagInstance *dag.DAG, server *mq.Server, log logger.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Info("DAG system is running. Available endpoints:",
		logger.Field{Key: "workflow", Value: "http://localhost:8080/dag/"},
		logger.Field{Key: "process", Value: "http://localhost:8080/dag/process"},
		logger.Field{Key: "metrics", Value: "http://localhost:8080/api/dag/metrics"},
		logger.Field{Key: "health", Value: "http://localhost:8080/api/dag/health"},
		logger.Field{Key: "activities", Value: "http://localhost:8080/demo/activities"},
		logger.Field{Key: "stats", Value: "http://localhost:8080/demo/stats"},
	)

	<-sigChan
	log.Info("Shutdown signal received, cleaning up...")

	// Graceful shutdown
	cancel()

	// Stop enhanced features
	dagInstance.StopEnhanced(ctx)

	// Stop server
	if err := server.Stop(ctx); err != nil {
		log.Error("Error stopping server", logger.Field{Key: "error", Value: err.Error()})
	}

	log.Info("Shutdown complete")
}
