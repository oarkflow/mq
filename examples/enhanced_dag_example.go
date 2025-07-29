package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// ExampleProcessor implements a simple processor
type ExampleProcessor struct {
	name string
}

func (p *ExampleProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Printf("Processing task %s in node %s\n", task.ID, p.name)

	// Simulate some work
	time.Sleep(100 * time.Millisecond)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
		Ctx:     ctx,
	}
}

func (p *ExampleProcessor) Consume(ctx context.Context) error { return nil }
func (p *ExampleProcessor) Pause(ctx context.Context) error   { return nil }
func (p *ExampleProcessor) Resume(ctx context.Context) error  { return nil }
func (p *ExampleProcessor) Stop(ctx context.Context) error    { return nil }
func (p *ExampleProcessor) Close() error                      { return nil }
func (p *ExampleProcessor) GetKey() string                    { return p.name }
func (p *ExampleProcessor) SetKey(key string)                 { p.name = key }
func (p *ExampleProcessor) GetType() string                   { return "example" }

func main() {
	// Create a new DAG with enhanced features
	dag := dag.NewDAG("enhanced-example", "example", finalResultCallback)

	// Configure enhanced features
	setupEnhancedFeatures(dag)

	// Build the DAG
	buildDAG(dag)

	// Validate the DAG
	if err := dag.ValidateDAG(); err != nil {
		log.Fatalf("DAG validation failed: %v", err)
	}

	// Start monitoring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dag.StartMonitoring(ctx)
	defer dag.StopMonitoring()

	// Set up API endpoints
	setupAPI(dag)

	// Process some tasks
	processTasks(dag)

	// Display statistics
	displayStatistics(dag)

	// Start HTTP server for API
	fmt.Println("Starting HTTP server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func finalResultCallback(taskID string, result mq.Result) {
	fmt.Printf("Task %s completed with status: %s\n", taskID, result.Status)
}

func setupEnhancedFeatures(d *dag.DAG) {
	// Configure retry settings
	retryConfig := &dag.RetryConfig{
		MaxRetries:    3,
		InitialDelay:  1 * time.Second,
		MaxDelay:      10 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        true,
	}
	d.SetRetryConfig(retryConfig)

	// Configure rate limiting
	d.SetRateLimit("process", 10.0, 5) // 10 requests per second, burst of 5
	d.SetRateLimit("validate", 5.0, 2) // 5 requests per second, burst of 2

	// Configure monitoring thresholds
	alertThresholds := &dag.AlertThresholds{
		MaxFailureRate:      0.1, // 10%
		MaxExecutionTime:    5 * time.Minute,
		MaxTasksInProgress:  100,
		MinSuccessRate:      0.9, // 90%
		MaxNodeFailures:     5,
		HealthCheckInterval: 30 * time.Second,
	}
	d.SetAlertThresholds(alertThresholds)

	// Add alert handler
	alertHandler := dag.NewAlertWebhookHandler(d.Logger())
	d.AddAlertHandler(alertHandler)

	// Configure webhook manager
	httpClient := dag.NewSimpleHTTPClient(30 * time.Second)
	webhookManager := dag.NewWebhookManager(httpClient, d.Logger())

	// Add webhook for task completion events
	webhookConfig := dag.WebhookConfig{
		URL:        "http://localhost:9090/webhook",
		Headers:    map[string]string{"Authorization": "Bearer token123"},
		Timeout:    30 * time.Second,
		RetryCount: 3,
		Events:     []string{"task_completed", "task_failed"},
	}
	webhookManager.AddWebhook("task_completed", webhookConfig)

	d.SetWebhookManager(webhookManager)

	// Update DAG configuration
	config := &dag.DAGConfig{
		MaxConcurrentTasks:     50,
		TaskTimeout:            2 * time.Minute,
		NodeTimeout:            1 * time.Minute,
		MonitoringEnabled:      true,
		AlertingEnabled:        true,
		CleanupInterval:        5 * time.Minute,
		TransactionTimeout:     3 * time.Minute,
		BatchProcessingEnabled: true,
		BatchSize:              20,
		BatchTimeout:           5 * time.Second,
	}

	if err := d.UpdateConfiguration(config); err != nil {
		log.Printf("Failed to update configuration: %v", err)
	}
}

func buildDAG(d *dag.DAG) {
	// Create processors with retry capabilities
	retryConfig := &dag.RetryConfig{
		MaxRetries:    2,
		InitialDelay:  500 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
	}

	// Add nodes with enhanced features
	d.AddNodeWithRetry(dag.Function, "Start Node", "start", &ExampleProcessor{name: "start"}, retryConfig, true)
	d.AddNodeWithRetry(dag.Function, "Process Node", "process", &ExampleProcessor{name: "process"}, retryConfig)
	d.AddNodeWithRetry(dag.Function, "Validate Node", "validate", &ExampleProcessor{name: "validate"}, retryConfig)
	d.AddNodeWithRetry(dag.Function, "End Node", "end", &ExampleProcessor{name: "end"}, retryConfig)

	// Add edges
	d.AddEdge(dag.Simple, "start-to-process", "start", "process")
	d.AddEdge(dag.Simple, "process-to-validate", "process", "validate")
	d.AddEdge(dag.Simple, "validate-to-end", "validate", "end")

	// Add conditional edges
	d.AddCondition("validate", map[string]string{
		"success": "end",
		"retry":   "process",
	})
}

func setupAPI(d *dag.DAG) {
	// Set up enhanced API endpoints
	apiHandler := dag.NewEnhancedAPIHandler(d)
	apiHandler.RegisterRoutes(http.DefaultServeMux)

	// Add custom endpoint
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `
<!DOCTYPE html>
<html>
<head>
    <title>Enhanced DAG Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .section { margin: 20px 0; padding: 20px; border: 1px solid #ddd; }
        .endpoint { margin: 10px 0; }
        .method { color: #007acc; font-weight: bold; }
    </style>
</head>
<body>
    <h1>Enhanced DAG Dashboard</h1>

    <div class="section">
        <h2>Monitoring Endpoints</h2>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/metrics">/api/dag/metrics</a> - Get monitoring metrics</div>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/node-stats">/api/dag/node-stats</a> - Get node statistics</div>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/health">/api/dag/health</a> - Get health status</div>
    </div>

    <div class="section">
        <h2>Management Endpoints</h2>
        <div class="endpoint"><span class="method">POST</span> /api/dag/validate - Validate DAG structure</div>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/topology">/api/dag/topology</a> - Get topological order</div>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/critical-path">/api/dag/critical-path</a> - Get critical path</div>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/statistics">/api/dag/statistics</a> - Get DAG statistics</div>
    </div>

    <div class="section">
        <h2>Configuration Endpoints</h2>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/config">/api/dag/config</a> - Get configuration</div>
        <div class="endpoint"><span class="method">PUT</span> /api/dag/config - Update configuration</div>
        <div class="endpoint"><span class="method">POST</span> /api/dag/rate-limit - Set rate limits</div>
    </div>

    <div class="section">
        <h2>Performance Endpoints</h2>
        <div class="endpoint"><span class="method">POST</span> /api/dag/optimize - Optimize performance</div>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/circuit-breaker">/api/dag/circuit-breaker</a> - Get circuit breaker status</div>
        <div class="endpoint"><span class="method">POST</span> /api/dag/cache/clear - Clear cache</div>
        <div class="endpoint"><span class="method">GET</span> <a href="/api/dag/cache/stats">/api/dag/cache/stats</a> - Get cache statistics</div>
    </div>
</body>
</html>
		`)
	})
}

func processTasks(d *dag.DAG) {
	// Process some example tasks
	for i := 0; i < 5; i++ {
		taskData := map[string]interface{}{
			"id":      fmt.Sprintf("task-%d", i),
			"payload": fmt.Sprintf("data-%d", i),
		}

		payload, _ := json.Marshal(taskData)

		// Start a transaction for the task
		taskID := fmt.Sprintf("task-%d", i)
		tx := d.BeginTransaction(taskID)

		// Process the task
		result := d.Process(context.Background(), payload)

		// Commit or rollback based on result
		if result.Error == nil {
			if tx != nil {
				d.CommitTransaction(tx.ID)
			}
			fmt.Printf("Task %s completed successfully\n", taskID)
		} else {
			if tx != nil {
				d.RollbackTransaction(tx.ID)
			}
			fmt.Printf("Task %s failed: %v\n", taskID, result.Error)
		}

		// Small delay between tasks
		time.Sleep(100 * time.Millisecond)
	}
}

func displayStatistics(d *dag.DAG) {
	fmt.Println("\n=== DAG Statistics ===")

	// Get task metrics
	metrics := d.GetTaskMetrics()
	fmt.Printf("Task Metrics:\n")
	fmt.Printf("  Completed: %d\n", metrics.Completed)
	fmt.Printf("  Failed: %d\n", metrics.Failed)
	fmt.Printf("  Cancelled: %d\n", metrics.Cancelled)

	// Get monitoring metrics
	if monitoringMetrics := d.GetMonitoringMetrics(); monitoringMetrics != nil {
		fmt.Printf("\nMonitoring Metrics:\n")
		fmt.Printf("  Total Tasks: %d\n", monitoringMetrics.TasksTotal)
		fmt.Printf("  Tasks in Progress: %d\n", monitoringMetrics.TasksInProgress)
		fmt.Printf("  Average Execution Time: %v\n", monitoringMetrics.AverageExecutionTime)
	}

	// Get DAG statistics
	dagStats := d.GetDAGStatistics()
	fmt.Printf("\nDAG Structure:\n")
	for key, value := range dagStats {
		fmt.Printf("  %s: %v\n", key, value)
	}

	// Get topological order
	if topology, err := d.GetTopologicalOrder(); err == nil {
		fmt.Printf("\nTopological Order: %v\n", topology)
	}

	// Get critical path
	if path, err := d.GetCriticalPath(); err == nil {
		fmt.Printf("Critical Path: %v\n", path)
	}

	fmt.Println("\n=== End Statistics ===\n")
}
