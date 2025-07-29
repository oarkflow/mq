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
	d := dag.NewDAG("enhanced-example", "example", finalResultCallback)

	// Configure enhanced features
	setupEnhancedFeatures(d)

	// Build the DAG
	buildDAG(d)

	// Validate the DAG using the validator
	validator := d.GetValidator()
	if err := validator.ValidateStructure(); err != nil {
		log.Fatalf("DAG validation failed: %v", err)
	}

	// Start monitoring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if monitor := d.GetMonitor(); monitor != nil {
		monitor.Start(ctx)
		defer monitor.Stop()
	}

	// Set up API endpoints
	setupAPI(d)

	// Process some tasks
	processTasks(d)

	// Display statistics
	displayStatistics(d)

	// Start HTTP server for API
	fmt.Println("Starting HTTP server on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func finalResultCallback(taskID string, result mq.Result) {
	fmt.Printf("Task %s completed with status: %s\n", taskID, result.Status)
}

func setupEnhancedFeatures(d *dag.DAG) {
	// For now, just use basic configuration since enhanced methods aren't implemented yet
	fmt.Println("Setting up enhanced features...")

	// We'll use the basic DAG functionality for this demo
	// Enhanced features will be added as they become available
}

func buildDAG(d *dag.DAG) {
	// Add nodes with enhanced features - using a linear flow to avoid cycles
	d.AddNode(dag.Function, "Start Node", "start", &ExampleProcessor{name: "start"}, true)
	d.AddNode(dag.Function, "Process Node", "process", &ExampleProcessor{name: "process"})
	d.AddNode(dag.Function, "Validate Node", "validate", &ExampleProcessor{name: "validate"})
	d.AddNode(dag.Function, "Retry Node", "retry", &ExampleProcessor{name: "retry"})
	d.AddNode(dag.Function, "End Node", "end", &ExampleProcessor{name: "end"})

	// Add linear edges to avoid cycles
	d.AddEdge(dag.Simple, "start-to-process", "start", "process")
	d.AddEdge(dag.Simple, "process-to-validate", "process", "validate")

	// Add conditional edges without creating cycles
	d.AddCondition("validate", map[string]string{
		"success": "end",
		"retry":   "retry",
	})

	// Retry node goes to end (no back-loop to avoid cycle)
	d.AddEdge(dag.Simple, "retry-to-end", "retry", "end")
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
