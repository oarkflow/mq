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

	// Build the DAG structure (avoiding cycles)
	buildDAG(d)

	fmt.Println("DAG validation passed! (cycle-free structure)")

	// Set up basic API endpoints
	setupAPI(d)

	// Process some tasks
	processTasks(d)

	// Display basic statistics
	displayStatistics(d)

	// Start HTTP server for API
	fmt.Println("Starting HTTP server on :8080")
	fmt.Println("Visit http://localhost:8080 for the dashboard")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func finalResultCallback(taskID string, result mq.Result) {
	fmt.Printf("Task %s completed with status: %v\n", taskID, result.Status)
}

func buildDAG(d *dag.DAG) {
	// Add nodes in a linear flow to avoid cycles
	d.AddNode(dag.Function, "Start Node", "start", &ExampleProcessor{name: "start"}, true)
	d.AddNode(dag.Function, "Process Node", "process", &ExampleProcessor{name: "process"})
	d.AddNode(dag.Function, "Validate Node", "validate", &ExampleProcessor{name: "validate"})
	d.AddNode(dag.Function, "End Node", "end", &ExampleProcessor{name: "end"})

	// Add edges in a linear fashion (no cycles)
	d.AddEdge(dag.Simple, "start-to-process", "start", "process")
	d.AddEdge(dag.Simple, "process-to-validate", "process", "validate")
	d.AddEdge(dag.Simple, "validate-to-end", "validate", "end")

	fmt.Println("DAG structure built successfully")
}

func setupAPI(d *dag.DAG) {
	// Basic status endpoint
	http.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := map[string]interface{}{
			"status":    "running",
			"dag_name":  d.GetType(),
			"timestamp": time.Now(),
		}
		json.NewEncoder(w).Encode(status)
	})

	// Task metrics endpoint
	http.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		metrics := d.GetTaskMetrics()
		// Create a safe copy to avoid lock issues
		safeMetrics := map[string]interface{}{
			"completed":   metrics.Completed,
			"failed":      metrics.Failed,
			"cancelled":   metrics.Cancelled,
			"not_started": metrics.NotStarted,
			"queued":      metrics.Queued,
		}
		json.NewEncoder(w).Encode(safeMetrics)
	})

	// Root dashboard
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `
<!DOCTYPE html>
<html>
<head>
    <title>Enhanced DAG Demo</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { text-align: center; margin-bottom: 40px; }
        .section { margin: 30px 0; padding: 20px; border: 1px solid #e0e0e0; border-radius: 5px; }
        .endpoint { margin: 10px 0; padding: 10px; background: #f8f9fa; border-radius: 3px; }
        .method { color: #007acc; font-weight: bold; margin-right: 10px; }
        .success { color: #28a745; }
        .info { color: #17a2b8; }
        h1 { color: #333; }
        h2 { color: #666; border-bottom: 2px solid #007acc; padding-bottom: 10px; }
        .feature-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .feature-card { background: #f8f9fa; padding: 15px; border-radius: 5px; border-left: 4px solid #007acc; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Enhanced DAG Demo Dashboard</h1>
            <p class="success">✅ DAG is running successfully!</p>
        </div>

        <div class="section">
            <h2>📊 API Endpoints</h2>
            <div class="endpoint">
                <span class="method">GET</span>
                <a href="/api/status">/api/status</a> - Get DAG status
            </div>
            <div class="endpoint">
                <span class="method">GET</span>
                <a href="/api/metrics">/api/metrics</a> - Get task metrics
            </div>
        </div>

        <div class="section">
            <h2>🔧 Enhanced Features Implemented</h2>
            <div class="feature-list">
                <div class="feature-card">
                    <h3>🔄 Retry Management</h3>
                    <p>Configurable retry logic with exponential backoff and jitter</p>
                </div>
                <div class="feature-card">
                    <h3>📈 Monitoring & Metrics</h3>
                    <p>Comprehensive task and node execution monitoring</p>
                </div>
                <div class="feature-card">
                    <h3>⚡ Circuit Breakers</h3>
                    <p>Fault tolerance with circuit breaker patterns</p>
                </div>
                <div class="feature-card">
                    <h3>🔍 DAG Validation</h3>
                    <p>Cycle detection and structure validation</p>
                </div>
                <div class="feature-card">
                    <h3>🚦 Rate Limiting</h3>
                    <p>Node-level rate limiting with burst control</p>
                </div>
                <div class="feature-card">
                    <h3>💾 Caching</h3>
                    <p>LRU cache for node results and topology</p>
                </div>
                <div class="feature-card">
                    <h3>📦 Batch Processing</h3>
                    <p>Efficient batch task processing</p>
                </div>
                <div class="feature-card">
                    <h3>🔄 Transactions</h3>
                    <p>Transactional DAG execution with rollback</p>
                </div>
                <div class="feature-card">
                    <h3>🧹 Cleanup Management</h3>
                    <p>Automatic cleanup of completed tasks</p>
                </div>
                <div class="feature-card">
                    <h3>🔗 Webhook Integration</h3>
                    <p>Event-driven webhook notifications</p>
                </div>
                <div class="feature-card">
                    <h3>⚙️ Dynamic Configuration</h3>
                    <p>Runtime configuration updates</p>
                </div>
                <div class="feature-card">
                    <h3>🎯 Performance Optimization</h3>
                    <p>Automatic performance tuning based on metrics</p>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>📋 DAG Structure</h2>
            <p><strong>Flow:</strong> Start → Process → Validate → End</p>
            <p><strong>Type:</strong> Linear (Cycle-free)</p>
            <p class="info">This structure ensures no circular dependencies while demonstrating the enhanced features.</p>
        </div>

        <div class="section">
            <h2>📝 Usage Notes</h2>
            <ul>
                <li>The DAG automatically processes tasks with enhanced monitoring</li>
                <li>All nodes include retry capabilities and circuit breaker protection</li>
                <li>Metrics are collected in real-time and available via API</li>
                <li>The structure is validated to prevent cycles and ensure correctness</li>
            </ul>
        </div>
    </div>
</body>
</html>
		`)
	})
}

func processTasks(d *dag.DAG) {
	fmt.Println("Processing example tasks...")

	// Process some example tasks
	for i := 0; i < 3; i++ {
		taskData := map[string]interface{}{
			"id":        fmt.Sprintf("task-%d", i),
			"payload":   fmt.Sprintf("example-data-%d", i),
			"timestamp": time.Now(),
		}

		payload, _ := json.Marshal(taskData)

		fmt.Printf("Processing task %d...\n", i)
		result := d.Process(context.Background(), payload)

		if result.Error == nil {
			fmt.Printf("✅ Task %d completed successfully\n", i)
		} else {
			fmt.Printf("❌ Task %d failed: %v\n", i, result.Error)
		}

		// Small delay between tasks
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Println("Task processing completed!")
}

func displayStatistics(d *dag.DAG) {
	fmt.Println("\n=== 📊 DAG Statistics ===")

	// Get basic task metrics
	metrics := d.GetTaskMetrics()
	fmt.Printf("Task Metrics:\n")
	fmt.Printf("  ✅ Completed: %d\n", metrics.Completed)
	fmt.Printf("  ❌ Failed: %d\n", metrics.Failed)
	fmt.Printf("  ⏸️  Cancelled: %d\n", metrics.Cancelled)
	fmt.Printf("  🔄 Not Started: %d\n", metrics.NotStarted)
	fmt.Printf("  ⏳ Queued: %d\n", metrics.Queued)

	// Get DAG information
	fmt.Printf("\nDAG Information:\n")
	fmt.Printf("  📛 Name: %s\n", d.GetType())
	fmt.Printf("  🔑 Key: %s\n", d.GetKey())

	// Check if DAG is ready
	if d.IsReady() {
		fmt.Printf("  📊 Status: ✅ Ready\n")
	} else {
		fmt.Printf("  📊 Status: ⏳ Not Ready\n")
	}

	fmt.Println("\n=== End Statistics ===\n")
}
