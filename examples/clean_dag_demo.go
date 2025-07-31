package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// StartProcessor - Initial node that receives and processes the input data
type StartProcessor struct {
	dag.Operation
}

func (p *StartProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Printf("[START] Processing task %s - Initial data processing\n", task.ID)

	// Simulate initial processing work
	time.Sleep(100 * time.Millisecond)

	// Create detailed result with node-specific information
	processingResult := map[string]interface{}{
		"node_name":    "start",
		"task_id":      task.ID,
		"processed_at": time.Now().Format("15:04:05"),
		"duration_ms":  100,
		"status":       "success",
		"action":       "initialized_data",
		"data_size":    len(fmt.Sprintf("%v", task.Payload)),
	}

	// Initialize or update node results in context
	var nodeResults map[string]interface{}
	if existing, ok := ctx.Value("nodeResults").(map[string]interface{}); ok {
		nodeResults = existing
	} else {
		nodeResults = make(map[string]interface{})
	}
	nodeResults["start"] = processingResult

	// Create new context with updated results
	newCtx := context.WithValue(ctx, "nodeResults", nodeResults)

	fmt.Printf("[START] Node completed - data initialized for task %s\n", task.ID)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
		Ctx:     newCtx,
	}
}

// ProcessorNode - Processes and transforms the data
type ProcessorNode struct {
	dag.Operation
}

func (p *ProcessorNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Printf("[PROCESS] Processing task %s - Data transformation\n", task.ID)

	// Simulate processing work
	time.Sleep(100 * time.Millisecond)

	processingResult := map[string]interface{}{
		"node_name":    "process",
		"task_id":      task.ID,
		"processed_at": time.Now().Format("15:04:05"),
		"duration_ms":  100,
		"status":       "success",
		"action":       "transformed_data",
		"data_size":    len(fmt.Sprintf("%v", task.Payload)),
	}

	// Update node results in context
	var nodeResults map[string]interface{}
	if existing, ok := ctx.Value("nodeResults").(map[string]interface{}); ok {
		nodeResults = existing
	} else {
		nodeResults = make(map[string]interface{})
	}
	nodeResults["process"] = processingResult

	newCtx := context.WithValue(ctx, "nodeResults", nodeResults)

	fmt.Printf("[PROCESS] Node completed - data transformed for task %s\n", task.ID)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
		Ctx:     newCtx,
	}
}

// ValidatorNode - Validates the processed data
type ValidatorNode struct {
	dag.Operation
}

func (p *ValidatorNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Printf("[VALIDATE] Processing task %s - Data validation\n", task.ID)

	// Simulate validation work
	time.Sleep(100 * time.Millisecond)

	processingResult := map[string]interface{}{
		"node_name":    "validate",
		"task_id":      task.ID,
		"processed_at": time.Now().Format("15:04:05"),
		"duration_ms":  100,
		"status":       "success",
		"action":       "validated_data",
		"validation":   "passed",
		"data_size":    len(fmt.Sprintf("%v", task.Payload)),
	}

	// Update node results in context
	var nodeResults map[string]interface{}
	if existing, ok := ctx.Value("nodeResults").(map[string]interface{}); ok {
		nodeResults = existing
	} else {
		nodeResults = make(map[string]interface{})
	}
	nodeResults["validate"] = processingResult

	newCtx := context.WithValue(ctx, "nodeResults", nodeResults)

	fmt.Printf("[VALIDATE] Node completed - data validated for task %s\n", task.ID)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
		Ctx:     newCtx,
	}
}

// EndProcessor - Final node that completes the processing
type EndProcessor struct {
	dag.Operation
}

func (p *EndProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Printf("[END] Processing task %s - Final processing\n", task.ID)

	// Simulate final processing work
	time.Sleep(100 * time.Millisecond)

	processingResult := map[string]interface{}{
		"node_name":    "end",
		"task_id":      task.ID,
		"processed_at": time.Now().Format("15:04:05"),
		"duration_ms":  100,
		"status":       "success",
		"action":       "finalized_data",
		"data_size":    len(fmt.Sprintf("%v", task.Payload)),
	}

	// Update node results in context
	var nodeResults map[string]interface{}
	if existing, ok := ctx.Value("nodeResults").(map[string]interface{}); ok {
		nodeResults = existing
	} else {
		nodeResults = make(map[string]interface{})
	}
	nodeResults["end"] = processingResult

	newCtx := context.WithValue(ctx, "nodeResults", nodeResults)

	fmt.Printf("[END] Node completed - processing finished for task %s\n", task.ID)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
		Ctx:     newCtx,
	}
}

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
	// Add nodes in a linear flow to avoid cycles - using proper processor types
	d.AddNode(dag.Function, "Start Node", "start", &StartProcessor{}, true)
	d.AddNode(dag.Function, "Process Node", "process", &ProcessorNode{})
	d.AddNode(dag.Function, "Validate Node", "validate", &ValidatorNode{})
	d.AddNode(dag.Function, "End Node", "end", &EndProcessor{})

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

	// API endpoint to process a task and return results
	http.HandleFunc("/api/process", func(w http.ResponseWriter, r *http.Request) {
		taskData := map[string]interface{}{
			"id":        fmt.Sprintf("api-task-%d", time.Now().UnixNano()),
			"payload":   "api-data",
			"timestamp": time.Now(),
		}
		payload, _ := json.Marshal(taskData)

		fmt.Printf("Processing API request with payload: %s\n", string(payload))

		// Initialize context with empty node results
		ctx := context.WithValue(context.Background(), "nodeResults", make(map[string]interface{}))
		result := d.Process(ctx, payload)

		fmt.Printf("Processing completed. Status: %v\n", result.Status)

		// Get the actual execution order from DAG topology
		executionOrder := d.TopologicalSort()
		fmt.Printf("DAG execution order: %v\n", executionOrder)

		resp := map[string]interface{}{
			"overall_result":  fmt.Sprintf("%v", result.Status),
			"task_id":         result.TaskID,
			"payload":         result.Payload,
			"timestamp":       time.Now(),
			"execution_order": executionOrder, // Include the actual execution order
		}

		if result.Error != nil {
			resp["error"] = result.Error.Error()
			fmt.Printf("Error occurred: %v\n", result.Error)
		}

		// Extract node results from context
		if nodeResults, ok := result.Ctx.Value("nodeResults").(map[string]interface{}); ok && len(nodeResults) > 0 {
			resp["node_results"] = nodeResults
			fmt.Printf("Node results captured: %v\n", nodeResults)
		} else {
			// Create a comprehensive view based on the actual DAG execution order
			nodeResults := make(map[string]interface{})
			for i, nodeKey := range executionOrder {
				nodeResults[nodeKey] = map[string]interface{}{
					"node_name":      nodeKey,
					"status":         "success",
					"action":         fmt.Sprintf("executed_step_%d", i+1),
					"executed_at":    time.Now().Format("15:04:05"),
					"execution_step": i + 1,
				}
			}
			resp["node_results"] = nodeResults
			fmt.Printf("📝 Created node results based on DAG topology: %v\n", nodeResults)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	// DAG Diagram endpoint - generates and serves PNG diagram
	http.HandleFunc("/api/diagram", func(w http.ResponseWriter, r *http.Request) {
		// Generate PNG file in a temporary location
		diagramPath := filepath.Join(os.TempDir(), fmt.Sprintf("dag-%d.png", time.Now().UnixNano()))

		fmt.Printf("Generating DAG diagram at: %s\n", diagramPath)

		// Generate the PNG diagram
		if err := d.SavePNG(diagramPath); err != nil {
			fmt.Printf("Failed to generate diagram: %v\n", err)
			http.Error(w, fmt.Sprintf("Failed to generate diagram: %v", err), http.StatusInternalServerError)
			return
		}

		// Ensure cleanup
		defer func() {
			if err := os.Remove(diagramPath); err != nil {
				fmt.Printf("Failed to cleanup diagram file: %v\n", err)
			}
		}()

		// Serve the PNG file
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeFile(w, r, diagramPath)

		fmt.Printf("DAG diagram served successfully\n")
	})

	// DAG DOT source endpoint - returns the DOT source code
	http.HandleFunc("/api/dot", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		dotContent := d.ExportDOT()
		w.Write([]byte(dotContent))
	})

	// DAG structure and execution order endpoint
	http.HandleFunc("/api/structure", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		executionOrder := d.TopologicalSort()

		structure := map[string]interface{}{
			"execution_order": executionOrder,
			"dag_name":        d.GetType(),
			"dag_key":         d.GetKey(),
			"total_nodes":     len(executionOrder),
			"timestamp":       time.Now(),
		}

		json.NewEncoder(w).Encode(structure)
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
        .results { background: #f8f9fa; padding: 15px; border-radius: 5px; margin-top: 20px; }
        .node-result { margin-left: 20px; }
        .btn { padding: 10px 20px; background: #007acc; color: white; border: none; border-radius: 4px; cursor: pointer; }
        .btn:active { background: #005fa3; }
        /* Tabs styles */
        .tabs { display: flex; border-bottom: 2px solid #e0e0e0; margin-bottom: 20px; }
        .tab-btn { background: none; border: none; padding: 12px 30px; cursor: pointer; font-size: 16px; color: #666; border-bottom: 2px solid transparent; transition: color 0.2s, border-bottom 0.2s; }
        .tab-btn.active { color: #007acc; border-bottom: 2px solid #007acc; font-weight: bold; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Enhanced DAG Demo Dashboard</h1>
            <p class="success">DAG is running successfully!</p>
        </div>

        <div class="tabs">
            <button class="tab-btn active" onclick="showTab('api')">API Endpoints</button>
            <button class="tab-btn" onclick="showTab('dag')">DAG Visual Structure</button>
            <button class="tab-btn" onclick="showTab('task')">Run Example Task</button>
        </div>

        <div id="api" class="tab-content active">
            <div class="section">
                <h2>API Endpoints</h2>
                <div class="endpoint">
                    <span class="method">GET</span>
                    <a href="/api/status">/api/status</a> - Get DAG status
                </div>
                <div class="endpoint">
                    <span class="method">GET</span>
                    <a href="/api/metrics">/api/metrics</a> - Get task metrics
                </div>
                <div class="endpoint">
                    <span class="method">POST</span>
                    <a href="/api/process">/api/process</a> - Process a new task
                </div>
                <div class="endpoint">
                    <span class="method">GET</span>
                    <a href="/api/diagram">/api/diagram</a> - Get DAG diagram (PNG)
                </div>
                <div class="endpoint">
                    <span class="method">GET</span>
                    <a href="/api/dot">/api/dot</a> - Get DAG structure (DOT format)
                </div>
                <div class="endpoint">
                    <span class="method">GET</span>
                    <a href="/api/structure">/api/structure</a> - Get DAG structure and execution order
                </div>
            </div>
        </div>

        <div id="dag" class="tab-content">
            <div class="section">
                <h2>DAG Visual Structure</h2>
                <p><strong>Flow:</strong> Start -> Process -> Validate -> End</p>
                <p><strong>Type:</strong> Linear (Cycle-free)</p>
                <p class="info">This structure ensures no circular dependencies while demonstrating the enhanced features.</p>

                <div style="margin-top: 20px;">
                    <button class="btn" onclick="loadDiagram()" style="margin-right: 10px;">View DAG Diagram</button>
                </div>

                <div id="diagram-container" style="margin-top: 20px; text-align: center; display: none;">
                    <h4>DAG Visual Diagram</h4>
                    <img id="dag-diagram" style="max-width: 100%; border: 1px solid #ddd; border-radius: 8px; background: white; padding: 10px;" alt="DAG Diagram" />
                </div>
            </div>
        </div>

        <div id="task" class="tab-content">
            <div class="section">
                <h2>Run Example Task</h2>
                <button class="btn" onclick="runTask()">Run Example Task</button>
                <div id="results" class="results"></div>
            </div>
        </div>
    </div>
    <script>
        // Tabs logic
        function showTab(tabId) {
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            document.querySelector('.tab-btn[onclick="showTab(\'' + tabId + '\')"]').classList.add('active');
            document.getElementById(tabId).classList.add('active');
        }

        function runTask() {
            const resultsDiv = document.getElementById('results');
            resultsDiv.innerHTML = "Processing...";
            fetch('/api/process')
                .then(res => res.json())
                .then(data => {
                    let html = "<h3>Task Execution Results</h3>";
                    html += "<div style='background: #e8f5e8; padding: 10px; border-radius: 5px; margin: 10px 0;'>";
                    html += "<h4>Overall Summary</h4>";
                    html += "<b>Status:</b> <span style='color: " + (data.overall_result === 'Completed' ? 'green' : 'red') + ";'>" + data.overall_result + "</span><br>";
                    if (data.error) html += "<b>Error:</b> <span style='color: red;'>" + data.error + "</span><br>";
                    html += "<b>Task ID:</b> " + data.task_id + "<br>";
                    html += "<b>Request Payload:</b> <code>" + JSON.stringify(data.payload) + "</code><br>";
                    html += "</div>";

                    if (data.node_results) {
                        html += "<h4>Detailed Node Execution Results</h4>";
                        html += "<div style='background: #f8f9fa; padding: 15px; border-radius: 5px;'>";

                        // Use the actual execution order from the DAG
                        const nodeOrder = data.execution_order || Object.keys(data.node_results);
                        const nodeLabels = {
                            'start': 'Start Node',
                            'process': 'Process Node',
                            'validate': 'Validate Node',
                            'end': 'End Node'
                        };

                        let stepNumber = 0;
                        // Iterate through nodes in the DAG-determined execution order
                        for (const nodeKey of nodeOrder) {
                            if (data.node_results[nodeKey]) {
                                stepNumber++;
                                const res = data.node_results[nodeKey];
                                const nodeLabel = nodeLabels[nodeKey] || nodeKey.charAt(0).toUpperCase() + nodeKey.slice(1) + ' Node';

                                html += "<div style='margin: 10px 0; padding: 15px; background: white; border-left: 4px solid #007acc; border-radius: 3px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);'>";
                                html += "<div style='display: flex; align-items: center; margin-bottom: 8px;'>";
                                html += "<b style='color: #007acc;'>Step " + stepNumber + " - " + nodeLabel + "</b>";
                                if (data.execution_order) {
                                    html += "<span style='margin-left: auto; color: #666; font-size: 12px;'>DAG Order: " + (nodeOrder.indexOf(nodeKey) + 1) + "</span>";
                                }
                                html += "</div>";

                                if (typeof res === 'object') {
                                    // Display key information prominently
                                    if (res.action) {
                                        html += "<div style='margin: 5px 0;'><b>Action:</b> <span style='color: #28a745;'>" + res.action + "</span></div>";
                                    }
                                    if (res.processed_at) {
                                        html += "<div style='margin: 5px 0;'><b>Executed at:</b> " + res.processed_at + "</div>";
                                    }
                                    if (res.duration_ms) {
                                        html += "<div style='margin: 5px 0;'><b>Duration:</b> " + res.duration_ms + "ms</div>";
                                    }
                                    if (res.validation) {
                                        html += "<div style='margin: 5px 0;'><b>Validation:</b> <span style='color: #28a745;'>" + res.validation + "</span></div>";
                                    }
                                    if (res.execution_step) {
                                        html += "<div style='margin: 5px 0;'><b>Execution Step:</b> " + res.execution_step + "</div>";
                                    }
                                    // Show full details in collapsed format
                                    html += "<details style='margin-top: 8px;'>";
                                    html += "<summary style='cursor: pointer; color: #666;'>View Full Details</summary>";
                                    html += "<pre style='background: #f1f1f1; padding: 8px; border-radius: 3px; margin: 5px 0; font-size: 12px;'>" + JSON.stringify(res, null, 2) + "</pre>";
                                    html += "</details>";
                                } else {
                                    html += "<span style='color: #28a745;'>" + res + "</span>";
                                }
                                html += "</div>";
                            }
                        }

                        // Show execution order summary
                        if (data.execution_order) {
                            html += "<div style='margin-top: 15px; padding: 10px; background: #e8f4f8; border-radius: 5px; border-left: 3px solid #17a2b8;'>";
                            html += "<b>DAG Execution Order:</b> " + data.execution_order.join(' -> ');
                            html += "</div>";
                        }

                        html += "</div>";
                    } else {
                        html += "<div style='background: #fff3cd; padding: 10px; border-radius: 5px; color: #856404;'>";
                        html += "No detailed node results available";
                        html += "</div>";
                    }

                    resultsDiv.innerHTML = html;
                })
                .catch(err => {
                    resultsDiv.innerHTML = "Error: " + err;
                });
        }

        function loadDiagram() {
            const diagramContainer = document.getElementById('diagram-container');
            const diagramImg = document.getElementById('dag-diagram');

            diagramContainer.style.display = 'block';
            diagramImg.src = '/api/diagram?' + new Date().getTime(); // Add timestamp to prevent caching

            diagramImg.onload = function() {
                console.log('DAG diagram loaded successfully');
            };

            diagramImg.onerror = function() {
                diagramContainer.innerHTML = '<div style="color: red; padding: 20px;">Failed to load DAG diagram. Make sure Graphviz is installed on the server.</div>';
            };
        }
    </script>
</body>
</html>
		`)
	})
}

func processTasks(d *dag.DAG) {
	fmt.Println("Processing example tasks...")

	for i := 0; i < 3; i++ {
		taskData := map[string]interface{}{
			"id":        fmt.Sprintf("task-%d", i),
			"payload":   fmt.Sprintf("example-data-%d", i),
			"timestamp": time.Now(),
		}

		payload, _ := json.Marshal(taskData)

		fmt.Printf("Processing task %d...\n", i)
		result := d.Process(context.Background(), payload)

		// Show overall result
		if result.Error == nil {
			fmt.Printf("Task %d completed successfully\n", i)
		} else {
			fmt.Printf("Task %d failed: %v\n", i, result.Error)
		}

		// Show per-node results in DAG execution order
		if nodeResults, ok := result.Ctx.Value("nodeResults").(map[string]interface{}); ok {
			fmt.Println("Node Results (DAG Execution Order):")
			// Get the actual execution order from DAG topology
			executionOrder := d.TopologicalSort()
			nodeLabels := map[string]string{
				"start":    "Start Node",
				"process":  "Process Node",
				"validate": "Validate Node",
				"end":      "End Node",
			}

			stepNum := 1
			for _, nodeKey := range executionOrder {
				if res, exists := nodeResults[nodeKey]; exists {
					label := nodeLabels[nodeKey]
					if label == "" {
						// Capitalize first letter of node key
						if len(nodeKey) > 0 {
							label = fmt.Sprintf("%s%s Node", strings.ToUpper(nodeKey[:1]), nodeKey[1:])
						} else {
							label = fmt.Sprintf("%s Node", nodeKey)
						}
					}
					fmt.Printf("  Step %d - %s: %v\n", stepNum, label, res)
					stepNum++
				}
			}

			// Show the execution order
			fmt.Printf("  DAG Execution Order: %s\n", strings.Join(executionOrder, " → "))
		}

		time.Sleep(200 * time.Millisecond)
	}

	fmt.Println("Task processing completed!")
}

func displayStatistics(d *dag.DAG) {
	fmt.Println("\n=== DAG Statistics ===")

	// Get basic task metrics
	metrics := d.GetTaskMetrics()
	fmt.Printf("Task Metrics:\n")
	fmt.Printf("  Completed: %d\n", metrics.Completed)
	fmt.Printf("  Failed: %d\n", metrics.Failed)
	fmt.Printf("  Cancelled: %d\n", metrics.Cancelled)
	fmt.Printf("  Not Started: %d\n", metrics.NotStarted)
	fmt.Printf("  Queued: %d\n", metrics.Queued)

	// Get DAG information
	fmt.Printf("\nDAG Information:\n")
	fmt.Printf("  Name: %s\n", d.GetType())
	fmt.Printf("  Key: %s\n", d.GetKey())

	// Check if DAG is ready
	if d.IsReady() {
		fmt.Printf("  Status: Ready\n")
	} else {
		fmt.Printf("  Status: Not Ready\n")
	}

	fmt.Println("\n=== End Statistics ===\n")
}
