package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/handlers"
)

func main() {
	// Initialize handlers
	handlers.Init()

	// Create a DAG with external service integration
	flow := dag.NewDAG("External Service Integration", "external-integration", func(taskID string, result mq.Result) {
		fmt.Printf("DAG completed for task %s\n", taskID)
	})

	// Add RPC node for calling PHP Laravel RPC service
	rpcHandler := handlers.NewRPCNodeHandler("laravel-rpc")
	rpcHandler.SetConfig(dag.Payload{
		Data: map[string]any{
			"endpoint": "http://localhost:8000/api/rpc", // Laravel RPC endpoint
			"method":   "user.create",                   // RPC method name
		},
	})

	// Add HTTP API node for calling PHP Laravel REST API
	httpHandler := handlers.NewHTTPAPINodeHandler("laravel-api")
	httpHandler.SetConfig(dag.Payload{
		Data: map[string]any{
			"method": "POST",
			"url":    "http://localhost:8000/api/users", // Laravel API endpoint
			"headers": map[string]any{
				"Authorization": "Bearer your-token-here",
				"Content-Type":  "application/json",
			},
		},
	})

	// Add nodes to DAG
	flow.AddNode(dag.RPC, "Create User via RPC", "rpc-create-user", rpcHandler, true)
	flow.AddNode(dag.HTTPAPI, "Create User via API", "api-create-user", httpHandler)
	flow.AddNode(dag.Function, "Process Result", "process-result", &ProcessResult{})

	// Add edges
	flow.AddEdge(dag.Simple, "RPC to API", "rpc-create-user", "api-create-user")
	flow.AddEdge(dag.Simple, "API to Process", "api-create-user", "process-result")

	// Example payload
	payload := map[string]any{
		"name":     "John Doe",
		"email":    "john@example.com",
		"password": "securepassword",
		"params": map[string]any{ // For RPC call
			"user_data": map[string]any{
				"name":  "John Doe",
				"email": "john@example.com",
			},
		},
	}

	// Set body for HTTP API call
	payload["body"] = map[string]any{
		"name":     "John Doe",
		"email":    "john@example.com",
		"password": "securepassword",
	}

	// Convert payload to JSON bytes for processing
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("Error marshaling payload: %v\n", err)
		return
	}

	// Process the DAG
	result := flow.Process(context.Background(), payloadBytes)
	if result.Error != nil {
		fmt.Printf("Error: %v\n", result.Error)
	} else {
		fmt.Printf("Success: %s\n", string(result.Payload))
	}
}

// ProcessResult is a simple handler to process the final result
type ProcessResult struct {
	dag.Operation
}

func (p *ProcessResult) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data, err := dag.UnmarshalPayload[map[string]any](ctx, task.Payload)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	fmt.Printf("Processing final result: %+v\n", data)

	// Extract results from both RPC and HTTP API calls
	rpcResult := data["rpc_response"]
	apiResult := data["http_response"]

	result := map[string]any{
		"rpc_call_result": rpcResult,
		"api_call_result": apiResult,
		"processed":       true,
	}

	payload, _ := json.Marshal(result)
	return mq.Result{Payload: payload, Ctx: ctx}
}
