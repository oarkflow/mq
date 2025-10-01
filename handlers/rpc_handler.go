package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// RPCNodeHandler handles RPC calls to external services
type RPCNodeHandler struct {
	dag.Operation
	client *http.Client
}

// RPCRequest represents a JSON-RPC 2.0 request
type RPCRequest struct {
	Jsonrpc string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      any    `json:"id,omitempty"`
}

// RPCResponse represents a JSON-RPC 2.0 response
type RPCResponse struct {
	Jsonrpc string    `json:"jsonrpc"`
	Result  any       `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
	ID      any       `json:"id,omitempty"`
}

// RPCError represents a JSON-RPC 2.0 error
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func NewRPCNodeHandler(id string) *RPCNodeHandler {
	handler := &RPCNodeHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "rpc",
			Type: dag.RPC,
			Tags: []string{"external", "rpc"},
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	handler.Payload = dag.Payload{Data: make(map[string]any)}
	handler.RequiredFields = []string{"endpoint", "method"}
	return handler
}

func (h *RPCNodeHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data, err := dag.UnmarshalPayload[map[string]any](ctx, task.Payload)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %w", err), Ctx: ctx}
	}

	// Extract RPC configuration from payload data
	endpoint, ok := h.Payload.Data["endpoint"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("RPC endpoint not specified"), Ctx: ctx}
	}

	method, ok := h.Payload.Data["method"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("RPC method not specified"), Ctx: ctx}
	}

	// Prepare RPC request
	rpcReq := RPCRequest{
		Jsonrpc: "2.0",
		Method:  method,
		ID:      task.ID, // Use task ID as request ID
	}

	// Get parameters from task data or payload
	if params, exists := data["params"]; exists {
		rpcReq.Params = params
	} else if params, exists := h.Payload.Data["params"]; exists {
		rpcReq.Params = params
	}

	// Add any additional data from task payload as params if not already set
	if rpcReq.Params == nil {
		rpcReq.Params = data
	}

	// Make the RPC call
	result, err := h.makeRPCCall(ctx, endpoint, rpcReq)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("RPC call failed: %w", err), Ctx: ctx}
	}

	// Prepare response
	responseData := map[string]any{
		"rpc_response":  result,
		"original_data": data,
	}
	generated := h.GeneratedFields
	if len(generated) == 0 {
		generated = append(generated, h.ID)
	}
	for _, g := range generated {
		data[g] = responseData
	}
	responsePayload, err := json.Marshal(responseData)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to marshal response: %w", err), Ctx: ctx}
	}

	return mq.Result{Payload: responsePayload, Ctx: ctx}
}

func (h *RPCNodeHandler) makeRPCCall(ctx context.Context, endpoint string, req RPCRequest) (any, error) {
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal RPC request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := h.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}

	var rpcResp RPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RPC response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}
