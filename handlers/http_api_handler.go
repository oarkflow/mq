package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// HTTPAPINodeHandler handles HTTP API calls to external services
type HTTPAPINodeHandler struct {
	dag.Operation
	client *http.Client
}

func NewHTTPAPINodeHandler(id string) *HTTPAPINodeHandler {
	handler := &HTTPAPINodeHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "http_api",
			Type: dag.HTTPAPI,
			Tags: []string{"external", "http", "api"},
		},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	handler.Payload = dag.Payload{Data: make(map[string]any)}
	handler.RequiredFields = []string{"url"}
	return handler
}

func (h *HTTPAPINodeHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data, err := dag.UnmarshalPayload[map[string]any](ctx, task.Payload)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %w", err), Ctx: ctx}
	}

	// Extract HTTP configuration from payload data
	method, ok := h.Payload.Data["method"].(string)
	if !ok {
		method = "GET" // default to GET
	}

	url, ok := h.Payload.Data["url"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("HTTP URL not specified"), Ctx: ctx}
	}

	// Prepare request body
	var reqBody io.Reader
	if method == "POST" || method == "PUT" || method == "PATCH" {
		if body, exists := data["body"]; exists {
			bodyBytes, err := json.Marshal(body)
			if err != nil {
				return mq.Result{Error: fmt.Errorf("failed to marshal request body: %w", err), Ctx: ctx}
			}
			reqBody = bytes.NewBuffer(bodyBytes)
		} else if body, exists := h.Payload.Data["body"]; exists {
			bodyBytes, err := json.Marshal(body)
			if err != nil {
				return mq.Result{Error: fmt.Errorf("failed to marshal request body: %w", err), Ctx: ctx}
			}
			reqBody = bytes.NewBuffer(bodyBytes)
		}
	}

	// Make the HTTP call
	result, err := h.makeHTTPCall(ctx, method, url, reqBody, data)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("HTTP call failed: %w", err), Ctx: ctx}
	}

	// Prepare response
	responseData := map[string]any{
		"http_response": result,
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

func (h *HTTPAPINodeHandler) makeHTTPCall(ctx context.Context, method, url string, body io.Reader, data map[string]any) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	// req.Header.Set("Content-Type", "application/json")
	// req.Header.Set("Accept", "application/json")

	// Add custom headers from configuration
	if headers, ok := h.Payload.Data["headers"].(map[string]any); ok {
		for k, v := range headers {
			if strVal, ok := v.(string); ok {
				req.Header.Set(k, strVal)
			}
		}
	}

	// Add query parameters from data
	if queryParams, ok := data["query"].(map[string]any); ok {
		q := req.URL.Query()
		for k, v := range queryParams {
			if strVal, ok := v.(string); ok {
				q.Add(k, strVal)
			}
		}
		req.URL.RawQuery = q.Encode()
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	result := map[string]any{
		"status_code": resp.StatusCode,
		"status":      resp.Status,
		"headers":     make(map[string]string),
	}

	// Convert headers to map
	for k, v := range resp.Header {
		result["headers"].(map[string]string)[k] = strings.Join(v, ", ")
	}

	// Try to parse response body as JSON
	var jsonBody any
	if err := json.Unmarshal(respBody, &jsonBody); err != nil {
		// If not JSON, store as string
		result["body"] = string(respBody)
	} else {
		result["body"] = jsonBody
	}

	return result, nil
}
