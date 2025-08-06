package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// SplitJoinHandler handles string split and join operations
type SplitJoinHandler struct {
	dag.Operation
}

func (h *SplitJoinHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %w", err)}
	}

	operation, ok := h.Payload.Data["operation"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("operation not specified")}
	}

	var result map[string]any
	switch operation {
	case "split":
		result = h.splitOperation(data)
	case "join":
		result = h.joinOperation(data)
	case "split_to_array":
		result = h.splitToArrayOperation(data)
	case "join_from_array":
		result = h.joinFromArrayOperation(data)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported operation: %s", operation)}
	}

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to marshal result: %w", err)}
	}

	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

func (h *SplitJoinHandler) splitOperation(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields()
	separator := h.getSeparator()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for _, field := range fields {
		if val, ok := data[field]; ok {
			if str, ok := val.(string); ok {
				parts := strings.Split(str, separator)

				// Create individual fields for each part
				for i, part := range parts {
					result[fmt.Sprintf("%s_%d", field, i)] = strings.TrimSpace(part)
				}

				// Also store as array
				result[field+"_parts"] = parts
				result[field+"_count"] = len(parts)
			}
		}
	}

	return result
}

func (h *SplitJoinHandler) joinOperation(data map[string]any) map[string]any {
	result := make(map[string]any)
	targetField := h.getTargetField()
	separator := h.getSeparator()
	sourceFields := h.getSourceFields()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	var parts []string
	for _, field := range sourceFields {
		if val, ok := data[field]; ok && val != nil {
			parts = append(parts, fmt.Sprintf("%v", val))
		}
	}

	if len(parts) > 0 {
		result[targetField] = strings.Join(parts, separator)
	}

	return result
}

func (h *SplitJoinHandler) splitToArrayOperation(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields()
	separator := h.getSeparator()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for _, field := range fields {
		if val, ok := data[field]; ok {
			if str, ok := val.(string); ok {
				parts := strings.Split(str, separator)
				var cleanParts []interface{}
				for _, part := range parts {
					cleanParts = append(cleanParts, strings.TrimSpace(part))
				}
				result[field+"_array"] = cleanParts
			}
		}
	}

	return result
}

func (h *SplitJoinHandler) joinFromArrayOperation(data map[string]any) map[string]any {
	result := make(map[string]any)
	targetField := h.getTargetField()
	separator := h.getSeparator()
	sourceField := h.getSourceField()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	if val, ok := data[sourceField]; ok {
		if arr, ok := val.([]interface{}); ok {
			var parts []string
			for _, item := range arr {
				if item != nil {
					parts = append(parts, fmt.Sprintf("%v", item))
				}
			}
			result[targetField] = strings.Join(parts, separator)
		}
	}

	return result
}

func (h *SplitJoinHandler) getTargetFields() []string {
	if fields, ok := h.Payload.Data["fields"].([]interface{}); ok {
		var result []string
		for _, field := range fields {
			if str, ok := field.(string); ok {
				result = append(result, str)
			}
		}
		return result
	}
	return nil
}

func (h *SplitJoinHandler) getTargetField() string {
	if field, ok := h.Payload.Data["target_field"].(string); ok {
		return field
	}
	return "joined_field"
}

func (h *SplitJoinHandler) getSourceField() string {
	if field, ok := h.Payload.Data["source_field"].(string); ok {
		return field
	}
	return ""
}

func (h *SplitJoinHandler) getSourceFields() []string {
	if fields, ok := h.Payload.Data["source_fields"].([]interface{}); ok {
		var result []string
		for _, field := range fields {
			if str, ok := field.(string); ok {
				result = append(result, str)
			}
		}
		return result
	}
	return nil
}

func (h *SplitJoinHandler) getSeparator() string {
	if sep, ok := h.Payload.Data["separator"].(string); ok {
		return sep
	}
	return "," // Default separator
}

func NewSplitJoinHandler(id string) *SplitJoinHandler {
	return &SplitJoinHandler{
		Operation: dag.Operation{ID: id, Key: "split_join", Type: dag.Function, Tags: []string{"data", "transformation", "string"}},
	}
}
