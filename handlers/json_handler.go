package handlers

import (
	"context"
	"fmt"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// JSONHandler handles JSON parsing and stringifying operations
type JSONHandler struct {
	dag.Operation
}

func (h *JSONHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data, err := dag.UnmarshalPayload[map[string]any](ctx, task.Payload)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %w", err), Ctx: ctx}
	}

	operation, ok := h.Payload.Data["operation"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("operation not specified"), Ctx: ctx}
	}

	var result map[string]any
	switch operation {
	case "parse", "string_to_json":
		result = h.parseJSON(data)
	case "stringify", "json_to_string":
		result = h.stringifyJSON(data)
	case "pretty_print":
		result = h.prettyPrintJSON(data)
	case "minify":
		result = h.minifyJSON(data)
	case "validate":
		result = h.validateJSON(data)
	case "extract_fields":
		result = h.extractFields(data)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported operation: %s", operation), Ctx: ctx}
	}

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to marshal result: %w", err), Ctx: ctx}
	}

	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

func (h *JSONHandler) parseJSON(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for _, field := range fields {
		if val, ok := data[field]; ok {
			if str, ok := val.(string); ok {
				var parsed any
				if err := json.Unmarshal([]byte(str), &parsed); err == nil {
					targetField := h.getTargetFieldForSource(field)
					result[targetField] = parsed
					result[field+"_parsed"] = true
				} else {
					result[field+"_parse_error"] = err.Error()
					result[field+"_parsed"] = false
				}
			}
		}
	}

	return result
}

func (h *JSONHandler) stringifyJSON(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields()
	indent := h.getIndent()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for _, field := range fields {
		if val, ok := data[field]; ok {
			var jsonBytes []byte
			var err error

			if indent {
				jsonBytes, err = json.MarshalIndent(val, "", "  ")
			} else {
				jsonBytes, err = json.Marshal(val)
			}

			if err == nil {
				targetField := h.getTargetFieldForSource(field)
				result[targetField] = string(jsonBytes)
				result[field+"_stringified"] = true
			} else {
				result[field+"_stringify_error"] = err.Error()
				result[field+"_stringified"] = false
			}
		}
	}

	return result
}

func (h *JSONHandler) prettyPrintJSON(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for _, field := range fields {
		if val, ok := data[field]; ok {
			var prettyJSON any

			// If it's a string, try to parse it first
			if str, ok := val.(string); ok {
				if err := json.Unmarshal([]byte(str), &prettyJSON); err != nil {
					prettyJSON = val
				}
			} else {
				prettyJSON = val
			}

			if jsonBytes, err := json.MarshalIndent(prettyJSON, "", "  "); err == nil {
				targetField := h.getTargetFieldForSource(field)
				result[targetField] = string(jsonBytes)
			}
		}
	}

	return result
}

func (h *JSONHandler) minifyJSON(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for _, field := range fields {
		if val, ok := data[field]; ok {
			var minifyJSON any

			// If it's a string, try to parse it first
			if str, ok := val.(string); ok {
				if err := json.Unmarshal([]byte(str), &minifyJSON); err != nil {
					minifyJSON = val
				}
			} else {
				minifyJSON = val
			}

			if jsonBytes, err := json.Marshal(minifyJSON); err == nil {
				targetField := h.getTargetFieldForSource(field)
				result[targetField] = string(jsonBytes)
			}
		}
	}

	return result
}

func (h *JSONHandler) validateJSON(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for _, field := range fields {
		if val, ok := data[field]; ok {
			if str, ok := val.(string); ok {
				var temp any
				if err := json.Unmarshal([]byte(str), &temp); err == nil {
					result[field+"_valid_json"] = true
					result[field+"_json_type"] = h.getJSONType(temp)
				} else {
					result[field+"_valid_json"] = false
					result[field+"_validation_error"] = err.Error()
				}
			} else {
				result[field+"_valid_json"] = true
				result[field+"_json_type"] = h.getJSONType(val)
			}
		}
	}

	return result
}

func (h *JSONHandler) extractFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	sourceField := h.getSourceField()
	fieldsToExtract := h.getFieldsToExtract()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	if val, ok := data[sourceField]; ok {
		var jsonData map[string]any

		// If it's a string, parse it
		if str, ok := val.(string); ok {
			if err := json.Unmarshal([]byte(str), &jsonData); err != nil {
				result["extract_error"] = err.Error()
				return result
			}
		} else if obj, ok := val.(map[string]any); ok {
			jsonData = obj
		} else {
			result["extract_error"] = "source field is not a JSON object or string"
			return result
		}

		// Extract specified fields
		for _, fieldPath := range fieldsToExtract {
			if extractedVal := h.extractNestedField(jsonData, fieldPath); extractedVal != nil {
				result[fieldPath] = extractedVal
			}
		}
	}

	return result
}

func (h *JSONHandler) extractNestedField(data map[string]any, fieldPath string) any {
	// Simple implementation for dot notation
	// For more complex path extraction, could use jsonpath library
	if val, ok := data[fieldPath]; ok {
		return val
	}
	return nil
}

func (h *JSONHandler) getJSONType(val any) string {
	switch val.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}

func (h *JSONHandler) getTargetFields() []string {
	if fields, ok := h.Payload.Data["fields"].([]any); ok {
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

func (h *JSONHandler) getSourceField() string {
	if field, ok := h.Payload.Data["source_field"].(string); ok {
		return field
	}
	return ""
}

func (h *JSONHandler) getFieldsToExtract() []string {
	if fields, ok := h.Payload.Data["extract_fields"].([]any); ok {
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

func (h *JSONHandler) getTargetFieldForSource(sourceField string) string {
	// Check if there's a specific mapping
	if mapping, ok := h.Payload.Data["field_mapping"].(map[string]any); ok {
		if target, ok := mapping[sourceField].(string); ok {
			return target
		}
	}

	// Default: append suffix based on operation
	operation, _ := h.Payload.Data["operation"].(string)
	switch operation {
	case "parse", "string_to_json":
		return sourceField + "_parsed"
	case "stringify", "json_to_string":
		return sourceField + "_string"
	case "pretty_print":
		return sourceField + "_pretty"
	case "minify":
		return sourceField + "_minified"
	default:
		return sourceField + "_result"
	}
}

func (h *JSONHandler) getIndent() bool {
	if indent, ok := h.Payload.Data["indent"].(bool); ok {
		return indent
	}
	return false
}

func NewJSONHandler(id string) *JSONHandler {
	return &JSONHandler{
		Operation: dag.Operation{ID: id, Key: "json", Type: dag.Function, Tags: []string{"data", "transformation", "json"}},
	}
}
