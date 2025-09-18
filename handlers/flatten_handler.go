package handlers

import (
	"context"
	"fmt"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// FlattenHandler handles flattening array of objects to single objects
type FlattenHandler struct {
	dag.Operation
}

func (h *FlattenHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
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
	case "flatten_settings":
		result = h.flattenSettings(data)
	case "flatten_key_value":
		result = h.flattenKeyValue(data)
	case "flatten_nested_objects":
		result = h.flattenNestedObjects(data)
	case "flatten_array":
		result = h.flattenArray(data)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported operation: %s", operation), Ctx: ctx}
	}

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to marshal result: %w", err), Ctx: ctx}
	}

	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

// flattenSettings converts array of settings objects with key, value, value_type to a flat object
func (h *FlattenHandler) flattenSettings(data map[string]any) map[string]any {
	result := make(map[string]any)
	sourceField := h.getSourceField()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	if settingsArray, ok := data[sourceField].([]any); ok {
		flattened := make(map[string]any)

		for _, item := range settingsArray {
			if setting, ok := item.(map[string]any); ok {
				key, keyExists := setting["key"].(string)
				value, valueExists := setting["value"]
				valueType, typeExists := setting["value_type"].(string)

				if keyExists && valueExists {
					// Convert value based on value_type
					if typeExists {
						flattened[key] = h.convertValue(value, valueType)
					} else {
						flattened[key] = value
					}
				}
			}
		}

		targetField := h.getTargetField()
		result[targetField] = flattened
	}

	return result
}

// flattenKeyValue converts array of key-value objects to a flat object
func (h *FlattenHandler) flattenKeyValue(data map[string]any) map[string]any {
	result := make(map[string]any)
	sourceField := h.getSourceField()
	keyField := h.getKeyField()
	valueField := h.getValueField()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	if kvArray, ok := data[sourceField].([]any); ok {
		flattened := make(map[string]any)

		for _, item := range kvArray {
			if kvPair, ok := item.(map[string]any); ok {
				if key, keyExists := kvPair[keyField]; keyExists {
					if value, valueExists := kvPair[valueField]; valueExists {
						if keyStr, ok := key.(string); ok {
							flattened[keyStr] = value
						}
					}
				}
			}
		}

		targetField := h.getTargetField()
		result[targetField] = flattened
	}

	return result
}

// flattenNestedObjects flattens nested objects using dot notation
func (h *FlattenHandler) flattenNestedObjects(data map[string]any) map[string]any {
	result := make(map[string]any)
	separator := h.getSeparator()

	h.flattenRecursive(data, "", result, separator)
	return result
}

// flattenArray flattens arrays by creating numbered fields
func (h *FlattenHandler) flattenArray(data map[string]any) map[string]any {
	result := make(map[string]any)
	sourceField := h.getSourceField()

	// Copy all original data except the source field
	for key, value := range data {
		if key != sourceField {
			result[key] = value
		}
	}

	if array, ok := data[sourceField].([]any); ok {
		for i, item := range array {
			if obj, ok := item.(map[string]any); ok {
				for key, value := range obj {
					result[fmt.Sprintf("%s_%d_%s", sourceField, i, key)] = value
				}
			} else {
				result[fmt.Sprintf("%s_%d", sourceField, i)] = item
			}
		}
	}

	return result
}

func (h *FlattenHandler) flattenRecursive(obj map[string]any, prefix string, result map[string]any, separator string) {
	for key, value := range obj {
		newKey := key
		if prefix != "" {
			newKey = prefix + separator + key
		}

		switch v := value.(type) {
		case map[string]any:
			nestedMap := make(map[string]any)
			for k, val := range v {
				nestedMap[k] = val
			}
			h.flattenRecursive(nestedMap, newKey, result, separator)
		case []any:
			// For arrays, create numbered fields
			for i, item := range v {
				itemKey := fmt.Sprintf("%s%s%d", newKey, separator, i)
				if itemMap, ok := item.(map[string]any); ok {
					nestedMap := make(map[string]any)
					for k, val := range itemMap {
						nestedMap[k] = val
					}
					h.flattenRecursive(nestedMap, itemKey, result, separator)
				} else {
					result[itemKey] = item
				}
			}
		default:
			result[newKey] = value
		}
	}
}

func (h *FlattenHandler) convertValue(value any, valueType string) any {
	switch valueType {
	case "string":
		return fmt.Sprintf("%v", value)
	case "int", "integer":
		if str, ok := value.(string); ok {
			var intVal int
			fmt.Sscanf(str, "%d", &intVal)
			return intVal
		}
		return value
	case "float", "number":
		if str, ok := value.(string); ok {
			var floatVal float64
			fmt.Sscanf(str, "%f", &floatVal)
			return floatVal
		}
		return value
	case "bool", "boolean":
		if str, ok := value.(string); ok {
			return str == "true" || str == "1"
		}
		return value
	case "json":
		if str, ok := value.(string); ok {
			var jsonVal any
			if err := json.Unmarshal([]byte(str), &jsonVal); err == nil {
				return jsonVal
			}
		}
		return value
	default:
		return value
	}
}

func (h *FlattenHandler) getSourceField() string {
	if field, ok := h.Payload.Data["source_field"].(string); ok {
		return field
	}
	return "settings" // Default
}

func (h *FlattenHandler) getTargetField() string {
	if field, ok := h.Payload.Data["target_field"].(string); ok {
		return field
	}
	return "flattened" // Default
}

func (h *FlattenHandler) getKeyField() string {
	if field, ok := h.Payload.Data["key_field"].(string); ok {
		return field
	}
	return "key" // Default
}

func (h *FlattenHandler) getValueField() string {
	if field, ok := h.Payload.Data["value_field"].(string); ok {
		return field
	}
	return "value" // Default
}

func (h *FlattenHandler) getSeparator() string {
	if sep, ok := h.Payload.Data["separator"].(string); ok {
		return sep
	}
	return "." // Default separator for flattening
}

func NewFlattenHandler(id string) *FlattenHandler {
	return &FlattenHandler{
		Operation: dag.Operation{ID: id, Key: "flatten", Type: dag.Function, Tags: []string{"data", "transformation", "flatten"}},
	}
}
