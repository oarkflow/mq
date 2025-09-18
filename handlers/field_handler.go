package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// FieldHandler handles field manipulation operations (filter, add, remove, rename, etc.)
type FieldHandler struct {
	dag.Operation
}

func (h *FieldHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data, err := dag.UnmarshalPayload[map[string]any](ctx, task.Payload)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %w", err), Ctx: ctx}
	}

	operation, ok := h.Payload.Data["operation"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("operation not specified")}
	}

	var result map[string]any
	switch operation {
	case "filter", "select":
		result = h.filterFields(data)
	case "exclude", "remove":
		result = h.excludeFields(data)
	case "rename":
		result = h.renameFields(data)
	case "add", "set":
		result = h.addFields(data)
	case "copy":
		result = h.copyFields(data)
	case "merge":
		result = h.mergeFields(data)
	case "prefix":
		result = h.prefixFields(data)
	case "suffix":
		result = h.suffixFields(data)
	case "transform_keys":
		result = h.transformKeys(data)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported operation: %s", operation)}
	}

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to marshal result: %w", err)}
	}

	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

func (h *FieldHandler) filterFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	allowedFields := h.getTargetFields()

	if len(allowedFields) == 0 {
		return data // If no fields specified, return all
	}

	for _, field := range allowedFields {
		if val, ok := data[field]; ok {
			result[field] = val
		}
	}

	return result
}

func (h *FieldHandler) excludeFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	excludeFields := h.getTargetFields()

	// Copy all fields except excluded ones
	for key, value := range data {
		if !contains(excludeFields, key) {
			result[key] = value
		}
	}

	return result
}

func (h *FieldHandler) renameFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	renameMap := h.getFieldMapping()

	// Copy all fields, renaming as specified
	for key, value := range data {
		if newKey, ok := renameMap[key]; ok {
			result[newKey] = value
		} else {
			result[key] = value
		}
	}

	return result
}

func (h *FieldHandler) addFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	newFields := h.getNewFields()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	// Add new fields
	for key, value := range newFields {
		result[key] = value
	}

	return result
}

func (h *FieldHandler) copyFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	copyMap := h.getFieldMapping()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	// Copy fields to new names
	for sourceKey, targetKey := range copyMap {
		if val, ok := data[sourceKey]; ok {
			result[targetKey] = val
		}
	}

	return result
}

func (h *FieldHandler) mergeFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	mergeConfig := h.getMergeConfig()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	// Merge fields
	for targetField, config := range mergeConfig {
		sourceFields := config["fields"].([]string)
		separator := config["separator"].(string)

		var values []string
		for _, field := range sourceFields {
			if val, ok := data[field]; ok && val != nil {
				values = append(values, fmt.Sprintf("%v", val))
			}
		}

		if len(values) > 0 {
			result[targetField] = strings.Join(values, separator)
		}
	}

	return result
}

func (h *FieldHandler) prefixFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	prefix := h.getPrefix()
	targetFields := h.getTargetFields()

	for key, value := range data {
		if len(targetFields) == 0 || contains(targetFields, key) {
			result[prefix+key] = value
		} else {
			result[key] = value
		}
	}

	return result
}

func (h *FieldHandler) suffixFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	suffix := h.getSuffix()
	targetFields := h.getTargetFields()

	for key, value := range data {
		if len(targetFields) == 0 || contains(targetFields, key) {
			result[key+suffix] = value
		} else {
			result[key] = value
		}
	}

	return result
}

func (h *FieldHandler) transformKeys(data map[string]any) map[string]any {
	result := make(map[string]any)
	transformation := h.getKeyTransformation()

	for key, value := range data {
		newKey := h.applyKeyTransformation(key, transformation)
		result[newKey] = value
	}

	return result
}

func (h *FieldHandler) applyKeyTransformation(key string, transformation string) string {
	switch transformation {
	case "lowercase":
		return strings.ToLower(key)
	case "uppercase":
		return strings.ToUpper(key)
	case "snake_case":
		return h.toSnakeCase(key)
	case "camel_case":
		return h.toCamelCase(key)
	case "kebab_case":
		return h.toKebabCase(key)
	case "pascal_case":
		return h.toPascalCase(key)
	default:
		return key
	}
}

func (h *FieldHandler) toSnakeCase(s string) string {
	result := strings.ReplaceAll(s, " ", "_")
	result = strings.ReplaceAll(result, "-", "_")
	return strings.ToLower(result)
}

func (h *FieldHandler) toCamelCase(s string) string {
	parts := strings.FieldsFunc(s, func(c rune) bool {
		return c == ' ' || c == '_' || c == '-'
	})

	if len(parts) == 0 {
		return s
	}

	result := strings.ToLower(parts[0])
	for _, part := range parts[1:] {
		if len(part) > 0 {
			result += strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
		}
	}
	return result
}

func (h *FieldHandler) toKebabCase(s string) string {
	result := strings.ReplaceAll(s, " ", "-")
	result = strings.ReplaceAll(result, "_", "-")
	return strings.ToLower(result)
}

func (h *FieldHandler) toPascalCase(s string) string {
	camel := h.toCamelCase(s)
	if len(camel) > 0 {
		return strings.ToUpper(camel[:1]) + camel[1:]
	}
	return camel
}

func (h *FieldHandler) getTargetFields() []string {
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

func (h *FieldHandler) getFieldMapping() map[string]string {
	result := make(map[string]string)
	if mapping, ok := h.Payload.Data["mapping"].(map[string]interface{}); ok {
		for key, value := range mapping {
			if str, ok := value.(string); ok {
				result[key] = str
			}
		}
	}
	return result
}

func (h *FieldHandler) getNewFields() map[string]interface{} {
	if fields, ok := h.Payload.Data["new_fields"].(map[string]interface{}); ok {
		return fields
	}
	return make(map[string]interface{})
}

func (h *FieldHandler) getMergeConfig() map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{})
	if config, ok := h.Payload.Data["merge_config"].(map[string]interface{}); ok {
		for key, value := range config {
			if configMap, ok := value.(map[string]interface{}); ok {
				result[key] = configMap
			}
		}
	}
	return result
}

func (h *FieldHandler) getPrefix() string {
	if prefix, ok := h.Payload.Data["prefix"].(string); ok {
		return prefix
	}
	return ""
}

func (h *FieldHandler) getSuffix() string {
	if suffix, ok := h.Payload.Data["suffix"].(string); ok {
		return suffix
	}
	return ""
}

func (h *FieldHandler) getKeyTransformation() string {
	if transform, ok := h.Payload.Data["transformation"].(string); ok {
		return transform
	}
	return ""
}

func NewFieldHandler(id string) *FieldHandler {
	return &FieldHandler{
		Operation: dag.Operation{ID: id, Key: "field", Type: dag.Function, Tags: []string{"data", "transformation", "field"}},
	}
}
