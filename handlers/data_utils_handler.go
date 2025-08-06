package handlers

import (
	"context"
	"fmt"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// DataUtilsHandler provides utility functions for common data operations
type DataUtilsHandler struct {
	dag.Operation
	UtilityType string         `json:"utility_type"` // type of utility operation
	Config      map[string]any `json:"config"`       // operation configuration
}

// Utility operation types:
// - "deduplicate": Remove duplicate entries from arrays or objects
// - "merge": Merge multiple objects or arrays
// - "diff": Compare two data structures and return differences
// - "sort": Sort arrays or object keys
// - "reverse": Reverse arrays or strings
// - "sample": Take a sample of data
// - "validate_schema": Validate data against a schema
// - "convert_types": Convert data types in bulk

func (d *DataUtilsHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	var result map[string]any
	var err error

	switch d.UtilityType {
	case "deduplicate":
		result, err = d.deduplicate(data)
	case "merge":
		result, err = d.merge(data)
	case "diff":
		result, err = d.diff(data)
	case "sort":
		result, err = d.sort(data)
	case "reverse":
		result, err = d.reverse(data)
	case "sample":
		result, err = d.sample(data)
	case "validate_schema":
		result, err = d.validateSchema(data)
	case "convert_types":
		result, err = d.convertTypes(data)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported utility type: %s", d.UtilityType), Ctx: ctx}
	}

	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	bt, _ := json.Marshal(result)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (d *DataUtilsHandler) deduplicate(data map[string]any) (map[string]any, error) {
	sourceField, _ := d.Config["source_field"].(string)
	targetField, _ := d.Config["target_field"].(string)
	dedupeBy, _ := d.Config["dedupe_by"].(string) // field to dedupe by, or empty for exact match

	if targetField == "" {
		targetField = sourceField
	}

	sourceData, exists := data[sourceField]
	if !exists {
		return nil, fmt.Errorf("source field '%s' not found", sourceField)
	}

	// Implementation depends on data type
	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}

	// Basic deduplication logic - can be extended
	if arrayData, ok := sourceData.([]any); ok {
		seen := make(map[string]bool)
		var dedupedArray []any

		for _, item := range arrayData {
			var key string
			if dedupeBy != "" {
				// Dedupe by specific field
				if itemMap, ok := item.(map[string]any); ok {
					key = fmt.Sprintf("%v", itemMap[dedupeBy])
				}
			} else {
				// Dedupe by entire item
				key = fmt.Sprintf("%v", item)
			}

			if !seen[key] {
				seen[key] = true
				dedupedArray = append(dedupedArray, item)
			}
		}

		result[targetField] = dedupedArray
	}

	return result, nil
}

func (d *DataUtilsHandler) merge(data map[string]any) (map[string]any, error) {
	sourceFields, _ := d.Config["source_fields"].([]any)
	targetField, _ := d.Config["target_field"].(string)
	mergeStrategy, _ := d.Config["strategy"].(string) // "overwrite", "append", "combine"

	if len(sourceFields) < 2 {
		return nil, fmt.Errorf("at least 2 source fields required for merge")
	}

	var mergedResult any

	switch mergeStrategy {
	case "overwrite":
		// Merge objects by overwriting keys
		merged := make(map[string]any)
		for _, fieldName := range sourceFields {
			if field, ok := fieldName.(string); ok {
				if fieldData, exists := data[field]; exists {
					if fieldMap, ok := fieldData.(map[string]any); ok {
						for k, v := range fieldMap {
							merged[k] = v
						}
					}
				}
			}
		}
		mergedResult = merged

	case "append":
		// Merge arrays by appending
		var merged []any
		for _, fieldName := range sourceFields {
			if field, ok := fieldName.(string); ok {
				if fieldData, exists := data[field]; exists {
					if fieldArray, ok := fieldData.([]any); ok {
						merged = append(merged, fieldArray...)
					}
				}
			}
		}
		mergedResult = merged

	default:
		return nil, fmt.Errorf("unsupported merge strategy: %s", mergeStrategy)
	}

	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}
	result[targetField] = mergedResult

	return result, nil
}

func (d *DataUtilsHandler) diff(data map[string]any) (map[string]any, error) {
	field1, _ := d.Config["first_field"].(string)
	field2, _ := d.Config["second_field"].(string)
	targetField, _ := d.Config["target_field"].(string)

	data1, exists1 := data[field1]
	data2, exists2 := data[field2]

	if !exists1 || !exists2 {
		return nil, fmt.Errorf("both comparison fields must exist")
	}

	// Basic diff implementation
	diffResult := map[string]any{
		"equal":       fmt.Sprintf("%v", data1) == fmt.Sprintf("%v", data2),
		"first_only":  d.findUniqueElements(data1, data2),
		"second_only": d.findUniqueElements(data2, data1),
		"common":      d.findCommonElements(data1, data2),
	}

	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}
	result[targetField] = diffResult

	return result, nil
}

func (d *DataUtilsHandler) findUniqueElements(data1, data2 any) []any {
	// Simplified implementation for arrays
	if array1, ok := data1.([]any); ok {
		if array2, ok := data2.([]any); ok {
			set2 := make(map[string]bool)
			for _, item := range array2 {
				set2[fmt.Sprintf("%v", item)] = true
			}

			var unique []any
			for _, item := range array1 {
				if !set2[fmt.Sprintf("%v", item)] {
					unique = append(unique, item)
				}
			}
			return unique
		}
	}
	return nil
}

func (d *DataUtilsHandler) findCommonElements(data1, data2 any) []any {
	// Simplified implementation for arrays
	if array1, ok := data1.([]any); ok {
		if array2, ok := data2.([]any); ok {
			set2 := make(map[string]bool)
			for _, item := range array2 {
				set2[fmt.Sprintf("%v", item)] = true
			}

			var common []any
			seen := make(map[string]bool)
			for _, item := range array1 {
				key := fmt.Sprintf("%v", item)
				if set2[key] && !seen[key] {
					common = append(common, item)
					seen[key] = true
				}
			}
			return common
		}
	}
	return nil
}

func (d *DataUtilsHandler) sort(data map[string]any) (map[string]any, error) {
	sourceField, _ := d.Config["source_field"].(string)
	targetField, _ := d.Config["target_field"].(string)
	// sortBy, _ := d.Config["sort_by"].(string)
	// direction, _ := d.Config["direction"].(string) // "asc" or "desc"

	if targetField == "" {
		targetField = sourceField
	}

	sourceData, exists := data[sourceField]
	if !exists {
		return nil, fmt.Errorf("source field '%s' not found", sourceField)
	}

	// Basic sorting implementation
	// For production, use more sophisticated sorting
	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}

	// This is a placeholder - implement proper sorting based on data type
	result[targetField] = sourceData

	return result, nil
}

func (d *DataUtilsHandler) reverse(data map[string]any) (map[string]any, error) {
	sourceField, _ := d.Config["source_field"].(string)
	targetField, _ := d.Config["target_field"].(string)

	if targetField == "" {
		targetField = sourceField
	}

	sourceData, exists := data[sourceField]
	if !exists {
		return nil, fmt.Errorf("source field '%s' not found", sourceField)
	}

	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}

	// Reverse arrays
	if arrayData, ok := sourceData.([]any); ok {
		reversed := make([]any, len(arrayData))
		for i, item := range arrayData {
			reversed[len(arrayData)-1-i] = item
		}
		result[targetField] = reversed
	} else if strData, ok := sourceData.(string); ok {
		// Reverse strings
		runes := []rune(strData)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		result[targetField] = string(runes)
	} else {
		result[targetField] = sourceData
	}

	return result, nil
}

func (d *DataUtilsHandler) sample(data map[string]any) (map[string]any, error) {
	sourceField, _ := d.Config["source_field"].(string)
	targetField, _ := d.Config["target_field"].(string)
	sampleSize, _ := d.Config["sample_size"].(float64)

	if targetField == "" {
		targetField = sourceField
	}

	sourceData, exists := data[sourceField]
	if !exists {
		return nil, fmt.Errorf("source field '%s' not found", sourceField)
	}

	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}

	// Basic sampling for arrays
	if arrayData, ok := sourceData.([]any); ok {
		size := int(sampleSize)
		if size > len(arrayData) {
			size = len(arrayData)
		}

		if size <= 0 {
			result[targetField] = []any{}
		} else if size >= len(arrayData) {
			result[targetField] = arrayData
		} else {
			// Simple sampling - take first N elements
			// For production, implement proper random sampling
			sample := make([]any, size)
			copy(sample, arrayData[:size])
			result[targetField] = sample
		}
	} else {
		result[targetField] = sourceData
	}

	return result, nil
}

func (d *DataUtilsHandler) validateSchema(data map[string]any) (map[string]any, error) {
	// Basic schema validation placeholder
	// For production, implement proper JSON schema validation
	sourceField, _ := d.Config["source_field"].(string)
	schema, _ := d.Config["schema"].(map[string]any)

	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}

	// Placeholder validation result
	result["validation_result"] = map[string]any{
		"valid":  true,
		"errors": []string{},
		"schema": schema,
		"data":   data[sourceField],
	}

	return result, nil
}

func (d *DataUtilsHandler) convertTypes(data map[string]any) (map[string]any, error) {
	conversions, _ := d.Config["conversions"].(map[string]any)

	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}

	// Apply type conversions
	for field, targetType := range conversions {
		if value, exists := result[field]; exists {
			converted, err := d.convertType(value, fmt.Sprintf("%v", targetType))
			if err == nil {
				result[field] = converted
			}
		}
	}

	return result, nil
}

func (d *DataUtilsHandler) convertType(value any, targetType string) (any, error) {
	switch targetType {
	case "string":
		return fmt.Sprintf("%v", value), nil
	case "int":
		if num := d.toFloat64(value); num != 0 {
			return int(num), nil
		}
		return 0, nil
	case "float":
		return d.toFloat64(value), nil
	case "bool":
		str := fmt.Sprintf("%v", value)
		return str == "true" || str == "1" || str == "yes", nil
	default:
		return value, fmt.Errorf("unsupported target type: %s", targetType)
	}
}

func (d *DataUtilsHandler) toFloat64(value any) float64 {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	case string:
		var result float64
		if n, err := fmt.Sscanf(v, "%f", &result); err == nil && n == 1 {
			return result
		}
	}
	return 0
}

// Factory functions for common utilities
func NewDeduplicateHandler(id, sourceField, targetField, dedupeBy string) *DataUtilsHandler {
	return &DataUtilsHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "deduplicate_data",
			Type: dag.Function,
			Tags: []string{"data", "utils", "deduplicate"},
		},
		UtilityType: "deduplicate",
		Config: map[string]any{
			"source_field": sourceField,
			"target_field": targetField,
			"dedupe_by":    dedupeBy,
		},
	}
}

func NewMergeHandler(id string, sourceFields []string, targetField, strategy string) *DataUtilsHandler {
	var anyFields []any
	for _, field := range sourceFields {
		anyFields = append(anyFields, field)
	}

	return &DataUtilsHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "merge_data",
			Type: dag.Function,
			Tags: []string{"data", "utils", "merge"},
		},
		UtilityType: "merge",
		Config: map[string]any{
			"source_fields": anyFields,
			"target_field":  targetField,
			"strategy":      strategy,
		},
	}
}

func NewDataDiffHandler(id, field1, field2, targetField string) *DataUtilsHandler {
	return &DataUtilsHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "diff_data",
			Type: dag.Function,
			Tags: []string{"data", "utils", "diff"},
		},
		UtilityType: "diff",
		Config: map[string]any{
			"first_field":  field1,
			"second_field": field2,
			"target_field": targetField,
		},
	}
}
