package handlers

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// DataHandler handles miscellaneous data operations
type DataHandler struct {
	dag.Operation
}

func (h *DataHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data, err := dag.UnmarshalPayload[map[string]any](ctx, task.Payload)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %w", err), Ctx: ctx}
	}
	if data == nil {
		data = make(map[string]any)
	}

	operation, ok := h.Payload.Data["operation"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("operation not specified")}
	}

	var result map[string]any
	var conditionStatus string
	switch operation {
	case "extract":
		result = h.extractData(ctx, data)
	case "sort":
		result = h.sortData(data)
	case "deduplicate":
		result = h.deduplicateData(data)
	case "calculate":
		result = h.calculateFields(data)
	case "conditional_set":
		result = h.conditionalSet(data)
		// Extract condition status from result
		if status, ok := result["_condition_status"].(string); ok {
			conditionStatus = status
			delete(result, "_condition_status") // Remove from payload
		}
	case "type_cast":
		result = h.typeCast(data)
	case "validate_fields":
		result = h.validateFields(data)
	case "normalize":
		result = h.normalizeData(data)
	case "pivot":
		result = h.pivotData(data)
	case "unpivot":
		result = h.unpivotData(data)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported operation: %s", operation)}
	}

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to marshal result: %w", err)}
	}

	return mq.Result{
		Payload:         resultPayload,
		Ctx:             ctx,
		ConditionStatus: conditionStatus,
	}
}

func (h *DataHandler) extractData(ctx context.Context, data map[string]any) map[string]any {
	result := make(map[string]any)

	// Copy existing data
	for k, v := range data {
		result[k] = v
	}

	// Extract data based on mapping
	if h.Payload.Mapping != nil {
		for targetField, sourcePath := range h.Payload.Mapping {
			_, val := dag.GetVal(ctx, sourcePath, data)
			if val != nil {
				result[targetField] = val
			}
		}
	}

	// Handle default values
	if defaultPath, ok := h.Payload.Data["default_path"].(string); ok {
		if path, exists := result["path"]; !exists || path == "" {
			result["path"] = defaultPath
		}
	}

	return result
}

func (h *DataHandler) sortData(data map[string]any) map[string]any {
	result := make(map[string]any)

	// Copy non-array data
	for key, value := range data {
		if key != "data" {
			result[key] = value
		}
	}

	if dataArray, ok := data["data"].([]any); ok {
		sortField := h.getSortField()
		sortOrder := h.getSortOrder() // "asc" or "desc"

		// Convert to slice of maps for sorting
		var records []map[string]any
		for _, item := range dataArray {
			if record, ok := item.(map[string]any); ok {
				records = append(records, record)
			}
		}

		// Sort the records
		sort.Slice(records, func(i, j int) bool {
			vi := records[i][sortField]
			vj := records[j][sortField]

			comparison := h.compareValues(vi, vj)
			if sortOrder == "desc" {
				return comparison > 0
			}
			return comparison < 0
		})

		// Convert back to []any
		var sortedData []any
		for _, record := range records {
			sortedData = append(sortedData, record)
		}

		result["data"] = sortedData
	}

	return result
}

func (h *DataHandler) deduplicateData(data map[string]any) map[string]any {
	result := make(map[string]any)

	// Copy non-array data
	for key, value := range data {
		if key != "data" {
			result[key] = value
		}
	}

	if dataArray, ok := data["data"].([]any); ok {
		dedupeFields := h.getDedupeFields()
		seen := make(map[string]bool)
		var uniqueData []any

		for _, item := range dataArray {
			if record, ok := item.(map[string]any); ok {
				key := h.createDedupeKey(record, dedupeFields)
				if !seen[key] {
					seen[key] = true
					uniqueData = append(uniqueData, item)
				}
			}
		}

		result["data"] = uniqueData
		result["original_count"] = len(dataArray)
		result["deduplicated_count"] = len(uniqueData)
		result["duplicates_removed"] = len(dataArray) - len(uniqueData)
	}

	return result
}

func (h *DataHandler) calculateFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	calculations := h.getCalculations()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for targetField, calc := range calculations {
		operation := calc["operation"].(string)
		sourceFields := calc["fields"].([]string)

		switch operation {
		case "sum":
			result[targetField] = h.sumFields(data, sourceFields)
		case "subtract":
			result[targetField] = h.subtractFields(data, sourceFields)
		case "multiply":
			result[targetField] = h.multiplyFields(data, sourceFields)
		case "divide":
			result[targetField] = h.divideFields(data, sourceFields)
		case "average":
			result[targetField] = h.averageFields(data, sourceFields)
		case "min":
			result[targetField] = h.minFields(data, sourceFields)
		case "max":
			result[targetField] = h.maxFields(data, sourceFields)
		}
	}

	return result
}

func (h *DataHandler) conditionalSet(data map[string]any) map[string]any {
	result := make(map[string]any)
	conditions := h.getConditions()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	// Track which condition was met for setting ConditionStatus
	var metCondition string

	for targetField, condConfig := range conditions {
		condition := condConfig["condition"].(string)
		ifTrue := condConfig["if_true"]
		ifFalse := condConfig["if_false"]

		if h.evaluateCondition(data, condition) {
			result[targetField] = ifTrue
			if metCondition == "" { // Take the first met condition
				metCondition = condition
			}
		} else {
			result[targetField] = ifFalse
		}
	}

	// Set condition status if any condition was evaluated
	if metCondition != "" {
		result["_condition_status"] = metCondition
	}

	return result
}

func (h *DataHandler) typeCast(data map[string]any) map[string]any {
	result := make(map[string]any)
	castConfig := h.getCastConfig()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for field, targetType := range castConfig {
		if val, ok := data[field]; ok {
			result[field] = h.castValue(val, targetType)
		}
	}

	return result
}

func (h *DataHandler) validateFields(data map[string]any) map[string]any {
	result := make(map[string]any)
	validationRules := h.getValidationRules()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	validationResults := make(map[string]any)
	allValid := true

	for field, rules := range validationRules {
		if val, ok := data[field]; ok {
			fieldResult := h.validateField(val, rules)
			validationResults[field] = fieldResult
			if !fieldResult["valid"].(bool) {
				allValid = false
			}
		}
	}

	result["validation_results"] = validationResults
	result["all_valid"] = allValid

	return result
}

func (h *DataHandler) normalizeData(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields()
	normalizationType := h.getNormalizationType()

	// Copy all original data
	for key, value := range data {
		result[key] = value
	}

	for _, field := range fields {
		if val, ok := data[field]; ok {
			result[field] = h.normalizeValue(val, normalizationType)
		}
	}

	return result
}

func (h *DataHandler) pivotData(data map[string]any) map[string]any {
	// Simplified pivot implementation
	result := make(map[string]any)

	if dataArray, ok := data["data"].([]any); ok {
		pivotField := h.getPivotField()
		valueField := h.getValueField()

		pivoted := make(map[string]any)

		for _, item := range dataArray {
			if record, ok := item.(map[string]any); ok {
				if pivotVal, ok := record[pivotField]; ok {
					if val, ok := record[valueField]; ok {
						key := fmt.Sprintf("%v", pivotVal)
						pivoted[key] = val
					}
				}
			}
		}

		result["pivoted_data"] = pivoted
	}

	return result
}

func (h *DataHandler) unpivotData(data map[string]any) map[string]any {
	// Simplified unpivot implementation
	result := make(map[string]any)
	unpivotFields := h.getUnpivotFields()

	var unpivotedData []any

	for _, field := range unpivotFields {
		if val, ok := data[field]; ok {
			record := map[string]any{
				"field": field,
				"value": val,
			}
			unpivotedData = append(unpivotedData, record)
		}
	}

	result["data"] = unpivotedData
	result["unpivoted"] = true

	return result
}

// Helper functions
func (h *DataHandler) compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison first
	if aNum, aOk := toFloat64(a); aOk {
		if bNum, bOk := toFloat64(b); bOk {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func (h *DataHandler) createDedupeKey(record map[string]any, fields []string) string {
	var keyParts []string
	for _, field := range fields {
		keyParts = append(keyParts, fmt.Sprintf("%v", record[field]))
	}
	return strings.Join(keyParts, "|")
}

func (h *DataHandler) sumFields(data map[string]any, fields []string) float64 {
	var sum float64
	for _, field := range fields {
		if val, ok := data[field]; ok {
			if num, ok := toFloat64(val); ok {
				sum += num
			}
		}
	}
	return sum
}

func (h *DataHandler) subtractFields(data map[string]any, fields []string) float64 {
	if len(fields) < 2 {
		return 0
	}

	var result float64
	if val, ok := data[fields[0]]; ok {
		if num, ok := toFloat64(val); ok {
			result = num
		}
	}

	for _, field := range fields[1:] {
		if val, ok := data[field]; ok {
			if num, ok := toFloat64(val); ok {
				result -= num
			}
		}
	}
	return result
}

func (h *DataHandler) multiplyFields(data map[string]any, fields []string) float64 {
	result := 1.0
	for _, field := range fields {
		if val, ok := data[field]; ok {
			if num, ok := toFloat64(val); ok {
				result *= num
			}
		}
	}
	return result
}

func (h *DataHandler) divideFields(data map[string]any, fields []string) float64 {
	if len(fields) < 2 {
		return 0
	}

	var result float64
	if val, ok := data[fields[0]]; ok {
		if num, ok := toFloat64(val); ok {
			result = num
		}
	}

	for _, field := range fields[1:] {
		if val, ok := data[field]; ok {
			if num, ok := toFloat64(val); ok && num != 0 {
				result /= num
			}
		}
	}
	return result
}

func (h *DataHandler) averageFields(data map[string]any, fields []string) float64 {
	sum := h.sumFields(data, fields)
	return sum / float64(len(fields))
}

func (h *DataHandler) minFields(data map[string]any, fields []string) float64 {
	min := math.Inf(1)
	for _, field := range fields {
		if val, ok := data[field]; ok {
			if num, ok := toFloat64(val); ok {
				if num < min {
					min = num
				}
			}
		}
	}
	return min
}

func (h *DataHandler) maxFields(data map[string]any, fields []string) float64 {
	max := math.Inf(-1)
	for _, field := range fields {
		if val, ok := data[field]; ok {
			if num, ok := toFloat64(val); ok {
				if num > max {
					max = num
				}
			}
		}
	}
	return max
}

func (h *DataHandler) evaluateCondition(data map[string]any, condition string) bool {
	// Simple condition evaluation - can be extended
	parts := strings.Fields(condition)
	if len(parts) >= 3 {
		field := parts[0]
		operator := parts[1]
		value := parts[2]

		if fieldVal, ok := data[field]; ok {
			switch operator {
			case "==", "=":
				return fmt.Sprintf("%v", fieldVal) == value
			case "!=":
				return fmt.Sprintf("%v", fieldVal) != value
			case ">":
				if fieldNum, ok := toFloat64(fieldVal); ok {
					if valueNum, ok := toFloat64(value); ok {
						return fieldNum > valueNum
					}
				}
			case "<":
				if fieldNum, ok := toFloat64(fieldVal); ok {
					if valueNum, ok := toFloat64(value); ok {
						return fieldNum < valueNum
					}
				}
			}
		}
	}
	return false
}

func (h *DataHandler) castValue(val any, targetType string) any {
	switch targetType {
	case "string":
		return fmt.Sprintf("%v", val)
	case "int":
		if num, ok := toFloat64(val); ok {
			return int(num)
		}
		return val
	case "float":
		if num, ok := toFloat64(val); ok {
			return num
		}
		return val
	case "bool":
		if str, ok := val.(string); ok {
			return str == "true" || str == "1"
		}
		return val
	default:
		return val
	}
}

func (h *DataHandler) validateField(val any, rules map[string]any) map[string]any {
	result := map[string]any{
		"valid":  true,
		"errors": []string{},
	}

	var errors []string

	// Required validation
	if required, ok := rules["required"].(bool); ok && required {
		if val == nil || val == "" {
			errors = append(errors, "field is required")
		}
	}

	// Type validation
	if expectedType, ok := rules["type"].(string); ok {
		if !h.validateType(val, expectedType) {
			errors = append(errors, fmt.Sprintf("expected type %s", expectedType))
		}
	}

	// Range validation for numbers
	if minVal, ok := rules["min"]; ok {
		if num, numOk := toFloat64(val); numOk {
			if minNum, minOk := toFloat64(minVal); minOk {
				if num < minNum {
					errors = append(errors, fmt.Sprintf("value must be >= %v", minVal))
				}
			}
		}
	}

	if len(errors) > 0 {
		result["valid"] = false
		result["errors"] = errors
	}

	return result
}

func (h *DataHandler) validateType(val any, expectedType string) bool {
	actualType := reflect.TypeOf(val).String()
	switch expectedType {
	case "string":
		return actualType == "string"
	case "int", "integer":
		return actualType == "int" || actualType == "float64"
	case "float", "number":
		return actualType == "float64" || actualType == "int"
	case "bool", "boolean":
		return actualType == "bool"
	default:
		return true
	}
}

func (h *DataHandler) normalizeValue(val any, normType string) any {
	switch normType {
	case "lowercase":
		if str, ok := val.(string); ok {
			return strings.ToLower(str)
		}
	case "uppercase":
		if str, ok := val.(string); ok {
			return strings.ToUpper(str)
		}
	case "trim":
		if str, ok := val.(string); ok {
			return strings.TrimSpace(str)
		}
	}
	return val
}

func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// Configuration getters
func (h *DataHandler) getSortField() string {
	if field, ok := h.Payload.Data["sort_field"].(string); ok {
		return field
	}
	return ""
}

func (h *DataHandler) getSortOrder() string {
	if order, ok := h.Payload.Data["sort_order"].(string); ok {
		return order
	}
	return "asc"
}

func (h *DataHandler) getDedupeFields() []string {
	// Support both []string and []any for dedupe_fields
	if fields, ok := h.Payload.Data["dedupe_fields"].([]string); ok {
		return fields
	}
	if fields, ok := h.Payload.Data["dedupe_fields"].([]any); ok {
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

func (h *DataHandler) getCalculations() map[string]map[string]any {
	result := make(map[string]map[string]any)
	if calc, ok := h.Payload.Data["calculations"].(map[string]any); ok {
		for key, value := range calc {
			if calcMap, ok := value.(map[string]any); ok {
				result[key] = calcMap
			}
		}
	}
	return result
}

func (h *DataHandler) getConditions() map[string]map[string]any {
	result := make(map[string]map[string]any)
	if cond, ok := h.Payload.Data["conditions"].(map[string]any); ok {
		for key, value := range cond {
			if condMap, ok := value.(map[string]any); ok {
				result[key] = condMap
			}
		}
	}
	return result
}

func (h *DataHandler) getCastConfig() map[string]string {
	result := make(map[string]string)
	if cast, ok := h.Payload.Data["cast"].(map[string]any); ok {
		for key, value := range cast {
			if str, ok := value.(string); ok {
				result[key] = str
			}
		}
	}
	return result
}

func (h *DataHandler) getValidationRules() map[string]map[string]any {
	result := make(map[string]map[string]any)
	if rules, ok := h.Payload.Data["validation_rules"].(map[string]any); ok {
		for key, value := range rules {
			if ruleMap, ok := value.(map[string]any); ok {
				result[key] = ruleMap
			}
		}
	}
	return result
}

func (h *DataHandler) getTargetFields() []string {
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

func (h *DataHandler) getNormalizationType() string {
	if normType, ok := h.Payload.Data["normalize_type"].(string); ok {
		return normType
	}
	return "trim"
}

func (h *DataHandler) getPivotField() string {
	if field, ok := h.Payload.Data["pivot_field"].(string); ok {
		return field
	}
	return ""
}

func (h *DataHandler) getValueField() string {
	if field, ok := h.Payload.Data["value_field"].(string); ok {
		return field
	}
	return ""
}

func (h *DataHandler) getUnpivotFields() []string {
	if fields, ok := h.Payload.Data["unpivot_fields"].([]any); ok {
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

func NewDataHandler(id string) *DataHandler {
	return &DataHandler{
		Operation: dag.Operation{ID: id, Key: "data:transform", Type: dag.Function, Tags: []string{"data", "transformation", "misc"}},
	}
}
