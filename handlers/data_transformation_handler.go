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

// DataTransformationHandler provides comprehensive data transformation capabilities
type DataTransformationHandler struct {
	dag.Operation
	Transformations []DataTransformation `json:"transformations"` // list of transformations to apply
}

type DataTransformation struct {
	Name        string              `json:"name"`         // transformation name/identifier
	Type        string              `json:"type"`         // transformation type
	SourceField string              `json:"source_field"` // source field (can be empty for data-wide operations)
	TargetField string              `json:"target_field"` // target field (can be empty to overwrite source)
	Config      map[string]any      `json:"config"`       // transformation configuration
	Condition   *TransformCondition `json:"condition"`    // optional condition for when to apply
}

type TransformCondition struct {
	Field    string `json:"field"`    // field to check
	Operator string `json:"operator"` // eq, ne, gt, lt, ge, le, contains, regex
	Value    any    `json:"value"`    // value to compare against
}

func (d *DataTransformationHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Apply transformations in sequence
	for i, transformation := range d.Transformations {
		// Check condition if specified
		if transformation.Condition != nil {
			if !d.evaluateCondition(data, transformation.Condition) {
				continue // skip this transformation
			}
		}

		var err error
		data, err = d.applyTransformation(data, transformation)
		if err != nil {
			return mq.Result{Error: fmt.Errorf("transformation %d (%s) failed: %v", i+1, transformation.Name, err), Ctx: ctx}
		}
	}

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (d *DataTransformationHandler) evaluateCondition(data map[string]any, condition *TransformCondition) bool {
	fieldValue, exists := data[condition.Field]
	if !exists {
		return false
	}

	switch condition.Operator {
	case "eq":
		return fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", condition.Value)
	case "ne":
		return fmt.Sprintf("%v", fieldValue) != fmt.Sprintf("%v", condition.Value)
	case "gt":
		return d.compareNumeric(fieldValue, condition.Value) > 0
	case "lt":
		return d.compareNumeric(fieldValue, condition.Value) < 0
	case "ge":
		return d.compareNumeric(fieldValue, condition.Value) >= 0
	case "le":
		return d.compareNumeric(fieldValue, condition.Value) <= 0
	case "contains":
		return strings.Contains(fmt.Sprintf("%v", fieldValue), fmt.Sprintf("%v", condition.Value))
	case "regex":
		// Basic regex support - in production, use proper regex library
		return strings.Contains(fmt.Sprintf("%v", fieldValue), fmt.Sprintf("%v", condition.Value))
	default:
		return false
	}
}

func (d *DataTransformationHandler) compareNumeric(a, b any) int {
	aFloat := d.toFloat64(a)
	bFloat := d.toFloat64(b)

	if aFloat < bFloat {
		return -1
	} else if aFloat > bFloat {
		return 1
	}
	return 0
}

func (d *DataTransformationHandler) applyTransformation(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	switch transformation.Type {
	case "normalize":
		return d.normalizeData(data, transformation)
	case "aggregate":
		return d.aggregateData(data, transformation)
	case "pivot":
		return d.pivotData(data, transformation)
	case "unpivot":
		return d.unpivotData(data, transformation)
	case "calculate":
		return d.calculateField(data, transformation)
	case "lookup":
		return d.lookupTransform(data, transformation)
	case "bucket":
		return d.bucketize(data, transformation)
	case "rank":
		return d.rankData(data, transformation)
	case "window":
		return d.windowFunction(data, transformation)
	case "encode":
		return d.encodeData(data, transformation)
	case "decode":
		return d.decodeData(data, transformation)
	case "validate":
		return d.validateData(data, transformation)
	default:
		return nil, fmt.Errorf("unsupported transformation type: %s", transformation.Type)
	}
}

func (d *DataTransformationHandler) normalizeData(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	sourceValue := data[transformation.SourceField]
	normalizeType, _ := transformation.Config["type"].(string)

	var normalized any
	var err error

	switch normalizeType {
	case "min_max":
		normalized, err = d.minMaxNormalize(sourceValue, transformation.Config)
	case "z_score":
		normalized, err = d.zScoreNormalize(sourceValue, transformation.Config)
	case "unit_vector":
		normalized, err = d.unitVectorNormalize(sourceValue, transformation.Config)
	default:
		return nil, fmt.Errorf("unsupported normalization type: %s", normalizeType)
	}

	if err != nil {
		return nil, err
	}

	targetField := transformation.TargetField
	if targetField == "" {
		targetField = transformation.SourceField
	}

	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}
	result[targetField] = normalized

	return result, nil
}

func (d *DataTransformationHandler) minMaxNormalize(value any, config map[string]any) (float64, error) {
	num := d.toFloat64(value)
	min, _ := config["min"].(float64)
	max, _ := config["max"].(float64)

	if max == min {
		return 0, nil
	}

	return (num - min) / (max - min), nil
}

func (d *DataTransformationHandler) zScoreNormalize(value any, config map[string]any) (float64, error) {
	num := d.toFloat64(value)
	mean, _ := config["mean"].(float64)
	stdDev, _ := config["std_dev"].(float64)

	if stdDev == 0 {
		return 0, nil
	}

	return (num - mean) / stdDev, nil
}

func (d *DataTransformationHandler) unitVectorNormalize(value any, config map[string]any) (float64, error) {
	num := d.toFloat64(value)
	magnitude, _ := config["magnitude"].(float64)

	if magnitude == 0 {
		return 0, nil
	}

	return num / magnitude, nil
}

func (d *DataTransformationHandler) calculateField(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	expression, _ := transformation.Config["expression"].(string)

	// Simple expression evaluator - in production, use a proper expression library
	result, err := d.evaluateExpression(expression, data)
	if err != nil {
		return nil, err
	}

	targetField := transformation.TargetField
	if targetField == "" {
		return nil, fmt.Errorf("target field is required for calculate transformation")
	}

	resultData := make(map[string]any)
	for k, v := range data {
		resultData[k] = v
	}
	resultData[targetField] = result

	return resultData, nil
}

func (d *DataTransformationHandler) evaluateExpression(expression string, data map[string]any) (any, error) {
	// Basic expression evaluation - replace with proper expression evaluator
	// This is a simplified implementation for common cases

	expression = strings.TrimSpace(expression)

	// Handle simple field references
	if value, exists := data[expression]; exists {
		return value, nil
	}

	// Handle simple arithmetic operations
	if strings.Contains(expression, "+") {
		parts := strings.Split(expression, "+")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			leftVal := d.getValueOrNumber(left, data)
			rightVal := d.getValueOrNumber(right, data)

			return d.toFloat64(leftVal) + d.toFloat64(rightVal), nil
		}
	}

	if strings.Contains(expression, "-") {
		parts := strings.Split(expression, "-")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			leftVal := d.getValueOrNumber(left, data)
			rightVal := d.getValueOrNumber(right, data)

			return d.toFloat64(leftVal) - d.toFloat64(rightVal), nil
		}
	}

	if strings.Contains(expression, "*") {
		parts := strings.Split(expression, "*")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			leftVal := d.getValueOrNumber(left, data)
			rightVal := d.getValueOrNumber(right, data)

			return d.toFloat64(leftVal) * d.toFloat64(rightVal), nil
		}
	}

	if strings.Contains(expression, "/") {
		parts := strings.Split(expression, "/")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			leftVal := d.getValueOrNumber(left, data)
			rightVal := d.toFloat64(d.getValueOrNumber(right, data))

			if rightVal == 0 {
				return nil, fmt.Errorf("division by zero")
			}

			return d.toFloat64(leftVal) / rightVal, nil
		}
	}

	return nil, fmt.Errorf("unable to evaluate expression: %s", expression)
}

func (d *DataTransformationHandler) getValueOrNumber(str string, data map[string]any) any {
	// Check if it's a field reference
	if value, exists := data[str]; exists {
		return value
	}

	// Try to parse as number
	if num, err := strconv.ParseFloat(str, 64); err == nil {
		return num
	}

	// Return as string
	return str
}

func (d *DataTransformationHandler) bucketize(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	sourceValue := data[transformation.SourceField]
	buckets, _ := transformation.Config["buckets"].([]any)
	labels, _ := transformation.Config["labels"].([]any)

	num := d.toFloat64(sourceValue)

	// Find the appropriate bucket
	var bucketIndex int = -1
	for i, bucket := range buckets {
		if bucketVal := d.toFloat64(bucket); num <= bucketVal {
			bucketIndex = i
			break
		}
	}

	var result any
	if bucketIndex >= 0 && bucketIndex < len(labels) {
		result = labels[bucketIndex]
	} else {
		result = "out_of_range"
	}

	targetField := transformation.TargetField
	if targetField == "" {
		targetField = transformation.SourceField
	}

	resultData := make(map[string]any)
	for k, v := range data {
		resultData[k] = v
	}
	resultData[targetField] = result

	return resultData, nil
}

func (d *DataTransformationHandler) encodeData(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	sourceValue := data[transformation.SourceField]
	encodingType, _ := transformation.Config["type"].(string)

	var encoded any
	var err error

	switch encodingType {
	case "one_hot":
		encoded, err = d.oneHotEncode(sourceValue, transformation.Config)
	case "label":
		encoded, err = d.labelEncode(sourceValue, transformation.Config)
	case "ordinal":
		encoded, err = d.ordinalEncode(sourceValue, transformation.Config)
	default:
		return nil, fmt.Errorf("unsupported encoding type: %s", encodingType)
	}

	if err != nil {
		return nil, err
	}

	targetField := transformation.TargetField
	if targetField == "" {
		targetField = transformation.SourceField
	}

	result := make(map[string]any)
	for k, v := range data {
		result[k] = v
	}
	result[targetField] = encoded

	return result, nil
}

func (d *DataTransformationHandler) oneHotEncode(value any, config map[string]any) (map[string]any, error) {
	categories, _ := config["categories"].([]any)
	valueStr := fmt.Sprintf("%v", value)

	result := make(map[string]any)
	for _, category := range categories {
		categoryStr := fmt.Sprintf("%v", category)
		if valueStr == categoryStr {
			result[categoryStr] = 1
		} else {
			result[categoryStr] = 0
		}
	}

	return result, nil
}

func (d *DataTransformationHandler) labelEncode(value any, config map[string]any) (int, error) {
	mapping, _ := config["mapping"].(map[string]any)
	valueStr := fmt.Sprintf("%v", value)

	if encoded, exists := mapping[valueStr]; exists {
		return int(d.toFloat64(encoded)), nil
	}

	return -1, fmt.Errorf("value '%s' not found in encoding mapping", valueStr)
}

func (d *DataTransformationHandler) ordinalEncode(value any, config map[string]any) (int, error) {
	order, _ := config["order"].([]any)
	valueStr := fmt.Sprintf("%v", value)

	for i, item := range order {
		if fmt.Sprintf("%v", item) == valueStr {
			return i, nil
		}
	}

	return -1, fmt.Errorf("value '%s' not found in ordinal order", valueStr)
}

func (d *DataTransformationHandler) aggregateData(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	// This is a simplified version - for complex aggregations, use GroupingHandler
	aggregationType, _ := transformation.Config["type"].(string)
	sourceField := transformation.SourceField

	// Assume source field contains an array of values
	sourceValue, exists := data[sourceField]
	if !exists {
		return nil, fmt.Errorf("source field '%s' not found", sourceField)
	}

	values := d.extractNumbers(sourceValue)
	if len(values) == 0 {
		return nil, fmt.Errorf("no numeric values found in source field")
	}

	var result float64

	switch aggregationType {
	case "sum":
		for _, v := range values {
			result += v
		}
	case "avg", "mean":
		for _, v := range values {
			result += v
		}
		result /= float64(len(values))
	case "min":
		result = values[0]
		for _, v := range values {
			if v < result {
				result = v
			}
		}
	case "max":
		result = values[0]
		for _, v := range values {
			if v > result {
				result = v
			}
		}
	case "std":
		// Calculate standard deviation
		mean := 0.0
		for _, v := range values {
			mean += v
		}
		mean /= float64(len(values))

		variance := 0.0
		for _, v := range values {
			variance += math.Pow(v-mean, 2)
		}
		variance /= float64(len(values))
		result = math.Sqrt(variance)
	default:
		return nil, fmt.Errorf("unsupported aggregation type: %s", aggregationType)
	}

	targetField := transformation.TargetField
	if targetField == "" {
		targetField = sourceField
	}

	resultData := make(map[string]any)
	for k, v := range data {
		resultData[k] = v
	}
	resultData[targetField] = result

	return resultData, nil
}

func (d *DataTransformationHandler) extractNumbers(value any) []float64 {
	var numbers []float64

	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		for i := 0; i < rv.Len(); i++ {
			if num := d.toFloat64(rv.Index(i).Interface()); num != 0 {
				numbers = append(numbers, num)
			}
		}
	} else {
		if num := d.toFloat64(value); num != 0 {
			numbers = append(numbers, num)
		}
	}

	return numbers
}

func (d *DataTransformationHandler) rankData(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	// For ranking, we need the data to contain an array of items
	arrayField, _ := transformation.Config["array_field"].(string)
	rankField := transformation.SourceField

	arrayData, exists := data[arrayField]
	if !exists {
		return nil, fmt.Errorf("array field '%s' not found", arrayField)
	}

	// Convert to slice and extract values for ranking
	rv := reflect.ValueOf(arrayData)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("array field must contain an array")
	}

	type rankItem struct {
		index int
		value float64
	}

	var items []rankItem
	for i := 0; i < rv.Len(); i++ {
		item := rv.Index(i).Interface()
		if itemMap, ok := item.(map[string]any); ok {
			if val, exists := itemMap[rankField]; exists {
				items = append(items, rankItem{
					index: i,
					value: d.toFloat64(val),
				})
			}
		}
	}

	// Sort by value
	sort.Slice(items, func(i, j int) bool {
		return items[i].value > items[j].value // descending order
	})

	// Assign ranks
	ranks := make(map[int]int)
	for rank, item := range items {
		ranks[item.index] = rank + 1
	}

	// Update the original data with ranks
	targetField := transformation.TargetField
	if targetField == "" {
		targetField = rankField + "_rank"
	}

	for i := 0; i < rv.Len(); i++ {
		item := rv.Index(i).Interface()
		if itemMap, ok := item.(map[string]any); ok {
			itemMap[targetField] = ranks[i]
		}
	}

	return data, nil
}

func (d *DataTransformationHandler) pivotData(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	// Pivot transformation implementation
	pivotField, _ := transformation.Config["pivot_field"].(string)
	valueField, _ := transformation.Config["value_field"].(string)

	if pivotField == "" || valueField == "" {
		return nil, fmt.Errorf("pivot_field and value_field are required")
	}

	result := make(map[string]any)
	for key, value := range data {
		if key == pivotField {
			result[fmt.Sprintf("%v", value)] = data[valueField]
		}
	}

	return result, nil
}

func (d *DataTransformationHandler) unpivotData(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	// Unpivot transformation implementation
	unpivotFields, _ := transformation.Config["fields"].([]string)
	if len(unpivotFields) == 0 {
		return nil, fmt.Errorf("fields for unpivoting are required")
	}

	result := make(map[string]any)
	for _, field := range unpivotFields {
		if value, exists := data[field]; exists {
			result[field] = value
		}
	}

	return result, nil
}

func (d *DataTransformationHandler) lookupTransform(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	// Lookup transformation implementation
	lookupTable, _ := transformation.Config["lookup_table"].(map[string]any)
	lookupKey, _ := transformation.Config["lookup_key"].(string)

	if lookupTable == nil || lookupKey == "" {
		return nil, fmt.Errorf("lookup_table and lookup_key are required")
	}

	lookupValue := data[lookupKey]
	if result, exists := lookupTable[fmt.Sprintf("%v", lookupValue)]; exists {
		return map[string]any{lookupKey: result}, nil
	}

	return nil, fmt.Errorf("lookup value not found")
}

func (d *DataTransformationHandler) windowFunction(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	// Window function transformation implementation
	windowField, _ := transformation.Config["window_field"].(string)
	operation, _ := transformation.Config["operation"].(string)

	if windowField == "" || operation == "" {
		return nil, fmt.Errorf("window_field and operation are required")
	}

	values := d.extractNumbers(data[windowField])
	if len(values) == 0 {
		return nil, fmt.Errorf("no numeric values found in window_field")
	}

	var result float64
	switch operation {
	case "sum":
		for _, v := range values {
			result += v
		}
	case "avg":
		for _, v := range values {
			result += v
		}
		result /= float64(len(values))
	default:
		return nil, fmt.Errorf("unsupported window operation: %s", operation)
	}

	return map[string]any{windowField: result}, nil
}

func (d *DataTransformationHandler) decodeData(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	// Data decoding implementation
	encodingType, _ := transformation.Config["type"].(string)

	if encodingType == "" {
		return nil, fmt.Errorf("encoding type is required")
	}

	sourceValue := data[transformation.SourceField]
	var decoded any
	var err error

	switch encodingType {
	case "base64":
		decoded, err = d.decodeBase64(fmt.Sprintf("%v", sourceValue))
	case "hex":
		decoded, err = d.decodeHex(fmt.Sprintf("%v", sourceValue))
	default:
		return nil, fmt.Errorf("unsupported decoding type: %s", encodingType)
	}

	if err != nil {
		return nil, err
	}

	return map[string]any{transformation.TargetField: decoded}, nil
}

func (d *DataTransformationHandler) decodeBase64(value string) (string, error) {
	decoded, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", decoded), nil
}

func (d *DataTransformationHandler) decodeHex(value string) (string, error) {
	decoded, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", decoded), nil
}

func (d *DataTransformationHandler) validateData(data map[string]any, transformation DataTransformation) (map[string]any, error) {
	// Data validation implementation
	validationRules, _ := transformation.Config["rules"].([]map[string]any)

	if len(validationRules) == 0 {
		return nil, fmt.Errorf("validation rules are required")
	}

	for _, rule := range validationRules {
		field, _ := rule["field"].(string)
		operator, _ := rule["operator"].(string)
		value := rule["value"]

		if !d.evaluateCondition(data, &TransformCondition{Field: field, Operator: operator, Value: value}) {
			return nil, fmt.Errorf("validation failed for field: %s", field)
		}
	}

	return data, nil
}

func (d *DataTransformationHandler) toFloat64(value any) float64 {
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
		if num, err := strconv.ParseFloat(v, 64); err == nil {
			return num
		}
	}
	return 0
}

// Factory function
func NewDataTransformationHandler(id string, transformations []DataTransformation) *DataTransformationHandler {
	return &DataTransformationHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "data_transformation",
			Type: dag.Function,
			Tags: []string{"data", "transformation", "advanced"},
		},
		Transformations: transformations,
	}
}
