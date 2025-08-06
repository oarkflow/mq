package handlers

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// FlattenHandler flattens array of objects to a single object or performs other flattening operations
type FlattenHandler struct {
	dag.Operation
	FlattenType string               `json:"flatten_type"` // "array_to_object", "nested_object", "key_value_pairs"
	SourceField string               `json:"source_field"` // field containing data to flatten
	TargetField string               `json:"target_field"` // field to store flattened result
	Config      FlattenConfiguration `json:"config"`       // configuration for flattening
}

type FlattenConfiguration struct {
	// For array_to_object flattening
	KeyField   string `json:"key_field"`   // field to use as key
	ValueField string `json:"value_field"` // field to use as value
	TypeField  string `json:"type_field"`  // optional field for value type conversion

	// For nested_object flattening
	Separator   string `json:"separator"`    // separator for nested keys (default: ".")
	MaxDepth    int    `json:"max_depth"`    // maximum depth to flatten (-1 for unlimited)
	Prefix      string `json:"prefix"`       // prefix for flattened keys
	SkipArrays  bool   `json:"skip_arrays"`  // skip array flattening
	SkipObjects bool   `json:"skip_objects"` // skip object flattening

	// For key_value_pairs flattening
	PairSeparator string `json:"pair_separator"` // separator between key-value pairs
	KVSeparator   string `json:"kv_separator"`   // separator between key and value

	// General options
	OverwriteExisting bool `json:"overwrite_existing"` // overwrite existing keys
	PreserveTypes     bool `json:"preserve_types"`     // preserve original data types
}

func (f *FlattenHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Get source data
	sourceData, exists := data[f.SourceField]
	if !exists {
		return mq.Result{Error: fmt.Errorf("source field '%s' not found", f.SourceField), Ctx: ctx}
	}

	var result any
	var err error

	switch f.FlattenType {
	case "array_to_object":
		result, err = f.flattenArrayToObject(sourceData)
	case "nested_object":
		result, err = f.flattenNestedObject(sourceData)
	case "key_value_pairs":
		result, err = f.flattenKeyValuePairs(sourceData)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported flatten type: %s", f.FlattenType), Ctx: ctx}
	}

	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	// Set target field
	targetField := f.TargetField
	if targetField == "" {
		targetField = f.SourceField // overwrite source if no target specified
	}
	data[targetField] = result

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (f *FlattenHandler) flattenArrayToObject(data any) (map[string]any, error) {
	// Convert to slice of maps
	items, err := f.convertToSliceOfMaps(data)
	if err != nil {
		return nil, err
	}

	result := make(map[string]any)

	for _, item := range items {
		key, keyExists := item[f.Config.KeyField]
		if !keyExists {
			continue
		}

		value, valueExists := item[f.Config.ValueField]
		if !valueExists {
			continue
		}

		keyStr := fmt.Sprintf("%v", key)

		// Handle type conversion if type field is specified
		if f.Config.TypeField != "" {
			if typeValue, typeExists := item[f.Config.TypeField]; typeExists {
				convertedValue, err := f.convertValueByType(value, fmt.Sprintf("%v", typeValue))
				if err == nil {
					value = convertedValue
				}
			}
		}

		// Check for overwrites
		if !f.Config.OverwriteExisting {
			if _, exists := result[keyStr]; exists {
				continue // skip if key already exists
			}
		}

		result[keyStr] = value
	}

	return result, nil
}

func (f *FlattenHandler) flattenNestedObject(data any) (map[string]any, error) {
	result := make(map[string]any)
	f.flattenRecursive(data, "", result, 0)
	return result, nil
}

func (f *FlattenHandler) flattenRecursive(data any, prefix string, result map[string]any, depth int) {
	// Check depth limit
	if f.Config.MaxDepth > 0 && depth >= f.Config.MaxDepth {
		key := prefix
		if key == "" {
			key = "root"
		}
		result[key] = data
		return
	}

	rv := reflect.ValueOf(data)
	if !rv.IsValid() {
		return
	}

	switch rv.Kind() {
	case reflect.Map:
		if f.Config.SkipObjects {
			result[prefix] = data
			return
		}

		if rv.Type().Key().Kind() == reflect.String {
			for _, key := range rv.MapKeys() {
				keyStr := key.String()
				value := rv.MapIndex(key).Interface()

				newPrefix := keyStr
				if prefix != "" {
					separator := f.Config.Separator
					if separator == "" {
						separator = "."
					}
					newPrefix = prefix + separator + keyStr
				}
				if f.Config.Prefix != "" {
					newPrefix = f.Config.Prefix + newPrefix
				}

				f.flattenRecursive(value, newPrefix, result, depth+1)
			}
		} else {
			result[prefix] = data
		}

	case reflect.Slice, reflect.Array:
		if f.Config.SkipArrays {
			result[prefix] = data
			return
		}

		for i := 0; i < rv.Len(); i++ {
			value := rv.Index(i).Interface()
			newPrefix := fmt.Sprintf("%s[%d]", prefix, i)
			f.flattenRecursive(value, newPrefix, result, depth+1)
		}

	default:
		if prefix == "" {
			prefix = "value"
		}
		result[prefix] = data
	}
}

func (f *FlattenHandler) flattenKeyValuePairs(data any) (map[string]any, error) {
	str := fmt.Sprintf("%v", data)
	result := make(map[string]any)

	pairSeparator := f.Config.PairSeparator
	if pairSeparator == "" {
		pairSeparator = ","
	}

	kvSeparator := f.Config.KVSeparator
	if kvSeparator == "" {
		kvSeparator = "="
	}

	pairs := strings.Split(str, pairSeparator)
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		kv := strings.SplitN(pair, kvSeparator, 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		// Check for overwrites
		if !f.Config.OverwriteExisting {
			if _, exists := result[key]; exists {
				continue
			}
		}

		// Try to preserve types if requested
		if f.Config.PreserveTypes {
			if convertedValue := f.tryConvertType(value); convertedValue != nil {
				result[key] = convertedValue
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}

	return result, nil
}

func (f *FlattenHandler) convertToSliceOfMaps(data any) ([]map[string]any, error) {
	rv := reflect.ValueOf(data)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("data must be an array or slice")
	}

	var items []map[string]any
	for i := 0; i < rv.Len(); i++ {
		item := rv.Index(i).Interface()

		// Convert item to map[string]any
		itemMap := make(map[string]any)
		itemBytes, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal item at index %d: %v", i, err)
		}

		if err := json.Unmarshal(itemBytes, &itemMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal item at index %d: %v", i, err)
		}

		items = append(items, itemMap)
	}

	return items, nil
}

func (f *FlattenHandler) convertValueByType(value any, typeStr string) (any, error) {
	valueStr := fmt.Sprintf("%v", value)

	switch strings.ToLower(typeStr) {
	case "string", "str":
		return valueStr, nil
	case "int", "integer":
		if i, err := fmt.Sscanf(valueStr, "%d", new(int)); err == nil && i == 1 {
			var result int
			fmt.Sscanf(valueStr, "%d", &result)
			return result, nil
		}
	case "float", "double", "number":
		if i, err := fmt.Sscanf(valueStr, "%f", new(float64)); err == nil && i == 1 {
			var result float64
			fmt.Sscanf(valueStr, "%f", &result)
			return result, nil
		}
	case "bool", "boolean":
		lower := strings.ToLower(valueStr)
		return lower == "true" || lower == "yes" || lower == "1" || lower == "on", nil
	case "json":
		var result any
		if err := json.Unmarshal([]byte(valueStr), &result); err == nil {
			return result, nil
		}
	}

	return value, fmt.Errorf("unable to convert to type %s", typeStr)
}

func (f *FlattenHandler) tryConvertType(value string) any {
	// Try int
	var intVal int
	if n, err := fmt.Sscanf(value, "%d", &intVal); err == nil && n == 1 {
		return intVal
	}

	// Try float
	var floatVal float64
	if n, err := fmt.Sscanf(value, "%f", &floatVal); err == nil && n == 1 {
		return floatVal
	}

	// Try bool
	lower := strings.ToLower(value)
	if lower == "true" || lower == "false" {
		return lower == "true"
	}

	// Try JSON
	var jsonVal any
	if err := json.Unmarshal([]byte(value), &jsonVal); err == nil {
		return jsonVal
	}

	return nil // Unable to convert, return nil to use original string
}

// Factory functions
func NewArrayToObjectFlattener(id, sourceField, targetField, keyField, valueField string, config FlattenConfiguration) *FlattenHandler {
	config.KeyField = keyField
	config.ValueField = valueField

	return &FlattenHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "flatten_array_to_object",
			Type: dag.Function,
			Tags: []string{"data", "flatten", "array", "object"},
		},
		FlattenType: "array_to_object",
		SourceField: sourceField,
		TargetField: targetField,
		Config:      config,
	}
}

func NewNestedObjectFlattener(id, sourceField, targetField string, config FlattenConfiguration) *FlattenHandler {
	return &FlattenHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "flatten_nested_object",
			Type: dag.Function,
			Tags: []string{"data", "flatten", "nested", "object"},
		},
		FlattenType: "nested_object",
		SourceField: sourceField,
		TargetField: targetField,
		Config:      config,
	}
}

func NewKeyValuePairsFlattener(id, sourceField, targetField string, config FlattenConfiguration) *FlattenHandler {
	return &FlattenHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "flatten_key_value_pairs",
			Type: dag.Function,
			Tags: []string{"data", "flatten", "key-value", "string"},
		},
		FlattenType: "key_value_pairs",
		SourceField: sourceField,
		TargetField: targetField,
		Config:      config,
	}
}
