package handlers

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// FieldManipulationHandler handles various field operations on data
type FieldManipulationHandler struct {
	dag.Operation
	Operations []FieldOperation `json:"operations"` // list of field operations to perform
}

type FieldOperation struct {
	Type   string               `json:"type"`   // "filter", "add", "remove", "rename", "copy", "transform"
	Config FieldOperationConfig `json:"config"` // operation-specific configuration
}

type FieldOperationConfig struct {
	// Common fields
	Fields        []string `json:"fields"`         // fields to operate on
	Pattern       string   `json:"pattern"`        // regex pattern for field matching
	CaseSensitive bool     `json:"case_sensitive"` // case sensitive pattern matching

	// Filter operation
	IncludeOnly []string `json:"include_only"` // only include these fields
	Exclude     []string `json:"exclude"`      // exclude these fields
	KeepNulls   bool     `json:"keep_nulls"`   // keep fields with null values
	KeepEmpty   bool     `json:"keep_empty"`   // keep fields with empty values

	// Add operation
	NewFields    map[string]any `json:"new_fields"`    // fields to add with their values
	DefaultValue any            `json:"default_value"` // default value for new fields

	// Rename operation
	FieldMapping map[string]string `json:"field_mapping"` // old field name -> new field name

	// Copy operation
	CopyMapping   map[string]string `json:"copy_mapping"`   // source field -> target field
	OverwriteCopy bool              `json:"overwrite_copy"` // overwrite target if exists

	// Transform operation
	Transformation  string         `json:"transformation"`   // transformation type
	TransformConfig map[string]any `json:"transform_config"` // transformation configuration
}

func (f *FieldManipulationHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Apply operations in sequence
	for i, operation := range f.Operations {
		var err error

		switch operation.Type {
		case "filter":
			data, err = f.filterFields(data, operation.Config)
		case "add":
			data, err = f.addFields(data, operation.Config)
		case "remove":
			data, err = f.removeFields(data, operation.Config)
		case "rename":
			data, err = f.renameFields(data, operation.Config)
		case "copy":
			data, err = f.copyFields(data, operation.Config)
		case "transform":
			data, err = f.transformFields(data, operation.Config)
		default:
			return mq.Result{Error: fmt.Errorf("unsupported operation type: %s", operation.Type), Ctx: ctx}
		}

		if err != nil {
			return mq.Result{Error: fmt.Errorf("operation %d (%s) failed: %v", i+1, operation.Type, err), Ctx: ctx}
		}
	}

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (f *FieldManipulationHandler) filterFields(data map[string]any, config FieldOperationConfig) (map[string]any, error) {
	result := make(map[string]any)

	// If include_only is specified, only include those fields
	if len(config.IncludeOnly) > 0 {
		for _, field := range config.IncludeOnly {
			if value, exists := data[field]; exists {
				if f.shouldKeepValue(value, config) {
					result[field] = value
				}
			}
		}
		return result, nil
	}

	// Otherwise, include all except excluded fields
	excludeSet := make(map[string]bool)
	for _, field := range config.Exclude {
		excludeSet[field] = true
	}

	// Compile regex pattern if provided
	var pattern *regexp.Regexp
	if config.Pattern != "" {
		flags := ""
		if !config.CaseSensitive {
			flags = "(?i)"
		}
		var err error
		pattern, err = regexp.Compile(flags + config.Pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %v", err)
		}
	}

	for field, value := range data {
		// Check if field should be excluded
		if excludeSet[field] {
			continue
		}

		// Check pattern matching
		if pattern != nil && !pattern.MatchString(field) {
			continue
		}

		// Check value conditions
		if f.shouldKeepValue(value, config) {
			result[field] = value
		}
	}

	return result, nil
}

func (f *FieldManipulationHandler) shouldKeepValue(value any, config FieldOperationConfig) bool {
	if value == nil {
		return config.KeepNulls
	}

	// Check for empty values
	if f.isEmpty(value) {
		return config.KeepEmpty
	}

	return true
}

func (f *FieldManipulationHandler) isEmpty(value any) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.String:
		return rv.String() == ""
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() == 0
	default:
		return false
	}
}

func (f *FieldManipulationHandler) addFields(data map[string]any, config FieldOperationConfig) (map[string]any, error) {
	result := make(map[string]any)

	// Copy existing data
	for k, v := range data {
		result[k] = v
	}

	// Add new fields from new_fields map
	for field, value := range config.NewFields {
		result[field] = value
	}

	// Add fields from fields list with default value
	for _, field := range config.Fields {
		if _, exists := result[field]; !exists {
			result[field] = config.DefaultValue
		}
	}

	return result, nil
}

func (f *FieldManipulationHandler) removeFields(data map[string]any, config FieldOperationConfig) (map[string]any, error) {
	result := make(map[string]any)

	// Create set of fields to remove
	removeSet := make(map[string]bool)
	for _, field := range config.Fields {
		removeSet[field] = true
	}

	// Compile regex pattern if provided
	var pattern *regexp.Regexp
	if config.Pattern != "" {
		flags := ""
		if !config.CaseSensitive {
			flags = "(?i)"
		}
		var err error
		pattern, err = regexp.Compile(flags + config.Pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %v", err)
		}
	}

	// Copy fields that should not be removed
	for field, value := range data {
		shouldRemove := removeSet[field]

		// Check pattern matching
		if !shouldRemove && pattern != nil {
			shouldRemove = pattern.MatchString(field)
		}

		if !shouldRemove {
			result[field] = value
		}
	}

	return result, nil
}

func (f *FieldManipulationHandler) renameFields(data map[string]any, config FieldOperationConfig) (map[string]any, error) {
	result := make(map[string]any)

	// Copy and rename fields
	for field, value := range data {
		newName := field
		if mappedName, exists := config.FieldMapping[field]; exists {
			newName = mappedName
		}
		result[newName] = value
	}

	return result, nil
}

func (f *FieldManipulationHandler) copyFields(data map[string]any, config FieldOperationConfig) (map[string]any, error) {
	result := make(map[string]any)

	// Copy existing data
	for k, v := range data {
		result[k] = v
	}

	// Copy fields based on mapping
	for sourceField, targetField := range config.CopyMapping {
		if value, exists := data[sourceField]; exists {
			// Check if target already exists and overwrite is not allowed
			if _, targetExists := result[targetField]; targetExists && !config.OverwriteCopy {
				continue
			}
			result[targetField] = value
		}
	}

	return result, nil
}

func (f *FieldManipulationHandler) transformFields(data map[string]any, config FieldOperationConfig) (map[string]any, error) {
	result := make(map[string]any)

	// Copy existing data
	for k, v := range data {
		result[k] = v
	}

	// Apply transformations to specified fields
	for _, field := range config.Fields {
		if value, exists := result[field]; exists {
			transformedValue, err := f.applyTransformation(value, config.Transformation, config.TransformConfig)
			if err != nil {
				return nil, fmt.Errorf("transformation failed for field '%s': %v", field, err)
			}
			result[field] = transformedValue
		}
	}

	return result, nil
}

func (f *FieldManipulationHandler) applyTransformation(value any, transformationType string, config map[string]any) (any, error) {
	switch transformationType {
	case "uppercase":
		return strings.ToUpper(fmt.Sprintf("%v", value)), nil

	case "lowercase":
		return strings.ToLower(fmt.Sprintf("%v", value)), nil

	case "title":
		return strings.Title(fmt.Sprintf("%v", value)), nil

	case "trim":
		return strings.TrimSpace(fmt.Sprintf("%v", value)), nil

	case "prefix":
		prefix, _ := config["prefix"].(string)
		return prefix + fmt.Sprintf("%v", value), nil

	case "suffix":
		suffix, _ := config["suffix"].(string)
		return fmt.Sprintf("%v", value) + suffix, nil

	case "replace":
		old, _ := config["old"].(string)
		new, _ := config["new"].(string)
		return strings.ReplaceAll(fmt.Sprintf("%v", value), old, new), nil

	case "regex_replace":
		pattern, _ := config["pattern"].(string)
		replacement, _ := config["replacement"].(string)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %v", err)
		}
		return re.ReplaceAllString(fmt.Sprintf("%v", value), replacement), nil

	case "multiply":
		if multiplier, ok := config["multiplier"].(float64); ok {
			if num := f.toFloat64(value); num != 0 {
				return num * multiplier, nil
			}
		}
		return value, nil

	case "add":
		if addend, ok := config["addend"].(float64); ok {
			if num := f.toFloat64(value); num != 0 {
				return num + addend, nil
			}
		}
		return value, nil

	case "absolute":
		if num := f.toFloat64(value); num != 0 {
			if num < 0 {
				return -num, nil
			}
			return num, nil
		}
		return value, nil

	case "default_if_empty":
		defaultVal := config["default"]
		if f.isEmpty(value) {
			return defaultVal, nil
		}
		return value, nil

	default:
		return nil, fmt.Errorf("unsupported transformation type: %s", transformationType)
	}
}

func (f *FieldManipulationHandler) toFloat64(value any) float64 {
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

// Factory functions for common operations
func NewFieldFilter(id string, includeOnly, exclude []string, options FieldOperationConfig) *FieldManipulationHandler {
	options.IncludeOnly = includeOnly
	options.Exclude = exclude

	return &FieldManipulationHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "filter_fields",
			Type: dag.Function,
			Tags: []string{"data", "fields", "filter"},
		},
		Operations: []FieldOperation{
			{
				Type:   "filter",
				Config: options,
			},
		},
	}
}

func NewFieldAdder(id string, newFields map[string]any, defaultValue any) *FieldManipulationHandler {
	return &FieldManipulationHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "add_fields",
			Type: dag.Function,
			Tags: []string{"data", "fields", "add"},
		},
		Operations: []FieldOperation{
			{
				Type: "add",
				Config: FieldOperationConfig{
					NewFields:    newFields,
					DefaultValue: defaultValue,
				},
			},
		},
	}
}

func NewFieldRemover(id string, fieldsToRemove []string, pattern string) *FieldManipulationHandler {
	return &FieldManipulationHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "remove_fields",
			Type: dag.Function,
			Tags: []string{"data", "fields", "remove"},
		},
		Operations: []FieldOperation{
			{
				Type: "remove",
				Config: FieldOperationConfig{
					Fields:  fieldsToRemove,
					Pattern: pattern,
				},
			},
		},
	}
}

func NewFieldRenamer(id string, fieldMapping map[string]string) *FieldManipulationHandler {
	return &FieldManipulationHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "rename_fields",
			Type: dag.Function,
			Tags: []string{"data", "fields", "rename"},
		},
		Operations: []FieldOperation{
			{
				Type: "rename",
				Config: FieldOperationConfig{
					FieldMapping: fieldMapping,
				},
			},
		},
	}
}

func NewFieldTransformer(id string, fields []string, transformation string, transformConfig map[string]any) *FieldManipulationHandler {
	return &FieldManipulationHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "transform_fields",
			Type: dag.Function,
			Tags: []string{"data", "fields", "transform"},
		},
		Operations: []FieldOperation{
			{
				Type: "transform",
				Config: FieldOperationConfig{
					Fields:          fields,
					Transformation:  transformation,
					TransformConfig: transformConfig,
				},
			},
		},
	}
}

func NewAdvancedFieldManipulator(id string, operations []FieldOperation) *FieldManipulationHandler {
	return &FieldManipulationHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "advanced_field_manipulation",
			Type: dag.Function,
			Tags: []string{"data", "fields", "advanced"},
		},
		Operations: operations,
	}
}
