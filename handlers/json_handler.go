package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// JSONHandler handles JSON parsing and stringification operations
type JSONHandler struct {
	dag.Operation
	OperationType string      `json:"operation_type"` // "parse" or "stringify"
	SourceField   string      `json:"source_field"`   // field containing data to process
	TargetField   string      `json:"target_field"`   // field to store result
	Options       JSONOptions `json:"options"`        // processing options
}

type JSONOptions struct {
	Pretty         bool   `json:"pretty"`           // pretty print JSON (stringify only)
	Indent         string `json:"indent"`           // indentation string (stringify only)
	EscapeHTML     bool   `json:"escape_html"`      // escape HTML in JSON strings (stringify only)
	ValidateOnly   bool   `json:"validate_only"`    // only validate, don't parse (parse only)
	ErrorOnInvalid bool   `json:"error_on_invalid"` // return error if JSON is invalid
	DefaultOnError any    `json:"default_on_error"` // default value to use if parsing fails
	StrictMode     bool   `json:"strict_mode"`      // strict JSON parsing
	AllowComments  bool   `json:"allow_comments"`   // allow comments in JSON (parse only)
	AllowTrailing  bool   `json:"allow_trailing"`   // allow trailing commas (parse only)
}

func (j *JSONHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Get source value
	sourceValue, exists := data[j.SourceField]
	if !exists {
		return mq.Result{Error: fmt.Errorf("source field '%s' not found", j.SourceField), Ctx: ctx}
	}

	var result any
	var err error

	switch j.OperationType {
	case "parse":
		result, err = j.parseJSON(sourceValue)
	case "stringify":
		result, err = j.stringifyJSON(sourceValue)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported operation type: %s", j.OperationType), Ctx: ctx}
	}

	if err != nil {
		if j.Options.ErrorOnInvalid {
			return mq.Result{Error: err, Ctx: ctx}
		}
		// Use default value if specified
		if j.Options.DefaultOnError != nil {
			result = j.Options.DefaultOnError
		} else {
			result = sourceValue // keep original value
		}
	}

	// Set target field
	targetField := j.TargetField
	if targetField == "" {
		targetField = j.SourceField // overwrite source if no target specified
	}
	data[targetField] = result

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (j *JSONHandler) parseJSON(value any) (any, error) {
	// Convert value to string
	jsonStr := fmt.Sprintf("%v", value)

	// Validate only if requested
	if j.Options.ValidateOnly {
		var temp any
		err := json.Unmarshal([]byte(jsonStr), &temp)
		if err != nil {
			return false, fmt.Errorf("invalid JSON: %v", err)
		}
		return true, nil
	}

	// Preprocess if needed
	if j.Options.AllowComments {
		jsonStr = j.removeComments(jsonStr)
	}

	if j.Options.AllowTrailing {
		jsonStr = j.removeTrailingCommas(jsonStr)
	}

	// Parse JSON
	var result any
	err := json.Unmarshal([]byte(jsonStr), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %v", err)
	}

	return result, nil
}

func (j *JSONHandler) stringifyJSON(value any) (string, error) {
	var result []byte
	var err error

	if j.Options.Pretty {
		indent := j.Options.Indent
		if indent == "" {
			indent = "  " // default indentation
		}
		result, err = json.MarshalIndent(value, "", indent)
	} else {
		result, err = json.Marshal(value)
	}

	if err != nil {
		return "", fmt.Errorf("failed to stringify JSON: %v", err)
	}

	return string(result), nil
}

func (j *JSONHandler) removeComments(jsonStr string) string {
	lines := strings.Split(jsonStr, "\n")
	var cleanLines []string

	for _, line := range lines {
		// Remove single-line comments
		if commentIndex := strings.Index(line, "//"); commentIndex != -1 {
			line = line[:commentIndex]
		}
		cleanLines = append(cleanLines, line)
	}

	result := strings.Join(cleanLines, "\n")

	// Remove multi-line comments (basic implementation)
	for {
		start := strings.Index(result, "/*")
		if start == -1 {
			break
		}
		end := strings.Index(result[start:], "*/")
		if end == -1 {
			break
		}
		result = result[:start] + result[start+end+2:]
	}

	return result
}

func (j *JSONHandler) removeTrailingCommas(jsonStr string) string {
	// Basic implementation - remove commas before closing brackets/braces
	jsonStr = strings.ReplaceAll(jsonStr, ",}", "}")
	jsonStr = strings.ReplaceAll(jsonStr, ",]", "]")
	return jsonStr
}

// Advanced JSON handler for complex operations
type AdvancedJSONHandler struct {
	dag.Operation
	Operations []JSONOperation `json:"operations"` // chain of JSON operations
}

type JSONOperation struct {
	Type        string      `json:"type"`         // "parse", "stringify", "validate", "extract", "merge"
	SourceField string      `json:"source_field"` // field to operate on
	TargetField string      `json:"target_field"` // field to store result
	Options     JSONOptions `json:"options"`      // operation options
	Path        string      `json:"path"`         // JSON path for extraction (extract only)
	MergeWith   string      `json:"merge_with"`   // field to merge with (merge only)
}

func (a *AdvancedJSONHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Execute operations in sequence
	for i, op := range a.Operations {
		var result any
		var err error

		switch op.Type {
		case "parse", "stringify":
			handler := &JSONHandler{
				OperationType: op.Type,
				SourceField:   op.SourceField,
				TargetField:   op.TargetField,
				Options:       op.Options,
			}

			tempData, _ := json.Marshal(data)
			tempTask := &mq.Task{Payload: tempData}

			handlerResult := handler.ProcessTask(ctx, tempTask)
			if handlerResult.Error != nil {
				return mq.Result{Error: fmt.Errorf("operation %d failed: %v", i+1, handlerResult.Error), Ctx: ctx}
			}

			if err := json.Unmarshal(handlerResult.Payload, &data); err != nil {
				return mq.Result{Error: fmt.Errorf("failed to unmarshal result from operation %d: %v", i+1, err), Ctx: ctx}
			}
			continue

		case "validate":
			result, err = a.validateJSON(data[op.SourceField])
		case "extract":
			result, err = a.extractFromJSON(data[op.SourceField], op.Path)
		case "merge":
			result, err = a.mergeJSON(data[op.SourceField], data[op.MergeWith])
		default:
			return mq.Result{Error: fmt.Errorf("unsupported operation type: %s", op.Type), Ctx: ctx}
		}

		if err != nil {
			if op.Options.ErrorOnInvalid {
				return mq.Result{Error: fmt.Errorf("operation %d failed: %v", i+1, err), Ctx: ctx}
			}
			result = op.Options.DefaultOnError
		}

		// Set target field
		targetField := op.TargetField
		if targetField == "" {
			targetField = op.SourceField
		}
		data[targetField] = result
	}

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (a *AdvancedJSONHandler) validateJSON(value any) (bool, error) {
	jsonStr := fmt.Sprintf("%v", value)
	var temp any
	err := json.Unmarshal([]byte(jsonStr), &temp)
	return err == nil, err
}

func (a *AdvancedJSONHandler) extractFromJSON(value any, path string) (any, error) {
	// Basic JSON path extraction (simplified implementation)
	// For production use, consider using a proper JSON path library

	var jsonData any
	if str, ok := value.(string); ok {
		if err := json.Unmarshal([]byte(str), &jsonData); err != nil {
			return nil, fmt.Errorf("invalid JSON: %v", err)
		}
	} else {
		jsonData = value
	}

	// Split path and navigate
	parts := strings.Split(strings.Trim(path, "."), ".")
	current := jsonData

	for _, part := range parts {
		if part == "" {
			continue
		}

		switch v := current.(type) {
		case map[string]any:
			current = v[part]
		default:
			return nil, fmt.Errorf("cannot navigate path '%s' at part '%s'", path, part)
		}
	}

	return current, nil
}

func (a *AdvancedJSONHandler) mergeJSON(value1, value2 any) (any, error) {
	// Convert both values to maps if they're JSON strings
	var map1, map2 map[string]any

	if str, ok := value1.(string); ok {
		if err := json.Unmarshal([]byte(str), &map1); err != nil {
			return nil, fmt.Errorf("invalid JSON in first value: %v", err)
		}
	} else if m, ok := value1.(map[string]any); ok {
		map1 = m
	} else {
		return nil, fmt.Errorf("first value is not a JSON object")
	}

	if str, ok := value2.(string); ok {
		if err := json.Unmarshal([]byte(str), &map2); err != nil {
			return nil, fmt.Errorf("invalid JSON in second value: %v", err)
		}
	} else if m, ok := value2.(map[string]any); ok {
		map2 = m
	} else {
		return nil, fmt.Errorf("second value is not a JSON object")
	}

	// Merge maps
	result := make(map[string]any)
	for k, v := range map1 {
		result[k] = v
	}
	for k, v := range map2 {
		result[k] = v // overwrites if key exists
	}

	return result, nil
}

// Factory functions
func NewJSONParser(id, sourceField, targetField string, options JSONOptions) *JSONHandler {
	return &JSONHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "json_parse",
			Type: dag.Function,
			Tags: []string{"data", "json", "parse"},
		},
		OperationType: "parse",
		SourceField:   sourceField,
		TargetField:   targetField,
		Options:       options,
	}
}

func NewJSONStringifier(id, sourceField, targetField string, options JSONOptions) *JSONHandler {
	return &JSONHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "json_stringify",
			Type: dag.Function,
			Tags: []string{"data", "json", "stringify"},
		},
		OperationType: "stringify",
		SourceField:   sourceField,
		TargetField:   targetField,
		Options:       options,
	}
}

func NewAdvancedJSONHandler(id string, operations []JSONOperation) *AdvancedJSONHandler {
	return &AdvancedJSONHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "advanced_json",
			Type: dag.Function,
			Tags: []string{"data", "json", "advanced"},
		},
		Operations: operations,
	}
}
