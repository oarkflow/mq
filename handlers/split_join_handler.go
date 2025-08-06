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

// SplitJoinHandler handles splitting strings into arrays and joining arrays into strings
type SplitJoinHandler struct {
	dag.Operation
	OpType      string           `json:"op_type"`      // "split" or "join"
	SourceField string           `json:"source_field"` // field to operate on
	TargetField string           `json:"target_field"` // field to store result
	Delimiter   string           `json:"delimiter"`    // delimiter for split/join
	Options     SplitJoinOptions `json:"options"`
}

type SplitJoinOptions struct {
	TrimSpaces      bool   `json:"trim_spaces"`      // trim spaces from elements (split only)
	RemoveEmpty     bool   `json:"remove_empty"`     // remove empty elements (split only)
	MaxSplit        int    `json:"max_split"`        // maximum number of splits (-1 for unlimited)
	UseRegex        bool   `json:"use_regex"`        // treat delimiter as regex pattern (split only)
	CaseInsensitive bool   `json:"case_insensitive"` // case insensitive regex (split only)
	Prefix          string `json:"prefix"`           // prefix for joined string (join only)
	Suffix          string `json:"suffix"`           // suffix for joined string (join only)
}

func (s *SplitJoinHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Get source value
	sourceValue, exists := data[s.SourceField]
	if !exists {
		return mq.Result{Error: fmt.Errorf("source field '%s' not found", s.SourceField), Ctx: ctx}
	}

	var result any
	var err error

	switch s.OpType {
	case "split":
		result, err = s.performSplit(sourceValue)
	case "join":
		result, err = s.performJoin(sourceValue)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported operation: %s", s.OpType), Ctx: ctx}
	}

	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	// Set target field
	targetField := s.TargetField
	if targetField == "" {
		targetField = s.SourceField // overwrite source if no target specified
	}
	data[targetField] = result

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (s *SplitJoinHandler) performSplit(value any) ([]string, error) {
	// Convert value to string
	str := fmt.Sprintf("%v", value)

	var parts []string

	if s.Options.UseRegex {
		// Use regex for splitting
		flags := ""
		if s.Options.CaseInsensitive {
			flags = "(?i)"
		}
		pattern := flags + s.Delimiter

		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern '%s': %v", pattern, err)
		}

		if s.Options.MaxSplit > 0 {
			parts = re.Split(str, s.Options.MaxSplit+1)
		} else {
			parts = re.Split(str, -1)
		}
	} else {
		// Use simple string splitting
		if s.Options.MaxSplit > 0 {
			parts = strings.SplitN(str, s.Delimiter, s.Options.MaxSplit+1)
		} else {
			parts = strings.Split(str, s.Delimiter)
		}
	}

	// Process the parts based on options
	var processedParts []string
	for _, part := range parts {
		if s.Options.TrimSpaces {
			part = strings.TrimSpace(part)
		}

		if s.Options.RemoveEmpty && part == "" {
			continue
		}

		processedParts = append(processedParts, part)
	}

	return processedParts, nil
}

func (s *SplitJoinHandler) performJoin(value any) (string, error) {
	// Convert value to slice of strings
	parts, err := s.convertToStringSlice(value)
	if err != nil {
		return "", err
	}

	// Join the parts
	joined := strings.Join(parts, s.Delimiter)

	// Add prefix/suffix if specified
	if s.Options.Prefix != "" {
		joined = s.Options.Prefix + joined
	}
	if s.Options.Suffix != "" {
		joined = joined + s.Options.Suffix
	}

	return joined, nil
}

func (s *SplitJoinHandler) convertToStringSlice(value any) ([]string, error) {
	rv := reflect.ValueOf(value)

	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("value must be an array or slice for join operation")
	}

	var parts []string
	for i := 0; i < rv.Len(); i++ {
		element := rv.Index(i).Interface()
		parts = append(parts, fmt.Sprintf("%v", element))
	}

	return parts, nil
}

// Advanced split/join handler for complex operations
type AdvancedSplitJoinHandler struct {
	dag.Operation
	Operations []SplitJoinOperation `json:"operations"` // chain of split/join operations
}

type SplitJoinOperation struct {
	Type        string           `json:"type"`         // "split" or "join"
	SourceField string           `json:"source_field"` // field to operate on
	TargetField string           `json:"target_field"` // field to store result
	Delimiter   string           `json:"delimiter"`    // delimiter for operation
	Options     SplitJoinOptions `json:"options"`      // operation options
}

func (a *AdvancedSplitJoinHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Execute operations in sequence
	for i, op := range a.Operations {
		handler := &SplitJoinHandler{
			Operation: dag.Operation{
				ID:   fmt.Sprintf("%s_op_%d", a.ID, i),
				Key:  "temp_split_join",
				Type: dag.Function,
				Tags: []string{"data", "temp"},
			},
			OpType:      op.Type,
			SourceField: op.SourceField,
			TargetField: op.TargetField,
			Delimiter:   op.Delimiter,
			Options:     op.Options,
		}

		// Create a temporary task for this operation
		tempData, _ := json.Marshal(data)
		tempTask := &mq.Task{Payload: tempData}

		result := handler.ProcessTask(ctx, tempTask)
		if result.Error != nil {
			return mq.Result{Error: fmt.Errorf("operation %d failed: %v", i+1, result.Error), Ctx: ctx}
		}

		// Update data with the result
		if err := json.Unmarshal(result.Payload, &data); err != nil {
			return mq.Result{Error: fmt.Errorf("failed to unmarshal result from operation %d: %v", i+1, err), Ctx: ctx}
		}
	}

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

// Factory functions
func NewSplitHandler(id, sourceField, targetField, delimiter string, options SplitJoinOptions) *SplitJoinHandler {
	return &SplitJoinHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "split_string",
			Type: dag.Function,
			Tags: []string{"data", "string", "split"},
		},
		OpType:      "split",
		SourceField: sourceField,
		TargetField: targetField,
		Delimiter:   delimiter,
		Options:     options,
	}
}

func NewJoinHandler(id, sourceField, targetField, delimiter string, options SplitJoinOptions) *SplitJoinHandler {
	return &SplitJoinHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "join_array",
			Type: dag.Function,
			Tags: []string{"data", "array", "join"},
		},
		OpType:      "join",
		SourceField: sourceField,
		TargetField: targetField,
		Delimiter:   delimiter,
		Options:     options,
	}
}

func NewAdvancedSplitJoinHandler(id string, operations []SplitJoinOperation) *AdvancedSplitJoinHandler {
	return &AdvancedSplitJoinHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "advanced_split_join",
			Type: dag.Function,
			Tags: []string{"data", "string", "array", "advanced"},
		},
		Operations: operations,
	}
}
