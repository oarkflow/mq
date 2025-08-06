package handlers

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// FormatHandler handles various data formatting operations
type FormatHandler struct {
	dag.Operation
	FormatType   string            `json:"format_type"`   // date, number, string, currency, etc.
	SourceField  string            `json:"source_field"`  // field to format
	TargetField  string            `json:"target_field"`  // field to store formatted result
	FormatConfig map[string]string `json:"format_config"` // format-specific configuration
}

func (f *FormatHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Get source value
	sourceValue, exists := data[f.SourceField]
	if !exists {
		return mq.Result{Error: fmt.Errorf("source field '%s' not found", f.SourceField), Ctx: ctx}
	}

	// Format based on type
	var formattedValue any
	var err error

	switch f.FormatType {
	case "date":
		formattedValue, err = f.formatDate(sourceValue)
	case "number":
		formattedValue, err = f.formatNumber(sourceValue)
	case "currency":
		formattedValue, err = f.formatCurrency(sourceValue)
	case "string":
		formattedValue, err = f.formatString(sourceValue)
	case "boolean":
		formattedValue, err = f.formatBoolean(sourceValue)
	case "array":
		formattedValue, err = f.formatArray(sourceValue)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported format type: %s", f.FormatType), Ctx: ctx}
	}

	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	// Set target field
	targetField := f.TargetField
	if targetField == "" {
		targetField = f.SourceField // overwrite source if no target specified
	}
	data[targetField] = formattedValue

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (f *FormatHandler) formatDate(value any) (string, error) {
	var t time.Time
	var err error

	switch v := value.(type) {
	case string:
		// Try parsing various date formats
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02",
			"01/02/2006",
			"02-01-2006",
			"2006/01/02",
		}

		for _, format := range formats {
			if t, err = time.Parse(format, v); err == nil {
				break
			}
		}
		if err != nil {
			return "", fmt.Errorf("unable to parse date string: %s", v)
		}
	case time.Time:
		t = v
	case int64:
		t = time.Unix(v, 0)
	case float64:
		t = time.Unix(int64(v), 0)
	default:
		return "", fmt.Errorf("unsupported date type: %T", value)
	}

	// Get output format from config
	outputFormat := f.FormatConfig["output_format"]
	if outputFormat == "" {
		outputFormat = "2006-01-02 15:04:05" // default format
	}

	return t.Format(outputFormat), nil
}

func (f *FormatHandler) formatNumber(value any) (string, error) {
	var num float64
	var err error

	switch v := value.(type) {
	case string:
		num, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return "", fmt.Errorf("unable to parse number string: %s", v)
		}
	case int:
		num = float64(v)
	case int32:
		num = float64(v)
	case int64:
		num = float64(v)
	case float32:
		num = float64(v)
	case float64:
		num = v
	default:
		return "", fmt.Errorf("unsupported number type: %T", value)
	}

	// Get precision from config
	precision := 2
	if p, exists := f.FormatConfig["precision"]; exists {
		if parsed, err := strconv.Atoi(p); err == nil {
			precision = parsed
		}
	}

	// Get format style
	style := f.FormatConfig["style"]
	switch style {
	case "scientific":
		return fmt.Sprintf("%e", num), nil
	case "percentage":
		return fmt.Sprintf("%."+strconv.Itoa(precision)+"f%%", num*100), nil
	default:
		return fmt.Sprintf("%."+strconv.Itoa(precision)+"f", num), nil
	}
}

func (f *FormatHandler) formatCurrency(value any) (string, error) {
	num, err := f.formatNumber(value)
	if err != nil {
		return "", err
	}

	symbol := f.FormatConfig["symbol"]
	if symbol == "" {
		symbol = "$" // default currency symbol
	}

	position := f.FormatConfig["position"]
	if position == "suffix" {
		return num + " " + symbol, nil
	}
	return symbol + num, nil
}

func (f *FormatHandler) formatString(value any) (string, error) {
	str := fmt.Sprintf("%v", value)

	operation := f.FormatConfig["operation"]
	switch operation {
	case "uppercase":
		return strings.ToUpper(str), nil
	case "lowercase":
		return strings.ToLower(str), nil
	case "title":
		return strings.Title(str), nil
	case "trim":
		return strings.TrimSpace(str), nil
	case "truncate":
		if lengthStr, exists := f.FormatConfig["length"]; exists {
			if length, err := strconv.Atoi(lengthStr); err == nil && len(str) > length {
				return str[:length] + "...", nil
			}
		}
		return str, nil
	default:
		return str, nil
	}
}

func (f *FormatHandler) formatBoolean(value any) (string, error) {
	var boolVal bool

	switch v := value.(type) {
	case bool:
		boolVal = v
	case string:
		lower := strings.ToLower(v)
		boolVal = lower == "true" || lower == "yes" || lower == "1" || lower == "on"
	case int, int32, int64:
		boolVal = reflect.ValueOf(v).Int() != 0
	case float32, float64:
		boolVal = reflect.ValueOf(v).Float() != 0
	default:
		return "", fmt.Errorf("unsupported boolean type: %T", value)
	}

	trueValue := f.FormatConfig["true_value"]
	falseValue := f.FormatConfig["false_value"]

	if trueValue == "" {
		trueValue = "true"
	}
	if falseValue == "" {
		falseValue = "false"
	}

	if boolVal {
		return trueValue, nil
	}
	return falseValue, nil
}

func (f *FormatHandler) formatArray(value any) (string, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return "", fmt.Errorf("value is not an array or slice")
	}

	separator := f.FormatConfig["separator"]
	if separator == "" {
		separator = ", "
	}

	var elements []string
	for i := 0; i < rv.Len(); i++ {
		elements = append(elements, fmt.Sprintf("%v", rv.Index(i).Interface()))
	}

	return strings.Join(elements, separator), nil
}

// Factory functions for different format types
func NewDateFormatter(id, sourceField, targetField string, config map[string]string) *FormatHandler {
	return &FormatHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "format_date",
			Type: dag.Function,
			Tags: []string{"data", "format", "date"},
		},
		FormatType:   "date",
		SourceField:  sourceField,
		TargetField:  targetField,
		FormatConfig: config,
	}
}

func NewNumberFormatter(id, sourceField, targetField string, config map[string]string) *FormatHandler {
	return &FormatHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "format_number",
			Type: dag.Function,
			Tags: []string{"data", "format", "number"},
		},
		FormatType:   "number",
		SourceField:  sourceField,
		TargetField:  targetField,
		FormatConfig: config,
	}
}

func NewCurrencyFormatter(id, sourceField, targetField string, config map[string]string) *FormatHandler {
	return &FormatHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "format_currency",
			Type: dag.Function,
			Tags: []string{"data", "format", "currency"},
		},
		FormatType:   "currency",
		SourceField:  sourceField,
		TargetField:  targetField,
		FormatConfig: config,
	}
}

func NewStringFormatter(id, sourceField, targetField string, config map[string]string) *FormatHandler {
	return &FormatHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "format_string",
			Type: dag.Function,
			Tags: []string{"data", "format", "string"},
		},
		FormatType:   "string",
		SourceField:  sourceField,
		TargetField:  targetField,
		FormatConfig: config,
	}
}
