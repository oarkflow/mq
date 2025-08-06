package handlers

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// FormatHandler handles data formatting operations
type FormatHandler struct {
	dag.Operation
}

func (h *FormatHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %w", err)}
	}

	formatType, ok := h.Payload.Data["format_type"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("format_type not specified")}
	}

	var result map[string]any
	switch formatType {
	case "string":
		result = h.formatToString(data)
	case "number":
		result = h.formatToNumber(data)
	case "date":
		result = h.formatDate(data)
	case "currency":
		result = h.formatCurrency(data)
	case "uppercase":
		result = h.formatUppercase(data)
	case "lowercase":
		result = h.formatLowercase(data)
	case "capitalize":
		result = h.formatCapitalize(data)
	case "trim":
		result = h.formatTrim(data)
	default:
		return mq.Result{Error: fmt.Errorf("unsupported format_type: %s", formatType)}
	}

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to marshal result: %w", err)}
	}

	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

func (h *FormatHandler) formatToString(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields(data)

	for key, value := range data {
		if len(fields) == 0 || contains(fields, key) {
			result[key] = fmt.Sprintf("%v", value)
		} else {
			result[key] = value
		}
	}
	return result
}

func (h *FormatHandler) formatToNumber(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields(data)

	for key, value := range data {
		if len(fields) == 0 || contains(fields, key) {
			if str, ok := value.(string); ok {
				if num, err := strconv.ParseFloat(str, 64); err == nil {
					result[key] = num
				} else {
					result[key] = value // Keep original if conversion fails
				}
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}
	return result
}

func (h *FormatHandler) formatDate(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields(data)
	dateFormat := h.getDateFormat()

	for key, value := range data {
		if len(fields) == 0 || contains(fields, key) {
			if str, ok := value.(string); ok {
				if t, err := time.Parse(time.RFC3339, str); err == nil {
					result[key] = t.Format(dateFormat)
				} else if t, err := time.Parse("2006-01-02", str); err == nil {
					result[key] = t.Format(dateFormat)
				} else {
					result[key] = value // Keep original if parsing fails
				}
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}
	return result
}

func (h *FormatHandler) formatCurrency(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields(data)
	currency := h.getCurrency()

	for key, value := range data {
		if len(fields) == 0 || contains(fields, key) {
			if num, ok := value.(float64); ok {
				result[key] = fmt.Sprintf("%s%.2f", currency, num)
			} else if str, ok := value.(string); ok {
				if num, err := strconv.ParseFloat(str, 64); err == nil {
					result[key] = fmt.Sprintf("%s%.2f", currency, num)
				} else {
					result[key] = value
				}
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}
	return result
}

func (h *FormatHandler) formatUppercase(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields(data)

	for key, value := range data {
		if len(fields) == 0 || contains(fields, key) {
			if str, ok := value.(string); ok {
				result[key] = strings.ToUpper(str)
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}
	return result
}

func (h *FormatHandler) formatLowercase(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields(data)

	for key, value := range data {
		if len(fields) == 0 || contains(fields, key) {
			if str, ok := value.(string); ok {
				result[key] = strings.ToLower(str)
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}
	return result
}

func (h *FormatHandler) formatCapitalize(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields(data)

	for key, value := range data {
		if len(fields) == 0 || contains(fields, key) {
			if str, ok := value.(string); ok {
				result[key] = strings.Title(strings.ToLower(str))
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}
	return result
}

func (h *FormatHandler) formatTrim(data map[string]any) map[string]any {
	result := make(map[string]any)
	fields := h.getTargetFields(data)

	for key, value := range data {
		if len(fields) == 0 || contains(fields, key) {
			if str, ok := value.(string); ok {
				result[key] = strings.TrimSpace(str)
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}
	return result
}

func (h *FormatHandler) getTargetFields(data map[string]any) []string {
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

func (h *FormatHandler) getDateFormat() string {
	if format, ok := h.Payload.Data["date_format"].(string); ok {
		return format
	}
	return "2006-01-02" // Default date format
}

func (h *FormatHandler) getCurrency() string {
	if currency, ok := h.Payload.Data["currency"].(string); ok {
		return currency
	}
	return "$" // Default currency symbol
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func NewFormatHandler(id string) *FormatHandler {
	return &FormatHandler{
		Operation: dag.Operation{ID: id, Key: "format", Type: dag.Function, Tags: []string{"data", "transformation"}},
	}
}
