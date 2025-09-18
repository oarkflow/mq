package handlers

import (
	"context"
	"fmt"
	"sort"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// GroupHandler handles data grouping operations with aggregation
type GroupHandler struct {
	dag.Operation
}

func (h *GroupHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data, err := dag.UnmarshalPayload[map[string]any](ctx, task.Payload)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %w", err), Ctx: ctx}
	}

	// Extract the data array
	dataArray, ok := data["data"].([]interface{})
	if !ok {
		return mq.Result{Error: fmt.Errorf("expected 'data' field to be an array"), Ctx: ctx}
	}

	groupByFields := h.getGroupByFields()
	if len(groupByFields) == 0 {
		return mq.Result{Error: fmt.Errorf("group_by fields not specified"), Ctx: ctx}
	}

	aggregations := h.getAggregations()
	result := h.groupData(dataArray, groupByFields, aggregations)

	// Update the data with grouped result
	data["data"] = result
	data["grouped"] = true
	data["group_count"] = len(result)

	resultPayload, err := json.Marshal(data)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to marshal result: %w", err), Ctx: ctx}
	}

	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

func (h *GroupHandler) groupData(dataArray []interface{}, groupByFields []string, aggregations map[string]string) []map[string]any {
	groups := make(map[string][]map[string]any)

	// Group data by specified fields
	for _, item := range dataArray {
		record, ok := item.(map[string]any)
		if !ok {
			continue
		}

		// Create group key
		groupKey := h.createGroupKey(record, groupByFields)
		groups[groupKey] = append(groups[groupKey], record)
	}

	// Apply aggregations
	var result []map[string]any
	for _, records := range groups {
		groupResult := make(map[string]any)

		// Add group by fields to result
		if len(records) > 0 {
			for _, field := range groupByFields {
				groupResult[field] = records[0][field]
			}
		}

		// Apply aggregations
		for field, aggType := range aggregations {
			switch aggType {
			case "count":
				groupResult[field+"_count"] = len(records)
			case "sum":
				groupResult[field+"_sum"] = h.sumField(records, field)
			case "avg", "average":
				sum := h.sumField(records, field)
				if count := len(records); count > 0 {
					groupResult[field+"_avg"] = sum / float64(count)
				}
			case "min":
				groupResult[field+"_min"] = h.minField(records, field)
			case "max":
				groupResult[field+"_max"] = h.maxField(records, field)
			case "first":
				if len(records) > 0 {
					groupResult[field+"_first"] = records[0][field]
				}
			case "last":
				if len(records) > 0 {
					groupResult[field+"_last"] = records[len(records)-1][field]
				}
			case "concat":
				groupResult[field+"_concat"] = h.concatField(records, field)
			case "unique":
				groupResult[field+"_unique"] = h.uniqueField(records, field)
			}
		}

		// Add record count
		groupResult["_record_count"] = len(records)

		result = append(result, groupResult)
	}

	// Sort results for consistent output
	sort.Slice(result, func(i, j int) bool {
		for _, field := range groupByFields {
			if fmt.Sprintf("%v", result[i][field]) < fmt.Sprintf("%v", result[j][field]) {
				return true
			} else if fmt.Sprintf("%v", result[i][field]) > fmt.Sprintf("%v", result[j][field]) {
				return false
			}
		}
		return false
	})

	return result
}

func (h *GroupHandler) createGroupKey(record map[string]any, fields []string) string {
	var keyParts []string
	for _, field := range fields {
		keyParts = append(keyParts, fmt.Sprintf("%v", record[field]))
	}
	return fmt.Sprintf("%v", keyParts)
}

func (h *GroupHandler) sumField(records []map[string]any, field string) float64 {
	var sum float64
	for _, record := range records {
		if val, ok := record[field]; ok {
			switch v := val.(type) {
			case float64:
				sum += v
			case int:
				sum += float64(v)
			case int64:
				sum += float64(v)
			}
		}
	}
	return sum
}

func (h *GroupHandler) minField(records []map[string]any, field string) interface{} {
	if len(records) == 0 {
		return nil
	}

	var min interface{}
	for _, record := range records {
		if val, ok := record[field]; ok {
			if min == nil {
				min = val
			} else {
				if h.compareValues(val, min) < 0 {
					min = val
				}
			}
		}
	}
	return min
}

func (h *GroupHandler) maxField(records []map[string]any, field string) interface{} {
	if len(records) == 0 {
		return nil
	}

	var max interface{}
	for _, record := range records {
		if val, ok := record[field]; ok {
			if max == nil {
				max = val
			} else {
				if h.compareValues(val, max) > 0 {
					max = val
				}
			}
		}
	}
	return max
}

func (h *GroupHandler) concatField(records []map[string]any, field string) string {
	var values []string
	separator := h.getConcatSeparator()

	for _, record := range records {
		if val, ok := record[field]; ok && val != nil {
			values = append(values, fmt.Sprintf("%v", val))
		}
	}

	result := ""
	for i, val := range values {
		if i > 0 {
			result += separator
		}
		result += val
	}
	return result
}

func (h *GroupHandler) uniqueField(records []map[string]any, field string) []interface{} {
	seen := make(map[string]bool)
	var unique []interface{}

	for _, record := range records {
		if val, ok := record[field]; ok && val != nil {
			key := fmt.Sprintf("%v", val)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, val)
			}
		}
	}

	return unique
}

func (h *GroupHandler) compareValues(a, b interface{}) int {
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func (h *GroupHandler) getGroupByFields() []string {
	if fields, ok := h.Payload.Data["group_by"].([]string); ok {
		return fields
	}
	if fields, ok := h.Payload.Data["group_by"].([]interface{}); ok {
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

func (h *GroupHandler) getAggregations() map[string]string {
	result := make(map[string]string)
	if aggs, ok := h.Payload.Data["aggregations"].(map[string]interface{}); ok {
		for field, aggType := range aggs {
			if str, ok := aggType.(string); ok {
				result[field] = str
			}
		}
	}
	return result
}

func (h *GroupHandler) getConcatSeparator() string {
	if sep, ok := h.Payload.Data["concat_separator"].(string); ok {
		return sep
	}
	return ", " // Default separator
}

func NewGroupHandler(id string) *GroupHandler {
	return &GroupHandler{
		Operation: dag.Operation{ID: id, Key: "group", Type: dag.Function, Tags: []string{"data", "aggregation"}},
	}
}
