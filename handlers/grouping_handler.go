package handlers

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// GroupingHandler groups data by specified fields and applies aggregations
type GroupingHandler struct {
	dag.Operation
	GroupByFields []string            `json:"group_by_fields"` // fields to group by
	Aggregations  []AggregationConfig `json:"aggregations"`    // aggregation configurations
	SourceField   string              `json:"source_field"`    // field containing array to group
	TargetField   string              `json:"target_field"`    // field to store grouped result
	Options       GroupingOptions     `json:"options"`         // additional options
}

type AggregationConfig struct {
	Field     string `json:"field"`     // field to aggregate
	Operation string `json:"operation"` // sum, count, avg, min, max, concat, first, last
	Alias     string `json:"alias"`     // optional alias for result field
}

type GroupingOptions struct {
	SortBy        string `json:"sort_by"`        // field to sort groups by
	SortDirection string `json:"sort_direction"` // asc or desc
	IncludeCount  bool   `json:"include_count"`  // include count of items in each group
	CountAlias    string `json:"count_alias"`    // alias for count field (default: "count")
}

func (g *GroupingHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal data: %v", err), Ctx: ctx}
	}

	// Get source data
	sourceData, exists := data[g.SourceField]
	if !exists {
		return mq.Result{Error: fmt.Errorf("source field '%s' not found", g.SourceField), Ctx: ctx}
	}

	// Convert to slice of maps
	items, err := g.convertToSliceOfMaps(sourceData)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	// Group the data
	groups := g.groupData(items)

	// Apply aggregations
	result := g.applyAggregations(groups)

	// Sort if requested
	if g.Options.SortBy != "" {
		result = g.sortGroups(result)
	}

	// Set target field
	targetField := g.TargetField
	if targetField == "" {
		targetField = "grouped_data"
	}
	data[targetField] = result

	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func (g *GroupingHandler) convertToSliceOfMaps(data any) ([]map[string]any, error) {
	rv := reflect.ValueOf(data)

	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("source data must be an array or slice")
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

func (g *GroupingHandler) groupData(items []map[string]any) map[string][]map[string]any {
	groups := make(map[string][]map[string]any)

	for _, item := range items {
		// Create group key
		var keyParts []string
		for _, field := range g.GroupByFields {
			value := fmt.Sprintf("%v", item[field])
			keyParts = append(keyParts, value)
		}
		groupKey := strings.Join(keyParts, "|")

		// Add item to group
		groups[groupKey] = append(groups[groupKey], item)
	}

	return groups
}

func (g *GroupingHandler) applyAggregations(groups map[string][]map[string]any) []map[string]any {
	var result []map[string]any

	for groupKey, items := range groups {
		groupResult := make(map[string]any)

		// Add group key fields
		keyParts := strings.Split(groupKey, "|")
		for i, field := range g.GroupByFields {
			if i < len(keyParts) {
				groupResult[field] = keyParts[i]
			}
		}

		// Add count if requested
		if g.Options.IncludeCount {
			countAlias := g.Options.CountAlias
			if countAlias == "" {
				countAlias = "count"
			}
			groupResult[countAlias] = len(items)
		}

		// Apply aggregations
		for _, agg := range g.Aggregations {
			fieldAlias := agg.Alias
			if fieldAlias == "" {
				fieldAlias = agg.Field + "_" + agg.Operation
			}

			aggregatedValue := g.performAggregation(items, agg)
			groupResult[fieldAlias] = aggregatedValue
		}

		result = append(result, groupResult)
	}

	return result
}

func (g *GroupingHandler) performAggregation(items []map[string]any, agg AggregationConfig) any {
	switch agg.Operation {
	case "count":
		return len(items)
	case "sum":
		return g.sumValues(items, agg.Field)
	case "avg":
		sum := g.sumValues(items, agg.Field)
		if count := len(items); count > 0 {
			return sum / float64(count)
		}
		return 0
	case "min":
		return g.minValue(items, agg.Field)
	case "max":
		return g.maxValue(items, agg.Field)
	case "first":
		if len(items) > 0 {
			return items[0][agg.Field]
		}
		return nil
	case "last":
		if len(items) > 0 {
			return items[len(items)-1][agg.Field]
		}
		return nil
	case "concat":
		return g.concatValues(items, agg.Field)
	case "unique":
		return g.uniqueValues(items, agg.Field)
	default:
		return nil
	}
}

func (g *GroupingHandler) sumValues(items []map[string]any, field string) float64 {
	var sum float64
	for _, item := range items {
		if value, exists := item[field]; exists {
			if num := g.toFloat64(value); num != 0 {
				sum += num
			}
		}
	}
	return sum
}

func (g *GroupingHandler) minValue(items []map[string]any, field string) any {
	var min any
	for _, item := range items {
		if value, exists := item[field]; exists {
			if min == nil {
				min = value
			} else {
				if g.compareValues(value, min) < 0 {
					min = value
				}
			}
		}
	}
	return min
}

func (g *GroupingHandler) maxValue(items []map[string]any, field string) any {
	var max any
	for _, item := range items {
		if value, exists := item[field]; exists {
			if max == nil {
				max = value
			} else {
				if g.compareValues(value, max) > 0 {
					max = value
				}
			}
		}
	}
	return max
}

func (g *GroupingHandler) concatValues(items []map[string]any, field string) string {
	var values []string
	for _, item := range items {
		if value, exists := item[field]; exists {
			values = append(values, fmt.Sprintf("%v", value))
		}
	}
	return strings.Join(values, ", ")
}

func (g *GroupingHandler) uniqueValues(items []map[string]any, field string) []any {
	seen := make(map[string]bool)
	var unique []any

	for _, item := range items {
		if value, exists := item[field]; exists {
			key := fmt.Sprintf("%v", value)
			if !seen[key] {
				seen[key] = true
				unique = append(unique, value)
			}
		}
	}
	return unique
}

func (g *GroupingHandler) toFloat64(value any) float64 {
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

func (g *GroupingHandler) compareValues(a, b any) int {
	aFloat := g.toFloat64(a)
	bFloat := g.toFloat64(b)

	if aFloat < bFloat {
		return -1
	} else if aFloat > bFloat {
		return 1
	}

	// If numeric comparison doesn't work, compare as strings
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return strings.Compare(aStr, bStr)
}

func (g *GroupingHandler) sortGroups(groups []map[string]any) []map[string]any {
	sort.Slice(groups, func(i, j int) bool {
		valueI := groups[i][g.Options.SortBy]
		valueJ := groups[j][g.Options.SortBy]

		comparison := g.compareValues(valueI, valueJ)

		if g.Options.SortDirection == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})

	return groups
}

// Factory function
func NewGroupingHandler(id, sourceField, targetField string, groupByFields []string, aggregations []AggregationConfig, options GroupingOptions) *GroupingHandler {
	return &GroupingHandler{
		Operation: dag.Operation{
			ID:   id,
			Key:  "group_data",
			Type: dag.Function,
			Tags: []string{"data", "grouping", "aggregation"},
		},
		GroupByFields: groupByFields,
		Aggregations:  aggregations,
		SourceField:   sourceField,
		TargetField:   targetField,
		Options:       options,
	}
}
