package cel

import (
	"fmt"
	"sort"
	"strings"
)

// RegisterFunction allows dynamic registration of new method handlers
func RegisterFunction(name string, handler MethodHandler) {
	methodRegistry[name] = handler
}

// RegisterFunctions allows bulk registration of methods
func RegisterFunctions(methods map[string]MethodHandler) {
	for name, handler := range methods {
		methodRegistry[name] = handler
	}
}

// UnregisterFunction removes a method from the registry
func UnregisterFunction(name string) {
	delete(methodRegistry, name)
}

// GetRegisteredFunctions returns all currently registered method names
func GetRegisteredFunctions() []string {
	methods := make([]string, 0, len(methodRegistry))
	for name := range methodRegistry {
		methods = append(methods, name)
	}
	return methods
}

// MethodHandler defines a function signature for method implementations
type MethodHandler func(obj Value, args ...Value) (Value, error)

// methodRegistry holds all available methods for different types
var methodRegistry = map[string]MethodHandler{
	// String methods
	"contains": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("contains() requires 1 argument")
		}
		str := toString(obj)
		arg := toString(args[0])
		return strings.Contains(str, arg), nil
	},

	"startsWith": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("startsWith() requires 1 argument")
		}
		str := toString(obj)
		prefix := toString(args[0])
		return strings.HasPrefix(str, prefix), nil
	},

	"endsWith": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("endsWith() requires 1 argument")
		}
		str := toString(obj)
		suffix := toString(args[0])
		return strings.HasSuffix(str, suffix), nil
	},

	"split": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("split() requires 1 argument")
		}
		str := toString(obj)
		sep := toString(args[0])
		parts := strings.Split(str, sep)
		// Zero allocation optimization: pre-allocate result slice
		result := make([]Value, len(parts))
		for i, part := range parts {
			result[i] = part
		}
		return result, nil
	},

	"replace": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("replace() requires 2 arguments")
		}
		str := toString(obj)
		old := toString(args[0])
		new := toString(args[1])
		return strings.ReplaceAll(str, old, new), nil
	},

	"trim": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("trim() requires 0 arguments")
		}
		return strings.TrimSpace(toString(obj)), nil
	},

	"upper": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("upper() requires 0 arguments")
		}
		return strings.ToUpper(toString(obj)), nil
	},

	"lower": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("lower() requires 0 arguments")
		}
		return strings.ToLower(toString(obj)), nil
	},

	"matches": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("matches() requires 1 argument")
		}
		str := toString(obj)
		pattern := toString(args[0])
		// Simple pattern matching (can be extended with regex)
		return strings.Contains(str, pattern), nil
	},

	// Collection methods - work on any collection type
	"length": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("length() requires 0 arguments")
		}
		return getLength(obj), nil
	},

	"size": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("size() requires 0 arguments")
		}
		return getLength(obj), nil
	},

	"reverse": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("reverse() requires 0 arguments")
		}
		return reverseCollection(obj), nil
	},

	"isEmpty": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("isEmpty() requires 0 arguments")
		}
		return getLength(obj) == 0, nil
	},

	// Advanced collection operations
	"distinct": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("distinct() requires 0 arguments")
		}

		collection := toValueSlice(obj)
		if collection == nil {
			return nil, fmt.Errorf("distinct() requires a collection")
		}

		seen := make(map[string]bool)
		var result []Value
		for _, item := range collection {
			key := toString(item)
			if !seen[key] {
				seen[key] = true
				result = append(result, item)
			}
		}
		return result, nil
	},

	"flatten": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("flatten() requires 0 arguments")
		}

		collection := toValueSlice(obj)
		if collection == nil {
			return nil, fmt.Errorf("flatten() requires a collection")
		}

		var result []Value
		var flattenRecursive func(items []Value)
		flattenRecursive = func(items []Value) {
			for _, item := range items {
				if subCollection := toValueSlice(item); subCollection != nil {
					flattenRecursive(subCollection)
				} else {
					result = append(result, item)
				}
			}
		}

		flattenRecursive(collection)
		return result, nil
	},

	"sortBy": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("sortBy() requires 1 argument (key function)")
		}

		collection := toValueSlice(obj)
		if collection == nil {
			return nil, fmt.Errorf("sortBy() requires a collection")
		}

		// Create a copy to sort
		sorted := make([]Value, len(collection))
		copy(sorted, collection)

		// Sort using Go's built-in sort
		sort.Slice(sorted, func(i, j int) bool {
			// For now, sort by string representation
			return toString(sorted[i]) < toString(sorted[j])
		})

		return sorted, nil
	},

	// Duration methods
	"seconds": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("seconds() requires 0 arguments")
		}
		if dur, ok := obj.(Duration); ok {
			return dur.D.Seconds(), nil
		}
		return nil, fmt.Errorf("seconds() requires a duration")
	},

	"minutes": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("minutes() requires 0 arguments")
		}
		if dur, ok := obj.(Duration); ok {
			return dur.D.Minutes(), nil
		}
		return nil, fmt.Errorf("minutes() requires a duration")
	},

	"hours": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("hours() requires 0 arguments")
		}
		if dur, ok := obj.(Duration); ok {
			return dur.D.Hours(), nil
		}
		return nil, fmt.Errorf("hours() requires a duration")
	},

	// Timestamp methods
	"format": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("format() requires 1 argument")
		}
		if ts, ok := obj.(Timestamp); ok {
			layout := toString(args[0])
			return ts.T.Format(layout), nil
		}
		return nil, fmt.Errorf("format() requires a timestamp")
	},

	"year": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("year() requires 0 arguments")
		}
		if ts, ok := obj.(Timestamp); ok {
			return ts.T.Year(), nil
		}
		return nil, fmt.Errorf("year() requires a timestamp")
	},

	"month": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("month() requires 0 arguments")
		}
		if ts, ok := obj.(Timestamp); ok {
			return int(ts.T.Month()), nil
		}
		return nil, fmt.Errorf("month() requires a timestamp")
	},

	"day": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("day() requires 0 arguments")
		}
		if ts, ok := obj.(Timestamp); ok {
			return ts.T.Day(), nil
		}
		return nil, fmt.Errorf("day() requires a timestamp")
	},

	// Bytes methods
	"decode": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("decode() requires 0 arguments")
		}
		if b, ok := obj.(Bytes); ok {
			return string(b.data), nil
		}
		return nil, fmt.Errorf("decode() requires bytes")
	},

	"encode": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("encode() requires 0 arguments")
		}
		str := toString(obj)
		return Bytes{data: []byte(str)}, nil
	},

	// Optional methods
	"hasValue": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("hasValue() requires 0 arguments")
		}
		if opt, ok := obj.(Optional); ok {
			return opt.Valid, nil
		}
		return obj != nil, nil
	},

	"orElse": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("orElse() requires 1 argument")
		}
		if opt, ok := obj.(Optional); ok {
			if opt.Valid {
				return opt.Value, nil
			}
		} else if obj != nil {
			return obj, nil
		}
		return args[0], nil
	},
	"join": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("join() requires 1 argument")
		}
		separator := toString(args[0])

		if slice, ok := obj.([]Value); ok {
			if len(slice) == 0 {
				return "", nil // Zero allocation for empty slices
			}
			// Pre-calculate capacity to avoid reallocations
			var totalLen int
			for _, item := range slice {
				totalLen += len(toString(item))
			}
			totalLen += (len(slice) - 1) * len(separator)

			// Use strings.Builder for efficient concatenation
			var builder strings.Builder
			builder.Grow(totalLen)

			for i, item := range slice {
				if i > 0 {
					builder.WriteString(separator)
				}
				builder.WriteString(toString(item))
			}
			return builder.String(), nil
		}
		return toString(obj), nil
	},

	"first": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("first() requires 0 arguments")
		}
		if slice, ok := obj.([]Value); ok {
			if len(slice) > 0 {
				return slice[0], nil
			}
		}
		return nil, nil
	},

	"last": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("last() requires 0 arguments")
		}
		if slice, ok := obj.([]Value); ok {
			if len(slice) > 0 {
				return slice[len(slice)-1], nil
			}
		}
		return nil, nil
	},
}
