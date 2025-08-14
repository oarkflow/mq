package cel

import (
	"fmt"
	"sync"
)

// OptimizedContext provides caching and optimization features
type OptimizedContext struct {
	*Context
	methodCache sync.Map // Cache for frequently used method lookups
	exprCache   sync.Map // Cache for compiled expressions
}

// NewOptimizedContext creates a new optimized context
func NewOptimizedContext() *OptimizedContext {
	return &OptimizedContext{
		Context: NewContext(),
	}
}

// CachedMethodCall provides optimized method calling with caching
func (oc *OptimizedContext) CachedMethodCall(obj Value, method string, args []Value) (Value, error) {
	// Create cache key
	cacheKey := method // Simple key for now, could be more sophisticated

	// Check cache first
	if cached, ok := oc.methodCache.Load(cacheKey); ok {
		if handler, ok := cached.(MethodHandler); ok {
			return handler(obj, args...)
		}
	}

	// Fallback to regular method call and cache result
	result, err := callMethod(obj, method, args)
	if err == nil {
		// Cache successful lookups
		if handler, exists := methodRegistry[method]; exists {
			oc.methodCache.Store(cacheKey, handler)
		}
	}

	return result, err
}

// Fast path optimizations for common operations
type FastPathOps struct{}

var FastPath = &FastPathOps{}

// FastStringConcat provides optimized string concatenation
func (fp *FastPathOps) StringConcat(values []Value) string {
	if len(values) == 0 {
		return ""
	}
	if len(values) == 1 {
		return toString(values[0])
	}

	// Calculate total length to avoid reallocations
	totalLen := 0
	strValues := make([]string, len(values))
	for i, v := range values {
		strValues[i] = toString(v)
		totalLen += len(strValues[i])
	}

	// Use a single allocation
	result := make([]byte, 0, totalLen)
	for _, s := range strValues {
		result = append(result, s...)
	}

	return string(result)
}

// FastArrayAccess provides bounds-checked array access with minimal overhead
func (fp *FastPathOps) ArrayAccess(arr []Value, index int) (Value, bool) {
	if index < 0 || index >= len(arr) {
		return nil, false
	}
	return arr[index], true
}

// FastMapAccess provides optimized map field access
func (fp *FastPathOps) MapAccess(obj map[string]Value, field string) (Value, bool) {
	val, exists := obj[field]
	return val, exists
}

// InlineMethodRegistry provides compile-time method resolution for better performance
var InlineMethodRegistry = map[string]func(obj Value, args ...Value) (Value, error){
	"size": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("size() requires 0 arguments")
		}
		return getLength(obj), nil
	},
	"length": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("length() requires 0 arguments")
		}
		return getLength(obj), nil
	},
	"first": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("first() requires 0 arguments")
		}
		if slice, ok := obj.([]Value); ok && len(slice) > 0 {
			return slice[0], nil
		}
		return nil, nil
	},
	"last": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("last() requires 0 arguments")
		}
		if slice, ok := obj.([]Value); ok && len(slice) > 0 {
			return slice[len(slice)-1], nil
		}
		return nil, nil
	},
}

// OptimizedCallMethod provides fast method calling for common cases
func OptimizedCallMethod(obj Value, method string, args []Value) (Value, error) {
	// Try inline registry first for hot path methods
	if handler, exists := InlineMethodRegistry[method]; exists {
		return handler(obj, args...)
	}

	// Fall back to regular method calling
	return callMethod(obj, method, args)
}
