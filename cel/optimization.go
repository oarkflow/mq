package cel

import (
	"fmt"
	"sync"
)

// CachedContext provides caching and performance features
type CachedContext struct {
	*Context
	methodCache sync.Map // Cache for frequently used method lookups
	exprCache   sync.Map // Cache for compiled expressions
}

// NewCachedContext creates a new cached context
func NewCachedContext() *CachedContext {
	return &CachedContext{
		Context: NewContext(),
	}
}

// CallWithCache provides method calling with caching
func (cc *CachedContext) CallWithCache(obj Value, method string, args []Value) (Value, error) {
	// Create cache key
	cacheKey := method // Simple key for now, could be more sophisticated

	// Check cache first
	if cached, ok := cc.methodCache.Load(cacheKey); ok {
		if handler, ok := cached.(MethodHandler); ok {
			return handler(obj, args...)
		}
	}

	// Fallback to regular method call and cache result
	result, err := callMethod(obj, method, args)
	if err == nil {
		// Cache successful lookups
		if handler, exists := methodRegistry[method]; exists {
			cc.methodCache.Store(cacheKey, handler)
		}
	}

	return result, err
}

// StringConcat provides efficient string concatenation
func (cc *CachedContext) StringConcat(values []Value) string {
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

// ArrayAccess provides bounds-checked array access
func (cc *CachedContext) ArrayAccess(arr []Value, index int) (Value, bool) {
	if index < 0 || index >= len(arr) {
		return nil, false
	}
	return arr[index], true
}

// MapAccess provides map field access
func (cc *CachedContext) MapAccess(obj map[string]Value, field string) (Value, bool) {
	val, exists := obj[field]
	return val, exists
}

// builtinMethodRegistry provides compile-time method resolution for common methods
var builtinMethodRegistry = map[string]func(obj Value, args ...Value) (Value, error){
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

// CallMethod provides method calling with builtin method support
func CallMethod(obj Value, method string, args []Value) (Value, error) {
	// Try builtin registry first for common methods
	if handler, exists := builtinMethodRegistry[method]; exists {
		return handler(obj, args...)
	}

	// Fall back to regular method calling
	return callMethod(obj, method, args)
}
