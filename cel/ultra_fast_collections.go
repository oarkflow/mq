package cel

import (
	"sync"
)

// UltraFastCollections provides zero-allocation collection operations where possible
type UltraFastCollections struct{}

var UltraFast = &UltraFastCollections{}

// Reusable slice pools sized for different use cases
var (
	smallSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Value, 0, 8)
		},
	}
	mediumSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Value, 0, 32)
		},
	}
	largeSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Value, 0, 128)
		},
	}

	// Context pool for reuse
	ultraContextPool = sync.Pool{
		New: func() interface{} {
			return &Context{
				Variables: make(map[string]Value, 8),
			}
		},
	}
)

// getOptimalSlice returns the best-sized slice from pools
func getOptimalSlice(size int) []Value {
	switch {
	case size <= 8:
		return smallSlicePool.Get().([]Value)[:0]
	case size <= 32:
		return mediumSlicePool.Get().([]Value)[:0]
	default:
		return largeSlicePool.Get().([]Value)[:0]
	}
}

// putSliceBack returns slice to appropriate pool
func putSliceBack(slice []Value) {
	if slice == nil {
		return
	}
	cap := cap(slice)
	switch {
	case cap <= 8:
		smallSlicePool.Put(slice)
	case cap <= 32:
		mediumSlicePool.Put(slice)
	case cap <= 128:
		largeSlicePool.Put(slice)
	}
}

// getUltraContext returns a reusable context
func getUltraContext() *Context {
	ctx := ultraContextPool.Get().(*Context)
	// Clear existing data
	for k := range ctx.Variables {
		delete(ctx.Variables, k)
	}
	return ctx
}

// putUltraContext returns context to pool
func putUltraContext(ctx *Context) {
	ultraContextPool.Put(ctx)
}

// UltraFastFilter performs filtering with minimal allocations
func (ufc *UltraFastCollections) Filter(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	if len(items) == 0 {
		return items, nil
	}

	// Get optimal slice and context
	filtered := getOptimalSlice(len(items) / 2) // Conservative estimate
	defer putSliceBack(filtered)

	newCtx := getUltraContext()
	defer putUltraContext(newCtx)

	// Set up context efficiently
	newCtx.Functions = baseCtx.Functions
	for k, v := range baseCtx.Variables {
		newCtx.Variables[k] = v
	}

	// Filter items
	for _, item := range items {
		newCtx.Variables[variable] = item
		result, err := body.Evaluate(newCtx)
		if err != nil {
			return nil, err
		}
		if toBool(result) {
			filtered = append(filtered, item)
		}
	}

	// Create result slice (can't return pooled slice)
	result := make([]Value, len(filtered))
	copy(result, filtered)

	return result, nil
}

// UltraFastMap performs mapping with pre-allocated result
func (ufc *UltraFastCollections) Map(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	if len(items) == 0 {
		return items, nil
	}

	// Pre-allocate exact size (no reallocation needed)
	mapped := make([]Value, len(items))

	newCtx := getUltraContext()
	defer putUltraContext(newCtx)

	// Set up context efficiently
	newCtx.Functions = baseCtx.Functions
	for k, v := range baseCtx.Variables {
		newCtx.Variables[k] = v
	}

	// Map items
	for i, item := range items {
		newCtx.Variables[variable] = item
		result, err := body.Evaluate(newCtx)
		if err != nil {
			return nil, err
		}
		mapped[i] = result
	}

	return mapped, nil
}

// UltraFastJoin performs string joining with pre-calculated buffer size
func (ufc *UltraFastCollections) Join(items []Value, separator string) string {
	if len(items) == 0 {
		return ""
	}

	if len(items) == 1 {
		return toString(items[0])
	}

	// Pre-calculate total length to avoid buffer growth
	totalLen := (len(items) - 1) * len(separator)
	stringItems := make([]string, len(items)) // Convert once
	for i, item := range items {
		s := toString(item)
		stringItems[i] = s
		totalLen += len(s)
	}

	// Use efficient builder
	result := make([]byte, 0, totalLen)
	result = append(result, stringItems[0]...)

	for i := 1; i < len(stringItems); i++ {
		result = append(result, separator...)
		result = append(result, stringItems[i]...)
	}

	return string(result)
}

// OptimizedMethodChain detects and optimizes common method chains
type OptimizedMethodChain struct {
	ops []chainOp
}

type chainOp struct {
	method string
	args   []Value
}

// DetectChainOptimization attempts to optimize complex method chains
func DetectChainOptimization(obj Value, method string, args []Value) (Value, bool, error) {
	// For now, focus on collection -> string chains (filter/map -> join)
	if slice, ok := obj.([]Value); ok {
		switch method {
		case "join":
			if len(args) == 1 {
				result := UltraFast.Join(slice, toString(args[0]))
				return result, true, nil
			}
		}
	}

	return nil, false, nil
}
