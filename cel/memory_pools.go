package cel

import (
	"sync"
)

// Memory pools for reducing allocations during evaluation

var (
	// Pool for []Value slices to reduce allocations in collection operations
	valueSlicePool = sync.Pool{
		New: func() interface{} {
			return make([]Value, 0, 8)
		},
	}

	// Pool for Context objects to avoid repeated allocations
	contextPool = sync.Pool{
		New: func() interface{} {
			return &Context{
				Variables: make(map[string]Value, 16),
				Functions: make(map[string]func([]Value) (Value, error)),
			}
		},
	}

	// Pool for string builders
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			return NewOptimizedStringBuilder(128)
		},
	}
)

// GetValueSlice returns a reusable []Value slice from the pool
func GetValueSlice() []Value {
	return valueSlicePool.Get().([]Value)[:0]
}

// PutValueSlice returns a []Value slice to the pool for reuse
func PutValueSlice(s []Value) {
	if cap(s) > 64 { // Don't pool very large slices
		return
	}
	valueSlicePool.Put(s)
}

// GetContext returns a reusable Context from the pool
func GetPooledContext() *Context {
	ctx := contextPool.Get().(*Context)
	// Clear the maps
	for k := range ctx.Variables {
		delete(ctx.Variables, k)
	}
	for k := range ctx.Functions {
		delete(ctx.Functions, k)
	}
	return ctx
}

// PutContext returns a Context to the pool for reuse
func PutPooledContext(ctx *Context) {
	contextPool.Put(ctx)
}

// GetStringBuilder returns a reusable OptimizedStringBuilder from the pool
func GetStringBuilder() *OptimizedStringBuilder {
	sb := stringBuilderPool.Get().(*OptimizedStringBuilder)
	sb.Reset()
	return sb
}

// PutStringBuilder returns an OptimizedStringBuilder to the pool for reuse
func PutStringBuilder(sb *OptimizedStringBuilder) {
	if cap(sb.buf) > 1024 { // Don't pool very large builders
		return
	}
	stringBuilderPool.Put(sb)
}

// PooledEvaluateMap performs map operation with pooled resources
func PooledEvaluateMap(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	// Get pooled context
	newCtx := GetPooledContext()
	defer PutPooledContext(newCtx)

	// Set up context
	newCtx.Functions = baseCtx.Functions
	for k, v := range baseCtx.Variables {
		newCtx.Variables[k] = v
	}

	// Pre-allocate result
	mapped := make([]Value, len(items))

	// Execute mapping
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

// PooledEvaluateFilter performs filter operation with pooled resources
func PooledEvaluateFilter(items []Value, variable string, body Expression, baseCtx *Context) ([]Value, error) {
	// Get pooled context and slice
	newCtx := GetPooledContext()
	defer PutPooledContext(newCtx)

	filtered := GetValueSlice()
	defer PutValueSlice(filtered)

	// Set up context
	newCtx.Functions = baseCtx.Functions
	for k, v := range baseCtx.Variables {
		newCtx.Variables[k] = v
	}

	// Execute filtering
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

	// Copy result to avoid returning pooled slice
	result := make([]Value, len(filtered))
	copy(result, filtered)

	return result, nil
}
