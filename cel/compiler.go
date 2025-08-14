package cel

import (
	"sync"
)

// Compiled expression system to compete with expr's compilation advantage
type CompiledExpression struct {
	ast      Expression
	metadata *ExpressionMetadata
}

type ExpressionMetadata struct {
	IsConstant      bool
	RequiredVars    []string
	UsedMethods     []string
	ComplexityScore int
}

// ExpressionCompiler provides compilation and optimization of expressions
type ExpressionCompiler struct {
	cache   sync.Map // Thread-safe cache for compiled expressions
	enabled bool
}

var GlobalCompiler = &ExpressionCompiler{enabled: true}

// Compile parses and optimizes an expression for repeated evaluation
func (ec *ExpressionCompiler) Compile(exprStr string) (*CompiledExpression, error) {
	if !ec.enabled {
		// Fallback to regular parsing
		parser := NewParser(exprStr)
		ast, err := parser.Parse()
		if err != nil {
			return nil, err
		}
		return &CompiledExpression{ast: ast}, nil
	}

	// Check cache first
	if cached, ok := ec.cache.Load(exprStr); ok {
		return cached.(*CompiledExpression), nil
	}

	// Parse and analyze expression
	parser := NewParser(exprStr)
	ast, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	// Analyze expression metadata
	metadata := ec.analyzeExpression(ast)

	// Create compiled expression
	compiled := &CompiledExpression{
		ast:      ast,
		metadata: metadata,
	}

	// Cache for future use
	ec.cache.Store(exprStr, compiled)

	return compiled, nil
}

// Fast evaluation using compiled expression
func (ce *CompiledExpression) EvaluateCompiled(ctx *Context) (Value, error) {
	// If it's a constant expression, we could pre-compute it
	if ce.metadata != nil && ce.metadata.IsConstant {
		// For demo purposes, just evaluate normally
		// In a real implementation, we'd cache the constant value
	}

	return ce.ast.Evaluate(ctx)
}

// analyzeExpression examines the AST to gather optimization metadata
func (ec *ExpressionCompiler) analyzeExpression(expr Expression) *ExpressionMetadata {
	metadata := &ExpressionMetadata{
		RequiredVars: make([]string, 0),
		UsedMethods:  make([]string, 0),
	}

	ec.walkExpression(expr, metadata)

	return metadata
}

func (ec *ExpressionCompiler) walkExpression(expr Expression, metadata *ExpressionMetadata) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *Variable:
		metadata.RequiredVars = append(metadata.RequiredVars, e.Name)
		metadata.ComplexityScore += 1

	case *MethodCall:
		metadata.UsedMethods = append(metadata.UsedMethods, e.Method)
		metadata.ComplexityScore += 2
		ec.walkExpression(e.Object, metadata)
		for _, arg := range e.Args {
			ec.walkExpression(arg, metadata)
		}

	case *BinaryOp:
		metadata.ComplexityScore += 1
		ec.walkExpression(e.Left, metadata)
		ec.walkExpression(e.Right, metadata)

	case *Macro:
		metadata.ComplexityScore += 5 // Macros are more expensive
		ec.walkExpression(e.Collection, metadata)
		if e.Body != nil {
			ec.walkExpression(e.Body, metadata)
		}

	case *Literal:
		// Literals are free
		if metadata.ComplexityScore == 0 {
			metadata.IsConstant = true
		}

		// Add more cases as needed
	}
}

// ClearCache clears the compilation cache
func (ec *ExpressionCompiler) ClearCache() {
	ec.cache = sync.Map{}
}

// GetCacheStats returns cache statistics
func (ec *ExpressionCompiler) GetCacheStats() map[string]int {
	stats := map[string]int{"entries": 0}
	ec.cache.Range(func(key, value interface{}) bool {
		stats["entries"]++
		return true
	})
	return stats
}
