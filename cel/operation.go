package cel

import (
	"fmt"
	"reflect"
	"time"
)

// Value represents any value in the expression language
type Value any

// Advanced CEL types
type Duration struct {
	D time.Duration
}

func (d Duration) String() string {
	return d.D.String()
}

type Timestamp struct {
	T time.Time
}

func (t Timestamp) String() string {
	return t.T.Format(time.RFC3339)
}

type Bytes struct {
	data []byte
}

func (b Bytes) String() string {
	return string(b.data)
}

// Optional type for error handling
type Optional struct {
	Value Value
	Valid bool
}

func (o Optional) IsValid() bool {
	return o.Valid
}

func (o Optional) Get() Value {
	if o.Valid {
		return o.Value
	}
	return nil
}

// Expression represents a parsed expression
type Expression interface {
	Evaluate(ctx *Context) (Value, error)
}

// AST Node types
type BinaryOp struct {
	Left  Expression
	Right Expression
	Op    string
}

func (b *BinaryOp) Evaluate(ctx *Context) (Value, error) {
	left, err := b.Left.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	right, err := b.Right.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return evaluateBinaryOp(left, right, b.Op)
}

type UnaryOp struct {
	Expr Expression
	Op   string
}

func (u *UnaryOp) Evaluate(ctx *Context) (Value, error) {
	val, err := u.Expr.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	switch u.Op {
	case "!":
		return !toBool(val), nil
	case "-":
		return negateNumber(val)
	}

	return nil, fmt.Errorf("unknown unary operator: %s", u.Op)
}

type Literal struct {
	Value Value
}

func (l *Literal) Evaluate(ctx *Context) (Value, error) {
	return l.Value, nil
}

type Variable struct {
	Name string
}

func (v *Variable) Evaluate(ctx *Context) (Value, error) {
	if val, exists := ctx.Get(v.Name); exists {
		return val, nil
	}
	return nil, fmt.Errorf("undefined variable: %s", v.Name)
}

type FieldAccess struct {
	Object Expression
	Field  string
}

func (f *FieldAccess) Evaluate(ctx *Context) (Value, error) {
	obj, err := f.Object.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return getField(obj, f.Field)
}

type IndexAccess struct {
	Object Expression
	Index  Expression
}

func (i *IndexAccess) Evaluate(ctx *Context) (Value, error) {
	obj, err := i.Object.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	idx, err := i.Index.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return getIndex(obj, idx)
}

type FunctionCall struct {
	Name string
	Args []Expression
}

func (f *FunctionCall) Evaluate(ctx *Context) (Value, error) {
	if fn, exists := ctx.Functions[f.Name]; exists {
		args := make([]Value, len(f.Args))
		var err error
		for i, arg := range f.Args {
			args[i], err = arg.Evaluate(ctx)
			if err != nil {
				return nil, err
			}
		}
		return fn(args)
	}
	return nil, fmt.Errorf("undefined function: %s", f.Name)
}

type MethodCall struct {
	Object Expression
	Method string
	Args   []Expression
}

// ChainInfo represents a method chain that can be optimized
type ChainInfo struct {
	BaseObject Expression
	Methods    []ChainStep
}

type ChainStep struct {
	Method string
	Args   []Expression
}

// detectMethodChain analyzes a MethodCall to see if it's part of an optimizable chain
func detectMethodChain(m *MethodCall) *ChainInfo {
	var steps []ChainStep
	current := m

	// Traverse up the method call chain
	for {
		steps = append([]ChainStep{{Method: current.Method, Args: current.Args}}, steps...)

		if nextMethod, ok := current.Object.(*MethodCall); ok {
			current = nextMethod
		} else {
			// Reached the base object
			return &ChainInfo{
				BaseObject: current.Object,
				Methods:    steps,
			}
		}
	}
}

// executeOptimizedChain tries to execute an optimized version of the method chain
func executeOptimizedChain(chain *ChainInfo, ctx *Context) (Value, bool) {
	// Check if this is a string method chain that we can optimize
	if isStringMethodChain(chain) {
		if result, ok := executeOptimizedStringChain(chain, ctx); ok {
			return result, true
		}
	}

	// Check if this is a collection chain that we can optimize
	if isCollectionChain(chain) {
		if result, ok := executeOptimizedCollectionChain(chain, ctx); ok {
			return result, true
		}
	}

	return nil, false
}

func isCollectionChain(chain *ChainInfo) bool {
	collectionMethods := map[string]bool{
		"filter": true, "map": true, "join": true, "reduce": true, "flatten": true, "distinct": true,
	}

	for _, step := range chain.Methods {
		if !collectionMethods[step.Method] {
			return false
		}
	}
	return len(chain.Methods) > 0
}

func executeOptimizedCollectionChain(chain *ChainInfo, ctx *Context) (Value, bool) {
	// Special case: If we have a single method like join() on a Macro (which is filter/map chain)
	if len(chain.Methods) == 1 && chain.Methods[0].Method == "join" {
		if macro, ok := chain.BaseObject.(*Macro); ok {
			// Check if this is a chained macro (filter followed by map)
			return optimizeMacroJoinChain(macro, chain.Methods[0], ctx)
		}
	}

	// Get base object
	baseObj, err := chain.BaseObject.Evaluate(ctx)
	if err != nil {
		return nil, false
	}

	// Convert to slice
	collection := toValueSlice(baseObj)
	if collection == nil {
		return nil, false
	}

	// Detect filter().map().join() pattern and use ultra-fast operations
	if len(chain.Methods) == 3 &&
		chain.Methods[0].Method == "filter" &&
		chain.Methods[1].Method == "map" &&
		chain.Methods[2].Method == "join" {

		// Use our ultra-fast operations from ultra_fast_collections.go
		return executeCachedFilterMapJoin(collection, chain.Methods, ctx)
	}

	return nil, false
}

func optimizeMacroJoinChain(macro *Macro, joinStep ChainStep, ctx *Context) (Value, bool) {
	// Check if this is a map operation on a filtered collection
	if macro.Type == "map" {
		// Check if the collection of the map is itself a filter macro
		if filterMacro, ok := macro.Collection.(*Macro); ok && filterMacro.Type == "filter" {
			// This is filter().map().join() - optimize it!
			return executeMacroFilterMapJoin(filterMacro, macro, joinStep, ctx)
		}
	}

	return nil, false
}

func executeMacroFilterMapJoin(filterMacro *Macro, mapMacro *Macro, joinStep ChainStep, ctx *Context) (Value, bool) {
	// Get the base collection
	baseCollection, err := filterMacro.Collection.Evaluate(ctx)
	if err != nil {
		return nil, false
	}

	collection := toValueSlice(baseCollection)
	if collection == nil {
		return nil, false
	}

	// Get join separator
	if len(joinStep.Args) != 1 {
		return nil, false
	}

	sepVal, err := joinStep.Args[0].Evaluate(ctx)
	if err != nil {
		return nil, false
	}

	separator := toString(sepVal)

	// Use the ultra-fast collection operations
	filtered, err := Cached.Filter(collection, filterMacro.Variable, filterMacro.Body, ctx)
	if err != nil {
		return nil, false
	}

	mapped, err := Cached.Map(filtered, mapMacro.Variable, mapMacro.Body, ctx)
	if err != nil {
		return nil, false
	}

	result := Cached.Join(mapped, separator)
	return result, true
}

func executeCachedFilterMapJoin(collection []Value, methods []ChainStep, ctx *Context) (Value, bool) {
	// Extract filter parameters
	if len(methods[0].Args) != 2 {
		return nil, false
	}

	filterVar, ok := methods[0].Args[0].(*Variable)
	if !ok {
		return nil, false
	}

	filterExpr := methods[0].Args[1]

	// Extract map parameters
	if len(methods[1].Args) != 2 {
		return nil, false
	}

	mapVar, ok := methods[1].Args[0].(*Variable)
	if !ok {
		return nil, false
	}

	mapExpr := methods[1].Args[1]

	// Extract join separator
	if len(methods[2].Args) != 1 {
		return nil, false
	}

	sepVal, err := methods[2].Args[0].Evaluate(ctx)
	if err != nil {
		return nil, false
	}

	separator := toString(sepVal)

	// Use the ultra-fast collection operations
	filtered, err := Cached.Filter(collection, filterVar.Name, filterExpr, ctx)
	if err != nil {
		return nil, false
	}

	mapped, err := Cached.Map(filtered, mapVar.Name, mapExpr, ctx)
	if err != nil {
		return nil, false
	}

	result := Cached.Join(mapped, separator)
	return result, true
}

func isStringMethodChain(chain *ChainInfo) bool {
	stringMethods := map[string]bool{
		"trim": true, "upper": true, "lower": true, "replace": true,
	}

	for _, step := range chain.Methods {
		if !stringMethods[step.Method] {
			return false
		}
	}
	return true
}

func executeOptimizedStringChain(chain *ChainInfo, ctx *Context) (Value, bool) {
	// Get base object
	baseObj, err := chain.BaseObject.Evaluate(ctx)
	if err != nil {
		return nil, false
	}

	baseStr, ok := baseObj.(string)
	if !ok {
		return nil, false
	}

	// Check for specific optimized patterns
	if len(chain.Methods) == 3 &&
		chain.Methods[0].Method == "trim" && len(chain.Methods[0].Args) == 0 &&
		chain.Methods[1].Method == "upper" && len(chain.Methods[1].Args) == 0 &&
		chain.Methods[2].Method == "replace" && len(chain.Methods[2].Args) == 2 {

		// Evaluate replace arguments
		oldVal, err := chain.Methods[2].Args[0].Evaluate(ctx)
		if err != nil {
			return nil, false
		}
		newVal, err := chain.Methods[2].Args[1].Evaluate(ctx)
		if err != nil {
			return nil, false
		}

		oldStr := toString(oldVal)
		newStr := toString(newVal)

		// Use our ultra-fast combined operation
		result := ZeroAlloc.CombinedTrimUpperReplace(baseStr, oldStr, newStr)
		return result, true
	}

	// Check for trim().upper() pattern
	if len(chain.Methods) == 2 &&
		chain.Methods[0].Method == "trim" && len(chain.Methods[0].Args) == 0 &&
		chain.Methods[1].Method == "upper" && len(chain.Methods[1].Args) == 0 {

		result := ZeroAlloc.CombinedTrimUpper(baseStr)
		return result, true
	}

	return nil, false
}

func (m *MethodCall) Evaluate(ctx *Context) (Value, error) {
	// Try to optimize method chains before evaluating normally
	if chain := detectMethodChain(m); chain != nil {
		if optimized, ok := executeOptimizedChain(chain, ctx); ok {
			return optimized, nil
		}
	}

	obj, err := m.Object.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	args := make([]Value, len(m.Args))
	for i, arg := range m.Args {
		args[i], err = arg.Evaluate(ctx)
		if err != nil {
			return nil, err
		}
	}

	return callMethod(obj, m.Method, args)
}

type ArrayLiteral struct {
	Elements []Expression
}

func (a *ArrayLiteral) Evaluate(ctx *Context) (Value, error) {
	result := make([]Value, len(a.Elements))
	for i, elem := range a.Elements {
		val, err := elem.Evaluate(ctx)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

type Macro struct {
	Collection Expression
	Variable   string
	Body       Expression
	Type       string // "all", "exists", "filter", "map", "find", "size", "reverse", "sort", "flatMap"
}

func (m *Macro) Evaluate(ctx *Context) (Value, error) {
	coll, err := m.Collection.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	return evaluateMacro(coll, m.Variable, m.Body, m.Type, ctx)
}

type Comprehension struct {
	Expression Expression
	Variable   string
	Collection Expression
	Condition  Expression // optional filter condition
}

func (c *Comprehension) Evaluate(ctx *Context) (Value, error) {
	coll, err := c.Collection.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	// Convert collection to []Value
	var items []Value
	switch col := coll.(type) {
	case []Value:
		items = col
	default:
		rv := reflect.ValueOf(col)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			items = make([]Value, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				items[i] = rv.Index(i).Interface()
			}
		} else {
			return nil, fmt.Errorf("cannot iterate over non-collection in comprehension")
		}
	}

	// Create new context for iteration
	newCtx := &Context{
		Variables: make(map[string]Value),
		Functions: ctx.Functions,
	}

	// Copy existing variables
	for k, v := range ctx.Variables {
		newCtx.Variables[k] = v
	}

	var result []Value
	for _, item := range items {
		newCtx.Variables[c.Variable] = item

		// Check condition if present
		if c.Condition != nil {
			condResult, err := c.Condition.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			if !toBool(condResult) {
				continue // Skip this item
			}
		}

		// Evaluate expression
		exprResult, err := c.Expression.Evaluate(newCtx)
		if err != nil {
			return nil, err
		}
		result = append(result, exprResult)
	}

	return result, nil
}

type TernaryOp struct {
	Condition Expression
	TrueExpr  Expression
	FalseExpr Expression
}

func (t *TernaryOp) Evaluate(ctx *Context) (Value, error) {
	cond, err := t.Condition.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	if toBool(cond) {
		return t.TrueExpr.Evaluate(ctx)
	} else {
		return t.FalseExpr.Evaluate(ctx)
	}
}
