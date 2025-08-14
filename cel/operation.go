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

func (m *MethodCall) Evaluate(ctx *Context) (Value, error) {
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
