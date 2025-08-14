package cel

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// Token types for lexical analysis
type TokenType int

const (
	// Literals
	IDENTIFIER TokenType = iota
	NUMBER
	STRING

	// Operators
	PLUS     // +
	MINUS    // -
	MULTIPLY // *
	DIVIDE   // /
	MODULO   // %
	POWER    // **

	// Comparison
	EQ // ==
	NE // !=
	LT // <
	LE // <=
	GT // >
	GE // >=

	// Logical
	AND // &&
	OR  // ||
	NOT // !

	// Other operators
	IN // in

	// Punctuation
	LPAREN   // (
	RPAREN   // )
	LBRACKET // [
	RBRACKET // ]
	LBRACE   // {
	RBRACE   // }
	DOT      // .
	COMMA    // ,
	COLON    // :
	QUESTION // ?
	PIPE     // |

	// Keywords
	TRUE
	FALSE
	NULL

	// Special
	EOF
	ILLEGAL
)

type Token struct {
	Type     TokenType
	Literal  string
	Position int
}

// Precedence levels
const (
	_ int = iota
	LOWEST
	TERNARY          // ? :
	OR_PRECEDENCE    // ||
	AND_PRECEDENCE   // &&
	EQUALS           // ==, !=
	LESSGREATER      // > or <, >=, <=
	IN_PRECEDENCE    // in
	SUM              // +
	PRODUCT          // *, /, %
	POWER_PRECEDENCE // **
	UNARY            // -X, !X
	CALL             // myFunction(X)
	INDEX            // array[index], obj.field
)

// Helper functions
func evaluateBinaryOp(left, right Value, op string) (Value, error) {
	switch op {
	case "==":
		return equals(left, right), nil
	case "!=":
		return !equals(left, right), nil
	case ">":
		return compare(left, right) > 0, nil
	case "<":
		return compare(left, right) < 0, nil
	case ">=":
		return compare(left, right) >= 0, nil
	case "<=":
		return compare(left, right) <= 0, nil
	case "&&":
		return toBool(left) && toBool(right), nil
	case "||":
		return toBool(left) || toBool(right), nil
	case "+":
		return add(left, right)
	case "-":
		return subtract(left, right)
	case "*":
		return multiply(left, right)
	case "/":
		return divide(left, right)
	case "%":
		return modulo(left, right)
	case "**":
		return power(left, right)
	case "in":
		return contains(right, left), nil
	}
	return nil, fmt.Errorf("unknown operator: %s", op)
}

func toBool(val Value) bool {
	if val == nil {
		return false
	}

	switch v := val.(type) {
	case bool:
		return v
	case int, int64:
		return reflect.ValueOf(v).Int() != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	case []Value:
		return len(v) > 0
	case map[string]Value:
		return len(v) > 0
	default:
		return true
	}
}

// High-performance toString with minimal allocations
func toString(val Value) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v // Zero allocation for strings
	case int:
		return strconv.Itoa(v) // Faster than fmt.Sprintf
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v) // Fallback
	}
}

// High-performance toFloat64 with minimal allocations
func toFloat64(val Value) float64 {
	switch v := val.(type) {
	case float64:
		return v // Zero allocation for float64
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	case bool:
		if v {
			return 1
		}
		return 0
	}
	return 0
}

func equals(left, right Value) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func compare(left, right Value) int {
	switch l := left.(type) {
	case string:
		if r, ok := right.(string); ok {
			if l < r {
				return -1
			} else if l > r {
				return 1
			}
			return 0
		}
	case int, int64, float64:
		lf := toFloat64(l)
		rf := toFloat64(right)
		if lf < rf {
			return -1
		} else if lf > rf {
			return 1
		}
		return 0
	}
	return 0
}

func add(left, right Value) (Value, error) {
	// String concatenation
	if l, ok := left.(string); ok {
		return l + toString(right), nil
	}
	if r, ok := right.(string); ok {
		return toString(left) + r, nil
	}

	// Numeric addition
	return toFloat64(left) + toFloat64(right), nil
}

func subtract(left, right Value) (Value, error) {
	return toFloat64(left) - toFloat64(right), nil
}

func multiply(left, right Value) (Value, error) {
	return toFloat64(left) * toFloat64(right), nil
}

func divide(left, right Value) (Value, error) {
	r := toFloat64(right)
	if r == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return toFloat64(left) / r, nil
}

func modulo(left, right Value) (Value, error) {
	r := toFloat64(right)
	if r == 0 {
		return nil, fmt.Errorf("division by zero")
	}
	return math.Mod(toFloat64(left), r), nil
}

func power(left, right Value) (Value, error) {
	return math.Pow(toFloat64(left), toFloat64(right)), nil
}

func negateNumber(val Value) (Value, error) {
	return -toFloat64(val), nil
}

func contains(container, item Value) bool {
	switch c := container.(type) {
	case []Value:
		for _, v := range c {
			if equals(v, item) {
				return true
			}
		}
	case map[string]Value:
		key := toString(item)
		_, exists := c[key]
		return exists
	case string:
		return strings.Contains(c, toString(item))
	}
	return false
}

func getField(obj Value, field string) (Value, error) {
	switch o := obj.(type) {
	case map[string]Value:
		if val, exists := o[field]; exists {
			return val, nil
		}
		return nil, nil
	default:
		rv := reflect.ValueOf(obj)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.Struct {
			fv := rv.FieldByName(field)
			if fv.IsValid() {
				return fv.Interface(), nil
			}
		}
	}
	return nil, fmt.Errorf("field %s not found", field)
}

func getIndex(obj, index Value) (Value, error) {
	switch o := obj.(type) {
	case []Value:
		idx := int(toFloat64(index))
		if idx >= 0 && idx < len(o) {
			return o[idx], nil
		}
		return nil, fmt.Errorf("index out of range")
	case map[string]Value:
		key := toString(index)
		if val, exists := o[key]; exists {
			return val, nil
		}
		return nil, nil
	}
	return nil, fmt.Errorf("cannot index into value")
}

func callMethod(obj Value, method string, args []Value) (Value, error) {
	// Special handling for collection-level methods that should NOT be vectorized
	nonVectorMethods := map[string]bool{
		"join": true, "first": true, "last": true, "size": true, "length": true,
		"reverse": true, "isEmpty": true, "flatten": true, "distinct": true, "sortBy": true,
	}

	// Check if this is a method call on a collection that should be vectorized
	if slice, ok := obj.([]Value); ok && len(slice) > 0 {
		// Only vectorize if method is not in nonVectorMethods and method exists on string/element types
		if !nonVectorMethods[method] {
			// Test if method works on individual elements by checking the first element
			firstElement := slice[0]
			_, err := callMethodOnSingle(firstElement, method, args)
			if err == nil {
				// Method is supported on elements, vectorize it
				result := make([]Value, len(slice))
				for i, item := range slice {
					val, err := callMethodOnSingle(item, method, args)
					if err != nil {
						return nil, err
					}
					result[i] = val
				}
				return result, nil
			}
		}
	}

	// Call method on the object itself (non-vectorized)
	return callMethodOnSingle(obj, method, args)
}

// Fast length calculation with zero allocation
func getLength(obj Value) int {
	switch o := obj.(type) {
	case []Value:
		return len(o)
	case map[string]Value:
		return len(o)
	case string:
		return len(o)
	case nil:
		return 0
	default:
		// Use reflection as fallback (slower but handles any type)
		rv := reflect.ValueOf(obj)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array, reflect.Map, reflect.String:
			return rv.Len()
		default:
			return 0
		}
	}
}

// Fast collection reversal with minimal allocations
func reverseCollection(obj Value) Value {
	switch o := obj.(type) {
	case []Value:
		if len(o) == 0 {
			return o // Return same empty slice to avoid allocation
		}
		// Pre-allocate result slice
		result := make([]Value, len(o))
		for i := 0; i < len(o); i++ {
			result[i] = o[len(o)-1-i]
		}
		return result
	case string:
		if len(o) == 0 {
			return o
		}
		runes := []rune(o)
		for i := 0; i < len(runes)/2; i++ {
			runes[i], runes[len(runes)-1-i] = runes[len(runes)-1-i], runes[i]
		}
		return string(runes)
	default:
		return obj // Can't reverse, return as-is
	}
}

func callMethodOnSingle(obj Value, method string, args []Value) (Value, error) {
	// Try to detect and optimize method chains
	if result, optimized, err := DetectChainOptimization(obj, method, args); optimized {
		return result, err
	}

	// Try ultra-fast string operations first for string objects
	if _, isString := obj.(string); isString {
		if result, optimized, err := DetectAndOptimizeStringChain(obj, method, args); optimized {
			return result, err
		}
		// Fallback to fast string methods
		if handler, exists := FastStringMethods[method]; exists {
			return handler(obj, args...)
		}
	}

	// Try optimized inline registry for hot path methods
	if handler, exists := InlineMethodRegistry[method]; exists {
		return handler(obj, args...)
	}

	// Fast method lookup using registry
	if handler, exists := methodRegistry[method]; exists {
		return handler(obj, args...)
	}

	// Check if it's a built-in function that can be called as method
	ctx := NewContext()
	if fn, exists := ctx.Functions[method]; exists {
		// Prepend obj to args for function call
		fnArgs := make([]Value, len(args)+1)
		fnArgs[0] = obj
		copy(fnArgs[1:], args)
		return fn(fnArgs)
	}

	// Try reflection-based method call for custom types
	return callReflectionMethod(obj, method, args)
} // High-performance reflection-based method calling
func callReflectionMethod(obj Value, method string, args []Value) (Value, error) {
	if obj == nil {
		return nil, fmt.Errorf("cannot call method %s on nil", method)
	}

	rv := reflect.ValueOf(obj)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	// Look for method on the type
	methodVal := rv.MethodByName(method)
	if !methodVal.IsValid() {
		// Try pointer receiver methods if obj is not pointer
		if rv.CanAddr() {
			ptrVal := rv.Addr()
			methodVal = ptrVal.MethodByName(method)
		}

		if !methodVal.IsValid() {
			return nil, fmt.Errorf("method %s not found on type %T", method, obj)
		}
	}

	// Prepare arguments for reflection call
	methodType := methodVal.Type()
	if len(args) != methodType.NumIn() {
		return nil, fmt.Errorf("method %s expects %d arguments, got %d", method, methodType.NumIn(), len(args))
	}

	// Convert arguments to appropriate types
	reflectArgs := make([]reflect.Value, len(args))
	for i, arg := range args {
		argType := methodType.In(i)
		reflectArgs[i] = convertToReflectValue(arg, argType)
	}

	// Call the method
	results := methodVal.Call(reflectArgs)

	// Handle return values
	switch len(results) {
	case 0:
		return nil, nil
	case 1:
		result := results[0].Interface()
		return result, nil
	case 2:
		// Assume second return value is error
		result := results[0].Interface()
		if err := results[1].Interface(); err != nil {
			if e, ok := err.(error); ok {
				return nil, e
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("method %s returns too many values", method)
	}
}

// Convert Value to reflect.Value with appropriate type conversion
func convertToReflectValue(arg Value, targetType reflect.Type) reflect.Value {
	if arg == nil {
		return reflect.Zero(targetType)
	}

	argValue := reflect.ValueOf(arg)
	if argValue.Type().ConvertibleTo(targetType) {
		return argValue.Convert(targetType)
	}

	// Handle common conversions
	switch targetType.Kind() {
	case reflect.String:
		return reflect.ValueOf(toString(arg))
	case reflect.Int, reflect.Int64:
		return reflect.ValueOf(int64(toFloat64(arg))).Convert(targetType)
	case reflect.Float64:
		return reflect.ValueOf(toFloat64(arg))
	case reflect.Bool:
		return reflect.ValueOf(toBool(arg))
	default:
		return argValue
	}
}

func evaluateMacro(coll Value, variable string, body Expression, macroType string, ctx *Context) (Value, error) {
	// Convert collection to []Value with optimized path
	var items []Value
	switch c := coll.(type) {
	case []Value:
		items = c // Zero-allocation path for our native type
	default:
		rv := reflect.ValueOf(c)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			items = make([]Value, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				items[i] = rv.Index(i).Interface()
			}
		} else {
			return nil, fmt.Errorf("cannot iterate over non-collection")
		}
	}

	// For performance-critical operations, avoid context creation overhead
	switch macroType {
	case "size":
		return len(items), nil
	case "reverse":
		if len(items) <= 1 {
			return items, nil // Optimization for small collections
		}
		reversed := make([]Value, len(items))
		for i := 0; i < len(items); i++ {
			reversed[i] = items[len(items)-1-i]
		}
		return reversed, nil
	}

	// Create optimized context with pre-allocated variables map
	newCtx := &Context{
		Variables: make(map[string]Value, len(ctx.Variables)+1), // Pre-size for existing vars + loop var
		Functions: ctx.Functions,
	}

	// Copy existing variables in bulk
	for k, v := range ctx.Variables {
		newCtx.Variables[k] = v
	}

	switch macroType {
	case "all":
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			if !toBool(result) {
				return false, nil
			}
		}
		return true, nil

	case "exists":
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			if toBool(result) {
				return true, nil
			}
		}
		return false, nil

	case "filter":
		// Use ultra-fast filter for better performance
		return UltraFast.Filter(items, variable, body, ctx)

	case "map":
		// Use ultra-fast map for better performance
		return UltraFast.Map(items, variable, body, ctx)

	case "find":
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}
			if toBool(result) {
				return item, nil
			}
		}
		return nil, nil // Return null if no item found

	case "sort":
		// Simple sort by the expression result with optimized comparison
		sorted := make([]Value, len(items))
		copy(sorted, items)

		// Simple bubble sort for demonstration (could be improved with quicksort)
		for i := 0; i < len(sorted)-1; i++ {
			for j := 0; j < len(sorted)-i-1; j++ {
				newCtx.Variables[variable] = sorted[j]
				val1, err := body.Evaluate(newCtx)
				if err != nil {
					return nil, err
				}

				newCtx.Variables[variable] = sorted[j+1]
				val2, err := body.Evaluate(newCtx)
				if err != nil {
					return nil, err
				}

				if compare(val1, val2) > 0 {
					sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
				}
			}
		}
		return sorted, nil

	case "flatMap":
		// Pre-allocate with conservative estimate
		flattened := make([]Value, 0, len(items)*2)
		for _, item := range items {
			newCtx.Variables[variable] = item
			result, err := body.Evaluate(newCtx)
			if err != nil {
				return nil, err
			}

			// If result is a collection, add all elements
			if resultSlice := toValueSlice(result); resultSlice != nil {
				flattened = append(flattened, resultSlice...)
			} else {
				// If result is a single value, add it directly
				flattened = append(flattened, result)
			}
		}
		return flattened, nil
	}

	return nil, fmt.Errorf("unknown macro type: %s", macroType)
}

func toValueSlice(val Value) []Value {
	switch v := val.(type) {
	case []Value:
		return v
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			result := make([]Value, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				result[i] = rv.Index(i).Interface()
			}
			return result
		}
	}
	return nil
}

// Helper function to convert JSON values to our Value type
func convertJsonValue(v any) Value {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case float64:
		return val
	case string:
		return val
	case []any:
		result := make([]Value, len(val))
		for i, item := range val {
			result[i] = convertJsonValue(item)
		}
		return result
	case map[string]any:
		result := make(map[string]Value)
		for k, item := range val {
			result[k] = convertJsonValue(item)
		}
		return result
	default:
		return val
	}
}

func getType(val Value) string {
	if val == nil {
		return "null"
	}

	switch val.(type) {
	case bool:
		return "bool"
	case int, int64:
		return "int"
	case float64:
		return "double"
	case string:
		return "string"
	case []Value:
		return "list"
	case map[string]Value:
		return "map"
	case Duration:
		return "duration"
	case Timestamp:
		return "timestamp"
	case Bytes:
		return "bytes"
	case Optional:
		return "optional"
	default:
		return "unknown"
	}
}
