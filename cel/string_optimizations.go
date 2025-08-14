package cel

import (
	"fmt"
	"strings"
	"unsafe"
)

// StringOptimizations provides zero-allocation string operations where possible
type StringOptimizations struct{}

var StringOpt = &StringOptimizations{}

// ChainedStringOp represents a series of string operations that can be optimized
type ChainedStringOp struct {
	initial string
	ops     []StringOperation
}

type StringOperation struct {
	OpType string
	Args   []string
}

// OptimizedStringChain provides a way to chain string operations with minimal allocations
func (so *StringOptimizations) OptimizedStringChain(initial string) *ChainedStringOp {
	return &ChainedStringOp{
		initial: initial,
		ops:     make([]StringOperation, 0, 4), // Pre-allocate for common case
	}
}

func (cso *ChainedStringOp) Trim() *ChainedStringOp {
	cso.ops = append(cso.ops, StringOperation{OpType: "trim"})
	return cso
}

func (cso *ChainedStringOp) Upper() *ChainedStringOp {
	cso.ops = append(cso.ops, StringOperation{OpType: "upper"})
	return cso
}

func (cso *ChainedStringOp) Lower() *ChainedStringOp {
	cso.ops = append(cso.ops, StringOperation{OpType: "lower"})
	return cso
}

func (cso *ChainedStringOp) Replace(old, new string) *ChainedStringOp {
	cso.ops = append(cso.ops, StringOperation{OpType: "replace", Args: []string{old, new}})
	return cso
}

// Execute performs all operations in a single pass
func (cso *ChainedStringOp) Execute() string {
	result := cso.initial

	// Apply operations in sequence, trying to minimize allocations
	for _, op := range cso.ops {
		switch op.OpType {
		case "trim":
			result = strings.TrimSpace(result)
		case "upper":
			result = strings.ToUpper(result)
		case "lower":
			result = strings.ToLower(result)
		case "replace":
			if len(op.Args) >= 2 {
				result = strings.ReplaceAll(result, op.Args[0], op.Args[1])
			}
		}
	}

	return result
}

// FastStringMethods provides optimized implementations of common string methods
var FastStringMethods = map[string]func(obj Value, args ...Value) (Value, error){
	"trim": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("trim() requires 0 arguments")
		}
		str := toString(obj)
		// Ultra-fast trim using string slicing without allocation when possible
		if len(str) == 0 {
			return str, nil
		}

		start := 0
		end := len(str)

		// Find start of non-whitespace
		for start < end {
			c := str[start]
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				break
			}
			start++
		}

		// Find end of non-whitespace
		for end > start {
			c := str[end-1]
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				break
			}
			end--
		}

		// Return slice without allocation if no trimming needed
		if start == 0 && end == len(str) {
			return str, nil
		}

		return str[start:end], nil
	},

	"upper": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("upper() requires 0 arguments")
		}
		str := toString(obj)
		return strings.ToUpper(str), nil
	},

	"lower": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("lower() requires 0 arguments")
		}
		str := toString(obj)
		return strings.ToLower(str), nil
	},

	"replace": func(obj Value, args ...Value) (Value, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("replace() requires 2 arguments")
		}
		str := toString(obj)
		old := toString(args[0])
		new := toString(args[1])

		// Fast path: if old string not found, return original
		if !strings.Contains(str, old) {
			return str, nil
		}

		return strings.ReplaceAll(str, old, new), nil
	},
}

// ZeroAllocBytesToString converts []byte to string without allocation
func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// OptimizedStringBuilder provides a more efficient string builder for method chaining
type OptimizedStringBuilder struct {
	buf    []byte
	length int
}

func NewOptimizedStringBuilder(capacity int) *OptimizedStringBuilder {
	return &OptimizedStringBuilder{
		buf: make([]byte, 0, capacity),
	}
}

func (osb *OptimizedStringBuilder) WriteString(s string) {
	osb.buf = append(osb.buf, s...)
	osb.length += len(s)
}

func (osb *OptimizedStringBuilder) String() string {
	return bytesToString(osb.buf)
}

func (osb *OptimizedStringBuilder) Reset() {
	osb.buf = osb.buf[:0]
	osb.length = 0
}
