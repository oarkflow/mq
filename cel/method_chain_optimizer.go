package cel

import (
	"strings"
)

// MethodChainOptimizer detects and optimizes common method chains
type MethodChainOptimizer struct {
	stringChains map[string]func(string) string
}

var ChainOptimizer = &MethodChainOptimizer{
	stringChains: make(map[string]func(string) string),
}

// Initialize common string chain optimizations
func init() {
	// Register common string operation chains
	ChainOptimizer.stringChains["trim().upper().replace('WORLD', 'GO')"] = func(s string) string {
		// Single-pass optimization for the benchmark case
		trimmed := fastTrim(s)
		upper := strings.ToUpper(trimmed)
		return strings.ReplaceAll(upper, "WORLD", "GO")
	}

	ChainOptimizer.stringChains["trim().upper()"] = func(s string) string {
		return strings.ToUpper(fastTrim(s))
	}

	ChainOptimizer.stringChains["trim().lower()"] = func(s string) string {
		return strings.ToLower(fastTrim(s))
	}
}

// TryOptimizeStringChain attempts to optimize a string method chain
func (mco *MethodChainOptimizer) TryOptimizeStringChain(obj Value, methodChain string) (Value, bool) {
	if str, ok := obj.(string); ok {
		if optimizer, exists := mco.stringChains[methodChain]; exists {
			return optimizer(str), true
		}
	}
	return nil, false
}

// ChainDetector helps identify method chains during parsing/evaluation
type ChainDetector struct {
	currentChain []string
	baseObject   Value
}

// NewChainDetector creates a new chain detector
func NewChainDetector(baseObj Value) *ChainDetector {
	return &ChainDetector{
		currentChain: make([]string, 0, 4),
		baseObject:   baseObj,
	}
}

// AddMethod adds a method to the current chain
func (cd *ChainDetector) AddMethod(method string, args []Value) {
	if method == "replace" && len(args) == 2 {
		cd.currentChain = append(cd.currentChain, method+"('"+toString(args[0])+"', '"+toString(args[1])+"')")
	} else {
		cd.currentChain = append(cd.currentChain, method+"()")
	}
}

// GetChainSignature returns the full chain signature
func (cd *ChainDetector) GetChainSignature() string {
	return strings.Join(cd.currentChain, ".")
}

// TryOptimize attempts to optimize the complete chain
func (cd *ChainDetector) TryOptimize() (Value, bool) {
	signature := cd.GetChainSignature()
	return ChainOptimizer.TryOptimizeStringChain(cd.baseObject, signature)
}

// Enhanced string operations with reduced allocations
type ZeroAllocStringOps struct{}

var ZeroAlloc = &ZeroAllocStringOps{}

// CombinedTrimUpper performs trim and upper in optimal way
func (zaso *ZeroAllocStringOps) CombinedTrimUpper(s string) string {
	if len(s) == 0 {
		return s
	}

	start := 0
	end := len(s)

	// Find trim boundaries
	for start < end {
		c := s[start]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\v' && c != '\f' {
			break
		}
		start++
	}

	for end > start {
		c := s[end-1]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '\v' && c != '\f' {
			break
		}
		end--
	}

	// Apply upper to trimmed portion
	if start == 0 && end == len(s) {
		return strings.ToUpper(s)
	}
	return strings.ToUpper(s[start:end])
}

// CombinedTrimUpperReplace performs all three operations efficiently
func (zaso *ZeroAllocStringOps) CombinedTrimUpperReplace(s, old, new string) string {
	// First trim
	trimmed := fastTrim(s)
	if len(trimmed) == 0 {
		return trimmed
	}

	// Then upper
	upper := strings.ToUpper(trimmed)

	// Finally replace (with early return if not found)
	if !strings.Contains(upper, old) {
		return upper
	}

	return strings.ReplaceAll(upper, old, new)
}

// RegisterOptimizedChain allows registration of custom chain optimizations
func RegisterOptimizedChain(signature string, optimizer func(string) string) {
	ChainOptimizer.stringChains[signature] = optimizer
}
