package cel

import (
	"testing"
	"time"
)

func BenchmarkStringDebug_Trim(b *testing.B) {
	parser := NewParser(`"  hello world  ".trim()`)
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(NewContext())
	}
}

func BenchmarkStringDebug_Upper(b *testing.B) {
	parser := NewParser(`"hello world".upper()`)
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(NewContext())
	}
}

func BenchmarkStringDebug_Replace(b *testing.B) {
	parser := NewParser(`"hello world".replace("world", "go")`)
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(NewContext())
	}
}

func BenchmarkStringDebug_Chain(b *testing.B) {
	parser := NewParser(`"  hello world  ".trim().upper().replace("WORLD", "GO")`)
	expr, _ := parser.Parse()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(NewContext())
	}
}

func TestStringDebug(t *testing.T) {
	parser := NewParser(`"  hello world  ".trim().upper().replace("WORLD", "GO")`)
	expr, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	result, err := expr.Evaluate(NewContext())
	elapsed := time.Since(start)

	t.Logf("Result: %v, Error: %v, Time: %v", result, err, elapsed)

	// Let's see what the AST looks like
	t.Logf("Expression AST: %+v", expr)
}
