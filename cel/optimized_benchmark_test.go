package cel

import (
	"testing"
)

func BenchmarkStringOptimized_ParseOnly(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parser := NewParser(`"  hello world  ".trim().upper().replace("WORLD", "GO")`)
		_, _ = parser.Parse()
	}
}

func BenchmarkStringOptimized_EvaluateOnly(b *testing.B) {
	parser := NewParser(`"  hello world  ".trim().upper().replace("WORLD", "GO")`)
	expr, _ := parser.Parse()
	ctx := NewContext()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = expr.Evaluate(ctx)
	}
}

func BenchmarkStringOptimized_Combined(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parser := NewParser(`"  hello world  ".trim().upper().replace("WORLD", "GO")`)
		expr, _ := parser.Parse()
		_, _ = expr.Evaluate(NewContext())
	}
}

func BenchmarkDirectStringOps_Manual(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := ZeroAlloc.CombinedTrimUpperReplace("  hello world  ", "WORLD", "GO")
		_ = result
	}
}
