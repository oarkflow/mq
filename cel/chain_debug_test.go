package cel

import (
	"testing"
)

func TestComplexChainDetection(t *testing.T) {
	ctx := setupCELContext()
	expression := "users.filter(u, u.salary > threshold).map(u, u.name.upper()).join(', ')"

	parser := NewParser(expression)
	expr, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	t.Logf("Expression type: %T", expr)
	if methodCall, ok := expr.(*MethodCall); ok {
		chain := detectMethodChain(methodCall)
		t.Logf("Chain detected: %+v", chain)
		if chain != nil {
			t.Logf("Base object: %T", chain.BaseObject)
			t.Logf("Methods: %v", len(chain.Methods))
			for i, method := range chain.Methods {
				t.Logf("Method %d: %s with %d args", i, method.Method, len(method.Args))
			}
		}
	}

	result, err := expr.Evaluate(ctx)
	if err != nil {
		t.Fatalf("Evaluation error: %v", err)
	}

	t.Logf("Result: %v", result)
}
