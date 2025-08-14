package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/mq/cel"
)

// Main function with examples
func main() {
	fmt.Println("🚀 Proper CEL Expression Language Implementation")
	fmt.Println("===============================================")

	ctx := cel.NewContext()

	// Set up test data
	users := []cel.Value{
		map[string]cel.Value{
			"id":         1,
			"name":       "Alice Johnson",
			"age":        30,
			"email":      "alice@techcorp.com",
			"roles":      []cel.Value{"admin", "user", "developer"},
			"active":     true,
			"salary":     85000.0,
			"department": "Engineering",
		},
		map[string]cel.Value{
			"id":         2,
			"name":       "Bob Smith",
			"age":        25,
			"email":      "bob@startup.io",
			"roles":      []cel.Value{"user", "intern"},
			"active":     true,
			"salary":     45000.0,
			"department": "Marketing",
		},
	}

	ctx.Set("users", users)
	ctx.Set("threshold", 50000.0)

	fmt.Println("\n📊 Basic Data Access Examples")
	fmt.Println("============================")

	testExpressions(ctx, []string{
		"users[0].name",
		"users[1].age",
		"users[0].salary + users[1].salary",
		"length(users)",
		"users[0].salary > threshold",
	})

	fmt.Println("\n🔍 Collection Operations Examples")
	fmt.Println("=================================")

	testExpressions(ctx, []string{
		"users.filter(u, u.age > 25)",
		"users.map(u, u.name)",
		"users.map(u, u.salary * 1.1)",
		"users.all(u, u.age >= 18)",
		"users.exists(u, u.salary > 80000)",
		"users.find(u, u.department == 'Engineering')",
		"users.find(u, u.age < 20)",
	})

	fmt.Println("\n🧮 Math and String Examples")
	fmt.Println("===========================")

	testExpressions(ctx, []string{
		"2 + 3 * 4",
		"(2 + 3) * 4",
		"abs(-42)",
		"upper('hello world')",
		"lower('HELLO WORLD')",
		"ceil(3.2)",
		"floor(3.7)",
		"round(3.6)",
		"min(5, 3, 8, 1)",
		"max(5, 3, 8, 1)",
		"sqrt(16)",
		"pow(2, 3)",
	})

	fmt.Println("\n🔧 Advanced String Operations")
	fmt.Println("=============================")

	ctx.Set("text", "  Hello, World!  ")
	testExpressions(ctx, []string{
		"text.trim()",
		"text.upper()",
		"text.lower()",
		"text.replace('World', 'Go')",
		"text.split(',')",
		"'admin' in ['admin', 'user']",
		"split('a,b,c', ',')",
		"replace('hello world', 'world', 'Go')",
	})

	fmt.Println("\n📋 Advanced Collection Operations")
	fmt.Println("=================================")

	numbers := []cel.Value{3, 1, 4, 1, 5, 9, 2, 6}
	ctx.Set("numbers", numbers)

	testExpressions(ctx, []string{
		"numbers.size()",
		"numbers.reverse()",
		"numbers.sort(n, n)",
		"users.size()",
		"users.reverse()",
	})

	fmt.Println("\n🏷️  Type System Examples")
	fmt.Println("=========================")

	testExpressions(ctx, []string{
		"type(42)",
		"type('hello')",
		"type(true)",
		"type([1, 2, 3])",
		"type(users[0])",
		"int(3.14)",
		"double(42)",
		"string(123)",
	})

	fmt.Println("\n🔀 Conditional (Ternary) Expressions")
	fmt.Println("===================================")

	testExpressions(ctx, []string{
		"true ? 'yes' : 'no'",
		"false ? 'yes' : 'no'",
		"users[0].age > 25 ? 'senior' : 'junior'",
		"length(users) > 1 ? 'multiple users' : 'single user'",
		"users[0].salary > threshold ? 'high' : 'low'",
	})

	fmt.Println("\n🚀 Testing Extended Method System")
	fmt.Println("=================================")

	// Test the new high-performance methods
	testExpressions(ctx, []string{
		"users.map(u, u.name).join(', ')",
		"users.map(u, u.age).first()",
		"users.map(u, u.age).last()",
		"split('a,b,c,d', ',').join(' | ')",
		"users.map(u, split(u.name, ' ')).first().join('-')",
	})

	fmt.Println("\n🔧 Method Chaining Performance Test")
	fmt.Println("==================================")

	// Test complex chaining that should be highly optimized
	testExpressions(ctx, []string{
		"users.map(u, u.name).split(' ').map(parts, parts.first()).join(', ')",
		"users.map(u, u.name).split(' ').reverse().join(' ')",
		"users.flatMap(u, split(u.name, ' ')).reverse().join(' -> ')",
	})

	fmt.Printf("\n📊 Registered Methods: %v\n", cel.GetRegisteredFunctions())

	fmt.Println("\n🚀 Advanced Features Examples")
	fmt.Println("=============================")

	// Advanced types examples
	testExpressions(ctx, []string{
		"duration('1h30m')",
		"duration('2m45s').seconds()",
		"duration('1h').minutes()",
		"timestamp('2024-01-15T10:30:00Z')",
		"now().year()",
		"now().month()",
		"timestamp('2024-12-25').format('2006-01-02')",
		"bytes('hello world').decode()",
		"'test string'.encode().decode()",
	})

	fmt.Println("\n🔍 List Comprehensions")
	fmt.Println("=====================")

	testExpressions(ctx, []string{
		"[u.name | u in users]",
		"[u.name | u in users, u.age > 25]",
		"[u.salary * 1.1 | u in users, u.department == 'Engineering']",
		"[n * 2 | n in [1, 2, 3, 4, 5], n % 2 == 0]",
		"[x + 1 | x in [10, 20, 30]]",
	})

	fmt.Println("\n🧬 Advanced Collection Operations")
	fmt.Println("================================")

	duplicates := []cel.Value{1, 2, 2, 3, 3, 3, 4}
	nested := []cel.Value{[]cel.Value{1, 2}, []cel.Value{3, 4}, []cel.Value{5, 6}}
	deepNested := []cel.Value{1, []cel.Value{2, 3}, []cel.Value{4, []cel.Value{5, 6}}}
	ctx.Set("duplicates", duplicates)
	ctx.Set("nested", nested)
	ctx.Set("deepNested", deepNested)

	testExpressions(ctx, []string{
		"duplicates.distinct()",
		"nested.flatten()",
		"users.map(u, u.age).distinct()",
		"deepNested.flatten()",
		"users.map(u, u.name).sortBy('identity')",
	})

	fmt.Println("\n🔤 Regex Operations")
	fmt.Println("==================")

	testExpressions(ctx, []string{
		"matches('hello@example.com', '^[\\w._%+-]+@[\\w.-]+\\.[a-zA-Z]{2,}$')",
		"findAll('Phone: 123-456-7890 or 987-654-3210', '\\d{3}-\\d{3}-\\d{4}')",
		"replaceRegex('Hello World 123', '\\d+', 'NUMBER')",
		"'test@email.com'.matches('[\\w]+@[\\w]+\\.[\\w]+')",
	})

	fmt.Println("\n📄 JSON Operations")
	fmt.Println("==================")

	jsonData := map[string]cel.Value{
		"name": "John Doe",
		"age":  30,
		"city": "New York",
	}
	ctx.Set("data", jsonData)

	testExpressions(ctx, []string{
		"toJson(data)",
		"fromJson('{\"message\": \"hello\", \"count\": 42}')",
		"toJson([1, 2, 3])",
		"fromJson('[\"a\", \"b\", \"c\"]')",
	})

	fmt.Println("\n⏰ Date/Time Operations")
	fmt.Println("=======================")

	ctx.Set("birthday", cel.Timestamp{T: time.Date(1990, 5, 15, 14, 30, 0, 0, time.UTC)})
	ctx.Set("oneHour", cel.Duration{D: time.Hour})

	testExpressions(ctx, []string{
		"date(2024, 12, 25)",
		"now().format('2006-01-02 15:04:05')",
		"birthday.year()",
		"birthday.month()",
		"birthday.day()",
		"addDuration(birthday, oneHour)",
		"formatTime(birthday, 'Monday, January 2, 2006')",
	})

	fmt.Println("\n🛡️ Optional Types & Error Handling")
	fmt.Println("==================================")

	ctx.Set("maybeValue", cel.Optional{Value: "found", Valid: true})
	ctx.Set("emptyValue", cel.Optional{Value: nil, Valid: false})

	testExpressions(ctx, []string{
		"maybeValue.hasValue()",
		"emptyValue.hasValue()",
		"maybeValue.orElse('default')",
		"emptyValue.orElse('default')",
		"optional('test').hasValue()",
		"optional(null).orElse('fallback')",
	})

	fmt.Println("\n🚀 Complex Nested Examples")
	fmt.Println("===========================")

	// Add more complex test data
	projects := []cel.Value{
		map[string]cel.Value{
			"name":   "Project Alpha",
			"team":   []cel.Value{"Alice", "Bob"},
			"budget": 100000.0,
			"status": "active",
		},
		map[string]cel.Value{
			"name":   "Project Beta",
			"team":   []cel.Value{"Alice", "Charlie"},
			"budget": 75000.0,
			"status": "completed",
		},
	}
	ctx.Set("projects", projects)

	testExpressions(ctx, []string{
		"projects.filter(p, p.budget > 80000).map(p, p.name)",
		"projects.all(p, length(p.team) >= 2)",
		"projects.exists(p, p.status == 'active') ? 'has active' : 'all done'",
		"users.map(u, u.name)",
		"users.filter(u, u.age > 25).size() > 0 ? 'found seniors' : 'no seniors'",
		"users.map(u, split(u.name, ' '))",
		"users.flatMap(u, split(u.name, ' '))",
		"users.flatMap(u, split(u.name, ' ')).filter(word, length(word) > 3)",
		"users.map(u, split(u.name, ' ')[0])",
		"users.map(u, u.name).split(' ')",
		"users.map(u, u.name).split(' ').map(parts, parts[0])",
	})

	fmt.Println("\n🚀 All tests completed!")

	// Print comprehensive feature summary
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📋 COMPREHENSIVE CEL FEATURE SUMMARY")
	fmt.Println(strings.Repeat("=", 60))
}

// Helper function to test expressions
func testExpressions(ctx *cel.Context, expressions []string) {
	for _, exprStr := range expressions {
		fmt.Printf("Expression: %s\n", exprStr)

		// Parse and evaluate the expression properly
		parser := cel.NewParser(exprStr)
		expr, err := parser.Parse()
		if err != nil {
			fmt.Printf("  Parse Error: %v\n", err)
		} else {
			result, err := expr.Evaluate(ctx)
			if err != nil {
				fmt.Printf("  Eval Error: %v\n", err)
			} else {
				fmt.Printf("  Result: %v\n", formatResult(result))
			}
		}
		fmt.Println()
	}
}

// Helper function to format results nicely
func formatResult(result cel.Value) string {
	if result == nil {
		return "null"
	}

	switch v := result.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", v)
	case []cel.Value:
		if len(v) == 0 {
			return "[]"
		}
		var items []string
		for i, item := range v {
			if i >= 3 { // Limit display to first 3 items
				items = append(items, "...")
				break
			}
			items = append(items, formatResult(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	case map[string]cel.Value:
		return fmt.Sprintf("map[%d keys]", len(v))
	default:
		return fmt.Sprintf("%v", v)
	}
}
