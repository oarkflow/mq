package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/jet"

	v2 "github.com/oarkflow/mq/dag/v2"
)

func Form(ctx context.Context, payload json.RawMessage) v2.Result {
	template := []byte(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>User Data Form</title>
</head>
<body>
<h1>Enter Your Information</h1>
<form action="/form" method="POST">
    <label for="email">Email:</label><br>
    <input type="email" id="email" name="email" value="s.baniya.np@gmail.com" required><br><br>

    <label for="age">Age:</label><br>
    <input type="number" id="age" name="age" value="18" required><br><br>

    <label for="gender">Gender:</label><br>
    <select id="gender" name="gender" required>
        <option value="male">Male</option>
        <option value="female">Female</option>
        <option value="other">Other</option>
    </select><br><br>

    <input type="submit" value="Submit">
</form>
</body>
</html>

`)
	return v2.Result{Data: template, Status: v2.StatusCompleted}
}

func NodeA(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Status: v2.StatusFailed}
	}
	data["allowed_voting"] = data["age"] == "18"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
}

func NodeB(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Status: v2.StatusFailed}
	}
	data["female_voter"] = data["gender"] == "female"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
}

func NodeC(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Status: v2.StatusFailed}
	}
	data["voted"] = true
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
}

func Result(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Status: v2.StatusFailed}
	}
	if templateFile, ok := data["html_content"].(string); ok {
		parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
		rs, err := parser.ParseTemplate(templateFile, data)
		if err != nil {
			return v2.Result{Error: err, Status: v2.StatusFailed}
		}
		ctx = context.WithValue(ctx, "Content-Type", "text/html; charset/utf-8")
		return v2.Result{Data: []byte(rs), Status: v2.StatusCompleted, Ctx: ctx}
	}
	return v2.Result{Data: payload, Status: v2.StatusCompleted}
}

func notify(taskID string, result v2.Result) {
	fmt.Printf("Final result for Task %s: %s\n", taskID, string(result.Data))
}

func main() {
	dag := v2.NewDAG(notify)
	dag.AddNode(v2.Page, "Form", Form)
	dag.AddNode(v2.Process, "NodeA", NodeA)
	dag.AddNode(v2.Process, "NodeB", NodeB)
	dag.AddNode(v2.Process, "NodeC", NodeC)
	dag.AddNode(v2.Page, "Result", Result)
	dag.AddEdge("Form", "NodeA")
	dag.AddEdge("NodeA", "NodeB")
	dag.AddEdge("NodeB", "NodeC")
	dag.AddEdge("NodeC", "Result")
	if dag.Error != nil {
		panic(dag.Error)
	}
	dag.Start(":8080")
}
