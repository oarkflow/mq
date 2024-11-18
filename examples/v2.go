package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq/consts"
	v2 "github.com/oarkflow/mq/dag/v2"
)

func Form(ctx context.Context, payload json.RawMessage) v2.Result {
	template := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>User Data Form</title>
</head>
<body>
<h1>Enter Your Information</h1>
<form action="/?task_id={{task_id}}&next=true" method="POST">
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

`
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(template, map[string]any{
		"task_id": ctx.Value("task_id"),
	})
	if err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	return v2.Result{Data: []byte(rs), Ctx: ctx}
}

func NodeA(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err}
	}
	data["allowed_voting"] = data["age"] == "18"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Ctx: ctx}
}

func NodeB(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	data["female_voter"] = data["gender"] == "female"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Ctx: ctx}
}

func NodeC(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	data["voted"] = true
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Ctx: ctx}
}

func Result(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	if templateFile, ok := data["html_content"].(string); ok {
		parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
		rs, err := parser.ParseTemplate(templateFile, data)
		if err != nil {
			return v2.Result{Error: err, Ctx: ctx}
		}
		ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
		return v2.Result{Data: []byte(rs), Ctx: ctx}
	}
	return v2.Result{Data: payload, Ctx: ctx}
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
