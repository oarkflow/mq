package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq/consts"
	v2 "github.com/oarkflow/mq/dag/v2"
)

func Form(ctx context.Context, payload json.RawMessage) v2.Result {
	bt, err := os.ReadFile("webroot/form.html")
	if err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(string(bt), map[string]any{
		"task_id": ctx.Value("task_id"),
	})
	if err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	data := map[string]any{
		"html_content": rs,
	}
	bt, _ = json.Marshal(data)
	return v2.Result{Data: bt, Ctx: ctx}
}

func NodeA(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Ctx: ctx}
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
	bt, err := os.ReadFile("webroot/result.html")
	if err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	if bt != nil {
		parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
		rs, err := parser.ParseTemplate(string(bt), data)
		if err != nil {
			return v2.Result{Error: err, Ctx: ctx}
		}
		ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
		data := map[string]any{
			"html_content": rs,
		}
		bt, _ := json.Marshal(data)
		return v2.Result{Data: bt, Ctx: ctx}
	}
	return v2.Result{Data: payload, Ctx: ctx}
}

func notify(taskID string, result v2.Result) {
	fmt.Printf("Final result for Task %s: %s\n", taskID, string(result.Data))
}

func main() {
	dag := v2.NewDAG(notify)
	dag.AddNode(v2.Page, "Form", Form)
	dag.AddNode(v2.Function, "NodeA", NodeA)
	dag.AddNode(v2.Function, "NodeB", NodeB)
	dag.AddNode(v2.Function, "NodeC", NodeC)
	dag.AddNode(v2.Page, "Result", Result)
	dag.AddEdge(v2.Simple, "Form", "NodeA")
	dag.AddEdge(v2.Simple, "NodeA", "NodeB")
	dag.AddEdge(v2.Simple, "NodeB", "NodeC")
	dag.AddEdge(v2.Simple, "NodeC", "Result")
	if dag.Error != nil {
		panic(dag.Error)
	}
	dag.Start("0.0.0.0:8080")
}
