package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq"
	"os"

	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq/consts"
	v2 "github.com/oarkflow/mq/dag/v2"
)

type Form struct {
	v2.Operation
}

func (p *Form) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	bt, err := os.ReadFile("webroot/form.html")
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(string(bt), map[string]any{
		"task_id": ctx.Value("task_id"),
	})
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	data := map[string]any{
		"html_content": rs,
	}
	bt, _ = json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

type NodeA struct {
	v2.Operation
}

func (p *NodeA) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	data["allowed_voting"] = data["age"] == "18"
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type NodeB struct {
	v2.Operation
}

func (p *NodeB) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	data["female_voter"] = data["gender"] == "female"
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type NodeC struct {
	v2.Operation
}

func (p *NodeC) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	data["voted"] = true
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type Result struct {
	v2.Operation
}

func (p *Result) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	bt, err := os.ReadFile("webroot/result.html")
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	var data map[string]any
	if task.Payload != nil {
		if err := json.Unmarshal(task.Payload, &data); err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
	}
	if bt != nil {
		parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
		rs, err := parser.ParseTemplate(string(bt), data)
		if err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
		data := map[string]any{
			"html_content": rs,
		}
		bt, _ := json.Marshal(data)
		return mq.Result{Payload: bt, Ctx: ctx}
	}
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

func notify(taskID string, result mq.Result) {
	fmt.Printf("Final result for task %s: %s\n", taskID, string(result.Payload))
}

func main() {
	flow := v2.NewDAG("Sample DAG", "sample-dag", notify)
	flow.AddNode(v2.Page, "Form", "Form", &Form{})
	flow.AddNode(v2.Function, "NodeA", "NodeA", &NodeA{})
	flow.AddNode(v2.Function, "NodeB", "NodeB", &NodeB{})
	flow.AddNode(v2.Function, "NodeC", "NodeC", &NodeC{})
	flow.AddNode(v2.Page, "Result", "Result", &Result{})
	flow.AddEdge(v2.Simple, "Form", "Form", "NodeA")
	flow.AddEdge(v2.Simple, "NodeA", "NodeA", "NodeB")
	flow.AddEdge(v2.Simple, "NodeB", "NodeB", "NodeC")
	flow.AddEdge(v2.Simple, "NodeC", "NodeC", "Result")
	if flow.Error != nil {
		panic(flow.Error)
	}
	flow.Start(context.Background(), "0.0.0.0:8082")
}
