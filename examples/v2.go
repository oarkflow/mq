package main

import (
	"context"
	"fmt"
	"os"

	"github.com/oarkflow/json"

	"github.com/gofiber/fiber/v2"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"

	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq/consts"
)

type Form struct {
	dag.Operation
}

func (p *Form) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	baseURI := ""
	if dg, ok := task.GetFlow().(*dag.DAG); ok {
		baseURI = dg.BaseURI()
	}
	bt, err := os.ReadFile("webroot/form.html")
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(string(bt), map[string]any{
		"task_id":  ctx.Value("task_id"),
		"base_uri": baseURI,
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
	dag.Operation
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
	dag.Operation
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
	dag.Operation
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
	dag.Operation
}

func (p *Result) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	baseURI := ""
	if dg, ok := task.GetFlow().(*dag.DAG); ok {
		baseURI = dg.BaseURI()
	}
	bt, err := os.ReadFile("webroot/result.html")
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	data := map[string]any{
		"base_uri": baseURI,
	}
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

// RemoveHTMLContent recursively removes the "html_content" field from the given JSON.
func RemoveHTMLContent(data json.RawMessage, field string) (json.RawMessage, error) {
	var result interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	removeField(result, field)
	return json.Marshal(result)
}

// removeField recursively traverses the structure and removes "html_content" field.
func removeField(v interface{}, field string) {
	switch v := v.(type) {
	case map[string]interface{}:
		// Check if the field is in the map and remove it.
		delete(v, field)
		// Recursively remove the field from nested objects.
		for _, value := range v {
			removeField(value, field)
		}
	case []interface{}:
		// If it's an array, recursively process each item.
		for _, item := range v {
			removeField(item, field)
		}
	}
}

func notify(taskID string, result mq.Result) {
	filteredData, err := RemoveHTMLContent(result.Payload, "html_content")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Final result for task %s: %s, status: %s, latency: %s\n", taskID, string(filteredData), result.Status, result.Latency)
}

func main() {
	flow := dag.NewDAG("Sample DAG", "sample-dag", notify, mq.WithBrokerURL(":8083"), mq.WithHTTPApi(true))
	flow.AddNode(dag.Page, "Form", "Form", &Form{})
	flow.AddNode(dag.Function, "NodeA", "NodeA", &NodeA{})
	flow.AddNode(dag.Function, "NodeB", "NodeB", &NodeB{})
	flow.AddNode(dag.Function, "NodeC", "NodeC", &NodeC{})
	flow.AddNode(dag.Page, "Result", "Result", &Result{})
	flow.AddEdge(dag.Simple, "Form", "Form", "NodeA")
	flow.AddEdge(dag.Simple, "NodeA", "NodeA", "NodeB")
	flow.AddEdge(dag.Simple, "NodeB", "NodeB", "NodeC")
	flow.AddEdge(dag.Simple, "NodeC", "NodeC", "Result")
	dag.AddHandler("Form", func(s string) mq.Processor {
		opt := dag.Operation{
			Tags: []string{"built-in", "form"},
		}
		return &Form{Operation: opt}
	})
	fmt.Println(dag.AvailableHandlers())
	if flow.Error != nil {
		panic(flow.Error)
	}
	app := fiber.New()
	flowApp := app.Group("/")
	flow.Handlers(flowApp, "/")
	app.Listen(":8082")
}
