package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq/dag"

	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

func main() {
	flow := dag.NewDAG("Multi-Step Form", "multi-step-form", func(taskID string, result mq.Result) {
		fmt.Printf("Final result for task %s: %s\n", taskID, string(result.Payload))
	})
	flow.AddNode(dag.Page, "Form Step1", "FormStep1", &FormStep1{})
	flow.AddNode(dag.Page, "Form Step2", "FormStep2", &FormStep2{})
	flow.AddNode(dag.Page, "Form Result", "FormResult", &FormResult{})

	// Define edges
	flow.AddEdge(dag.Simple, "Form Step1", "FormStep1", "FormStep2")
	flow.AddEdge(dag.Simple, "Form Step2", "FormStep2", "FormResult")

	// Start the flow
	if flow.Error != nil {
		panic(flow.Error)
	}
	flow.Start(context.Background(), "0.0.0.0:8082")
}

type FormStep1 struct {
	dag.Operation
}

func (p *FormStep1) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	bt := []byte(`
<html>

<body>
<form method="post" action="/process?task_id={{task_id}}&next=true">
    <label>Name:</label>
    <input type="text" name="name" required>
    <label>Age:</label>
    <input type="number" name="age" required>
    <button type="submit">Next</button>
</form>
</body
</html

`)
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(string(bt), map[string]any{
		"task_id": ctx.Value("task_id"),
	})
	if err != nil {
		fmt.Println("FormStep1", string(task.Payload))
		return mq.Result{Error: err, Ctx: ctx}
	}
	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	data := map[string]any{
		"html_content": rs,
	}
	bt, _ = json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

type FormStep2 struct {
	dag.Operation
}

func (p *FormStep2) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Parse input from Step 1
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	// Determine dynamic content
	isEligible := inputData["age"] == "18"
	inputData["show_voting_controls"] = isEligible

	bt := []byte(`
<html>

<body>
<form method="post" action="/process?task_id={{task_id}}&next=true">
    {{ if show_voting_controls }}
        <label>Do you want to register to vote?</label>
        <input type="checkbox" name="register_vote">
        <button type="submit">Next</button>
    {{ else }}
        <p>You are not eligible to vote.</p>
    {{ end }}
</form>
</body>
</html>
`)
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	inputData["task_id"] = ctx.Value("task_id")
	rs, err := parser.ParseTemplate(string(bt), inputData)
	if err != nil {
		fmt.Println("FormStep2", inputData)
		return mq.Result{Error: err, Ctx: ctx}
	}
	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	inputData["html_content"] = rs
	bt, _ = json.Marshal(inputData)
	return mq.Result{Payload: bt, Ctx: ctx}
}

type FormResult struct {
	dag.Operation
}

func (p *FormResult) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Load HTML template for results
	bt := []byte(`
<html>

<body>
<h1>Form Summary</h1>
<p>Name: {{ name }}</p>
<p>Age: {{ age }}</p>
{{ if register_vote }}
    <p>You have registered to vote!</p>
{{ else }}
    <p>You did not register to vote.</p>
{{ end }}

</body>
</html>

`)
	var inputData map[string]any
	if task.Payload != nil {
		if err := json.Unmarshal(task.Payload, &inputData); err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
	}
	if inputData != nil {
		if isEligible, ok := inputData["register_vote"].(string); ok {
			inputData["register_vote"] = isEligible
		} else {
			inputData["register_vote"] = false
		}
	}
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(string(bt), inputData)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	inputData["html_content"] = rs
	bt, _ = json.Marshal(inputData)
	return mq.Result{Payload: bt, Ctx: ctx}
}
