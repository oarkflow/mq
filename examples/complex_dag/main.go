package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
)

// subDAG1 creates a SubDAG
func subDAG1() *dag.DAG {
	f := dag.NewDAG("Sub DAG 1", "sub-dag-1", func(taskID string, result mq.Result) {
		fmt.Printf("Sub DAG 1 Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))
	f.
		AddNode(dag.Function, "Store Data", "store", &tasks.StoreData{Operation: dag.Operation{Type: dag.Function}}, true).
		AddNode(dag.Function, "Send SMS", "sms", &tasks.SendSms{Operation: dag.Operation{Type: dag.Function}}).
		AddEdge(dag.Simple, "Store to SMS", "store", "sms")
	return f
}

// subDAG2 creates another SubDAG
func subDAG2() *dag.DAG {
	f := dag.NewDAG("Sub DAG 2", "sub-dag-2", func(taskID string, result mq.Result) {
		fmt.Printf("Sub DAG 2 Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))
	f.
		AddNode(dag.Function, "Prepare Email", "prepare", &tasks.PrepareEmail{Operation: dag.Operation{Type: dag.Function}}, true).
		AddNode(dag.Function, "Email Delivery", "email", &tasks.EmailDelivery{Operation: dag.Operation{Type: dag.Function}}).
		AddEdge(dag.Simple, "Prepare to Email", "prepare", "email")
	return f
}

func main() {
	flow := dag.NewDAG("Complex Sample DAG", "complex-sample-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Complex DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	})
	flow.ConfigureMemoryStorage()

	// Main nodes
	flow.AddNode(dag.Function, "Get Data", "GetData", &GetData{}, true)
	flow.AddNode(dag.Function, "Main Loop", "MainLoop", &MainLoop{})
	flow.AddNode(dag.Function, "Validate", "Validate", &Validate{})
	flow.AddNode(dag.Function, "Process Valid", "ProcessValid", &ProcessValid{})
	flow.AddNode(dag.Function, "Process Invalid", "ProcessInvalid", &ProcessInvalid{})
	flow.AddDAGNode(dag.Function, "Sub DAG 1", "Sub1", subDAG1())
	flow.AddDAGNode(dag.Function, "Sub DAG 2", "Sub2", subDAG2())
	flow.AddNode(dag.Function, "Aggregate", "Aggregate", &Aggregate{})
	flow.AddNode(dag.Function, "Final", "Final", &Final{})

	// Edges
	flow.AddEdge(dag.Simple, "Start", "GetData", "MainLoop")
	flow.AddEdge(dag.Iterator, "Loop over data", "MainLoop", "Validate")
	flow.AddCondition("Validate", map[string]string{"valid": "ProcessValid", "invalid": "ProcessInvalid"})
	flow.AddEdge(dag.Simple, "Valid to Sub1", "ProcessValid", "Sub1")
	flow.AddEdge(dag.Simple, "Invalid to Sub2", "ProcessInvalid", "Sub2")
	flow.AddEdge(dag.Simple, "Sub1 to Aggregate", "Sub1", "Aggregate")
	flow.AddEdge(dag.Simple, "Sub2 to Aggregate", "Sub2", "Aggregate")
	flow.AddEdge(dag.Simple, "Main Loop to Final", "MainLoop", "Final")

	data := []byte(`[
		{"name": "Alice", "age": "25", "valid": true},
		{"name": "Bob", "age": "17", "valid": false},
		{"name": "Charlie", "age": "30", "valid": true}
	]`)
	if flow.Error != nil {
		panic(flow.Error)
	}

	rs := flow.Process(context.Background(), data)
	if rs.Error != nil {
		panic(rs.Error)
	}
	fmt.Println("Complex DAG Status:", rs.Status, "Topic:", rs.Topic)
	fmt.Println("Final Payload:", string(rs.Payload))
}

// Task implementations

type GetData struct {
	dag.Operation
}

func (p *GetData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type MainLoop struct {
	dag.Operation
}

func (p *MainLoop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type Validate struct {
	dag.Operation
}

func (p *Validate) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("Validate Error: %s", err.Error()), Ctx: ctx}
	}
	status := "invalid"
	if valid, ok := data["valid"].(bool); ok && valid {
		status = "valid"
	}
	data["validated"] = true
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx, ConditionStatus: status}
}

type ProcessValid struct {
	dag.Operation
}

func (p *ProcessValid) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("ProcessValid Error: %s", err.Error()), Ctx: ctx}
	}
	data["processed_valid"] = true
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type ProcessInvalid struct {
	dag.Operation
}

func (p *ProcessInvalid) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("ProcessInvalid Error: %s", err.Error()), Ctx: ctx}
	}
	data["processed_invalid"] = true
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type Aggregate struct {
	dag.Operation
}

func (p *Aggregate) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type Final struct {
	dag.Operation
}

func (p *Final) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data []map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("Final Error: %s", err.Error()), Ctx: ctx}
	}
	for i, row := range data {
		row["finalized"] = true
		data[i] = row
	}
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}
