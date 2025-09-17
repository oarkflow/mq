package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
)

func subDAG() *dag.DAG {
	f := dag.NewDAG("Sub DAG", "sub-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Sub DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))
	f.
		AddNode(dag.Function, "Store data", "store:data", &tasks.StoreData{Operation: dag.Operation{Type: dag.Function}}, true).
		AddNode(dag.Function, "Send SMS", "send:sms", &tasks.SendSms{Operation: dag.Operation{Type: dag.Function}}).
		AddNode(dag.Function, "Notification", "notification", &tasks.InAppNotification{Operation: dag.Operation{Type: dag.Function}}).
		AddEdge(dag.Simple, "Store Payload to send sms", "store:data", "send:sms").
		AddEdge(dag.Simple, "Store Payload to notification", "send:sms", "notification")
	return f
}

func main() {
	flow := dag.NewDAG("Sample DAG", "sample-dag", func(taskID string, result mq.Result) {
		fmt.Printf("DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	})
	flow.ConfigureMemoryStorage()
	flow.AddNode(dag.Function, "GetData", "GetData", &GetData{}, true)
	flow.AddNode(dag.Function, "Loop", "Loop", &Loop{})
	flow.AddNode(dag.Function, "ValidateAge", "ValidateAge", &ValidateAge{})
	flow.AddNode(dag.Function, "ValidateGender", "ValidateGender", &ValidateGender{})
	flow.AddNode(dag.Function, "Final", "Final", &Final{})
	flow.AddDAGNode(dag.Function, "Check", "persistent", subDAG())
	flow.AddEdge(dag.Simple, "GetData", "GetData", "Loop")
	flow.AddEdge(dag.Iterator, "Validate age for each item", "Loop", "ValidateAge")
	flow.AddCondition("ValidateAge", map[string]string{"pass": "ValidateGender", "default": "persistent"})
	flow.AddEdge(dag.Simple, "Mark as Done", "Loop", "Final")

	// flow.Start(":8080")
	data := []byte(`[{"age": "15", "gender": "female"}, {"age": "18", "gender": "male"}]`)
	if flow.Error != nil {
		panic(flow.Error)
	}

	rs := flow.Process(context.Background(), data)
	if rs.Error != nil {
		panic(rs.Error)
	}
	time.Sleep(1 * time.Millisecond)
	fmt.Println(rs.Status, rs.Topic, string(rs.Payload))
}

type GetData struct {
	dag.Operation
}

func (p *GetData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type Loop struct {
	dag.Operation
}

func (p *Loop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type ValidateAge struct {
	dag.Operation
}

func (p *ValidateAge) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("ValidateAge Error: %s", err.Error()), Ctx: ctx}
	}
	var status string
	if data["age"] == "18" {
		status = "pass"
	} else {
		status = "default"
	}
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx, ConditionStatus: status}
}

type ValidateGender struct {
	dag.Operation
}

func (p *ValidateGender) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("ValidateGender Error: %s", err.Error()), Ctx: ctx}
	}
	data["female_voter"] = data["gender"] == "female"
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
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
		row["done"] = true
		data[i] = row
	}
	updatedPayload, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}
