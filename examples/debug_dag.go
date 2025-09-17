package main

import (
	"context"
	"fmt"
	"log"

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

	// Test without the Final node to see if it's causing the issue
	// Let's also enable hook to see the flow
	flow.SetPreProcessHook(func(ctx context.Context, node *dag.Node, taskID string, payload json.RawMessage) context.Context {
		log.Printf("PRE-HOOK: Processing node %s, taskID %s, payload size: %d", node.ID, taskID, len(payload))
		return ctx
	})

	flow.SetPostProcessHook(func(ctx context.Context, node *dag.Node, taskID string, result mq.Result) {
		log.Printf("POST-HOOK: Completed node %s, taskID %s, status: %v, payload size: %d", node.ID, taskID, result.Status, len(result.Payload))
	})

	data := []byte(`[{"age": "15", "gender": "female"}, {"age": "18", "gender": "male"}]`)
	if flow.Error != nil {
		panic(flow.Error)
	}

	rs := flow.Process(context.Background(), data)
	if rs.Error != nil {
		panic(rs.Error)
	}
	fmt.Println(rs.Status, rs.Topic, string(rs.Payload))
}

type GetData struct {
	dag.Operation
}

func (p *GetData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("GetData: Processing payload of size %d", len(task.Payload))
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type Loop struct {
	dag.Operation
}

func (p *Loop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("Loop: Processing payload of size %d", len(task.Payload))
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
	log.Printf("ValidateAge: Processing age %s, status %s", data["age"], status)
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
	log.Printf("ValidateGender: Processing gender %s", data["gender"])
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
	log.Printf("Final: Processing array with %d items", len(data))
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
