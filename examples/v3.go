package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq"
	v2 "github.com/oarkflow/mq/dag/v2"
)

func main() {
	flow := v2.NewDAG("Sample DAG", "sample-dag", func(taskID string, result mq.Result) {
		// fmt.Printf("Final result for task %s: %s\n", taskID, string(result.Payload))
	})
	flow.AddNode(v2.Function, "GetData", "GetData", &GetData{}, true)
	flow.AddNode(v2.Function, "Loop", "Loop", &Loop{})
	flow.AddNode(v2.Function, "ValidateAge", "ValidateAge", &ValidateAge{})
	flow.AddNode(v2.Function, "ValidateGender", "ValidateGender", &ValidateGender{})
	flow.AddNode(v2.Function, "Final", "Final", &Final{})

	flow.AddEdge(v2.Simple, "GetData", "GetData", "Loop")
	flow.AddEdge(v2.Iterator, "Validate age for each item", "Loop", "ValidateAge")
	flow.AddCondition("ValidateAge", map[string]string{"pass": "ValidateGender"})
	flow.AddEdge(v2.Simple, "Mark as Done", "Loop", "Final")

	// flow.Start(":8080")
	data := []byte(`[{"age": "15", "gender": "female"}, {"age": "18", "gender": "male"}]`)
	if flow.Error != nil {
		panic(flow.Error)
	}

	fmt.Println(flow.ExportDOT())
	rs := flow.Process(context.Background(), data)
	if rs.Error != nil {
		panic(rs.Error)
	}
	fmt.Println(rs.Status, rs.Topic, string(rs.Payload))
}

type GetData struct {
	v2.Operation
}

func (p *GetData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type Loop struct {
	v2.Operation
}

func (p *Loop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Ctx: ctx, Payload: task.Payload}
}

type ValidateAge struct {
	v2.Operation
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
	v2.Operation
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
	v2.Operation
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
