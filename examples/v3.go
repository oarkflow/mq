package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"

	v2 "github.com/oarkflow/mq/dag/v2"
)

func main() {
	dag := v2.NewDAG(func(taskID string, result mq.Result) {
		// fmt.Printf("Final result for task %s: %s\n", taskID, string(result.Payload))
	})
	dag.AddNode(v2.Function, "GetData", "GetData", &GetData{}, true)
	dag.AddNode(v2.Function, "Loop", "Loop", &Loop{})
	dag.AddNode(v2.Function, "ValidateAge", "ValidateAge", &ValidateAge{})
	dag.AddNode(v2.Function, "ValidateGender", "ValidateGender", &ValidateGender{})
	dag.AddNode(v2.Function, "Final", "Final", &Final{})

	dag.AddEdge(v2.Simple, "GetData", "Loop")
	dag.AddEdge(v2.Iterator, "Loop", "ValidateAge")
	dag.AddCondition("ValidateAge", map[string]string{"pass": "ValidateGender"})
	dag.AddEdge(v2.Simple, "Loop", "Final")

	// dag.Start(":8080")
	data := []byte(`[{"age": "15", "gender": "female"}, {"age": "18", "gender": "male"}]`)
	if dag.Error != nil {
		panic(dag.Error)
	}

	rs := dag.Process(context.Background(), data)
	if rs.Error != nil {
		panic(rs.Error)
	}
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
