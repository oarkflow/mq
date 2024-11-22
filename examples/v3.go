package main

import (
	"context"
	"encoding/json"
	"fmt"

	v2 "github.com/oarkflow/mq/dag/v2"
)

func main() {
	dag := v2.NewDAG(func(taskID string, result v2.Result) {
		// fmt.Printf("Final result for Task %s: %s\n", taskID, string(result.Payload))
	})
	dag.AddNode(v2.Function, "GetData", GetData, true)
	dag.AddNode(v2.Function, "Loop", Loop)
	dag.AddNode(v2.Function, "ValidateAge", ValidateAge)
	dag.AddNode(v2.Function, "ValidateGender", ValidateGender)
	dag.AddNode(v2.Function, "Final", Final)

	dag.AddEdge(v2.Simple, "GetData", "Loop")
	dag.AddEdge(v2.Iterator, "Loop", "ValidateAge")
	dag.AddCondition("ValidateAge", map[string]string{"pass": "ValidateGender"})
	dag.AddEdge(v2.Simple, "Loop", "Final")

	// dag.Start(":8080")
	data := []byte(`[{"age": "15", "gender": "female"}, {"age": "18", "gender": "male"}]`)
	if dag.Error != nil {
		panic(dag.Error)
	}

	rs := dag.ProcessTask(context.Background(), data)
	if rs.Error != nil {
		panic(rs.Error)
	}
	fmt.Println(rs.Status, rs.Topic, string(rs.Payload))
}

func GetData(ctx context.Context, payload json.RawMessage) v2.Result {
	return v2.Result{Ctx: ctx, Payload: payload}
}

func Loop(ctx context.Context, payload json.RawMessage) v2.Result {
	return v2.Result{Ctx: ctx, Payload: payload}
}

func ValidateAge(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: fmt.Errorf("ValidateAge Error: %s", err.Error()), Ctx: ctx}
	}
	var status string
	if data["age"] == "18" {
		status = "pass"
	} else {
		status = "default"
	}
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Payload: updatedPayload, Ctx: ctx, ConditionStatus: status}
}

func ValidateGender(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: fmt.Errorf("ValidateGender Error: %s", err.Error()), Ctx: ctx}
	}
	data["female_voter"] = data["gender"] == "female"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Payload: updatedPayload, Ctx: ctx}
}

func Final(ctx context.Context, payload json.RawMessage) v2.Result {
	var data []map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: fmt.Errorf("Final Error: %s", err.Error()), Ctx: ctx}
	}
	for i, row := range data {
		row["done"] = true
		data[i] = row
	}
	updatedPayload, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return v2.Result{Payload: updatedPayload, Ctx: ctx}
}
