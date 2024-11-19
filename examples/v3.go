package main

import (
	"context"
	"encoding/json"

	v2 "github.com/oarkflow/mq/dag/v2"
)

func main() {
	dag := v2.NewDAG(func(taskID string, result v2.Result) {
		// fmt.Printf("Final resuslt for Task %s: %s\n", taskID, string(result.Data))
	})
	dag.AddNode(v2.Function, "GetData", GetData, true)
	dag.AddNode(v2.Function, "Loop", Loop)
	dag.AddNode(v2.Function, "ValidateAge", ValidateAge)
	dag.AddNode(v2.Function, "ValidateGender", ValidateGender)
	dag.AddNode(v2.Function, "Final", Final)

	dag.AddEdge(v2.Simple, "GetData", "Loop")
	dag.AddEdge(v2.Iterator, "Loop", "ValidateAge")
	dag.AddEdge(v2.Simple, "ValidateAge", "ValidateGender")
	// dag.AddCondition("ValidateAge", map[string]string{"pass": "ValidateGender"})
	dag.AddEdge(v2.Simple, "Loop", "Final")

	data := []byte(`[{"age": "15", "gender": "female"}, {"age": "18", "gender": "male"}]`)
	if dag.Error != nil {
		panic(dag.Error)
	}
	dag.ProcessTask(context.Background(), data)
	// fmt.Println(rs.Status, rs.Topic, string(rs.Data))
}

func GetData(ctx context.Context, payload json.RawMessage) v2.Result {
	return v2.Result{Ctx: ctx, Data: payload}
}

func Loop(ctx context.Context, payload json.RawMessage) v2.Result {
	return v2.Result{Ctx: ctx, Data: payload}
}

func ValidateAge(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	var status string
	if data["age"] == "18" {
		status = "pass"
	} else {
		status = "default"
	}
	data["age_voter"] = data["age"] == "18"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Ctx: ctx, ConditionStatus: status}
}

func ValidateGender(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	data["female_voter"] = data["gender"] == "female"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Ctx: ctx}
}

func Final(ctx context.Context, payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Ctx: ctx}
	}
	data["done"] = true
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Ctx: ctx}
}
