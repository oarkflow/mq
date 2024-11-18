package main

import (
	"encoding/json"
	"fmt"

	v2 "github.com/oarkflow/mq/dag/v2"
)

func NodeA(payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Status: v2.StatusFailed}
	}
	data["allowed_voting"] = data["age"] == "18"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
}

func NodeB(payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Status: v2.StatusFailed}
	}
	data["female_voter"] = data["gender"] == "female"
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
}

func NodeC(payload json.RawMessage) v2.Result {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return v2.Result{Error: err, Status: v2.StatusFailed}
	}
	data["voted"] = true
	updatedPayload, _ := json.Marshal(data)
	return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
}

func Result(payload json.RawMessage) v2.Result {
	var data map[string]any
	json.Unmarshal(payload, &data)

	return v2.Result{Data: payload, Status: v2.StatusCompleted}
}

func notify(taskID string, result v2.Result) {
	fmt.Printf("Final result for Task %s: %s\n", taskID, string(result.Data))
}

func main() {
	dag := v2.NewDAG(notify)
	dag.AddNode("NodeA", NodeA)
	dag.AddNode("NodeB", NodeB)
	dag.AddNode("NodeC", NodeC)
	dag.AddNode("Result", Result)
	dag.Start(":8080")
}
