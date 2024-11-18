package main

import (
	"encoding/json"
	"fmt"

	v2 "github.com/oarkflow/mq/dag/v2"
)

func main() {
	dag := v2.NewDAG(func(taskID string, result v2.Result) {
		fmt.Printf("Final result for Task %s: %s\n", taskID, string(result.Data))
	})
	dag.AddNode("NodeA", func(payload json.RawMessage) v2.Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return v2.Result{Error: err, Status: v2.StatusFailed}
		}
		data["allowed_voting"] = data["age"] == "18"
		updatedPayload, _ := json.Marshal(data)
		return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
	})
	dag.AddNode("NodeB", func(payload json.RawMessage) v2.Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return v2.Result{Error: err, Status: v2.StatusFailed}
		}
		data["female_voter"] = data["gender"] == "female"
		updatedPayload, _ := json.Marshal(data)
		return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
	})
	dag.AddNode("NodeC", func(payload json.RawMessage) v2.Result {
		var data map[string]any
		if err := json.Unmarshal(payload, &data); err != nil {
			return v2.Result{Error: err, Status: v2.StatusFailed}
		}
		data["voted"] = true
		updatedPayload, _ := json.Marshal(data)
		return v2.Result{Data: updatedPayload, Status: v2.StatusCompleted}
	})
	dag.AddNode("Result", func(payload json.RawMessage) v2.Result {
		var data map[string]any
		json.Unmarshal(payload, &data)

		return v2.Result{Data: payload, Status: v2.StatusCompleted}
	})
	dag.Start(":8080")
}
