package main

import (
	"context"
	"encoding/json"
	"fmt"
	v2 "github.com/oarkflow/mq/v2"
)

func handler1(ctx context.Context, task *v2.Task) v2.Result {
	return v2.Result{
		TaskID:  task.ID,
		NodeKey: "A",
		Payload: task.Payload,
		Status:  "success",
	}
}

func handler2(ctx context.Context, task *v2.Task) v2.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	return v2.Result{
		TaskID:  task.ID,
		NodeKey: "B",
		Payload: task.Payload,
		Status:  "success",
	}
}

func handler3(ctx context.Context, task *v2.Task) v2.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	age := int(user["age"].(float64))
	status := "FAIL"
	if age > 20 {
		status = "PASS"
	}
	user["status"] = status
	resultPayload, _ := json.Marshal(user)
	return v2.Result{
		TaskID:  task.ID,
		NodeKey: "C",
		Payload: resultPayload,
		Status:  status,
	}
}

func main() {
	dag := v2.NewDAG()
	dag.AddNode("A", handler1)
	dag.AddNode("B", handler2)
	dag.AddNode("C", handler3)
	dag.AddEdge("A", "B", v2.LoopEdge)
	dag.AddEdge("B", "C", v2.SimpleEdge)
	initialPayload, _ := json.Marshal([]map[string]any{
		{"user_id": 1, "age": 12},
		{"user_id": 2, "age": 34},
	})
	rs := dag.ProcessTask(context.Background(), "A", initialPayload)
	fmt.Println(string(rs.Payload))
}
