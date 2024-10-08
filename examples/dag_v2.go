package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/v2"
)

func handler1(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

func handler2(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

func handler3(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	age := int(user["age"].(float64))
	status := "FAIL"
	if age > 20 {
		status = "PASS"
	}
	user["status"] = status
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload, Status: status, Ctx: ctx}
}

func handler4(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	user["final"] = "D"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

func handler5(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	user["salary"] = "E"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

func handler6(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	resultPayload, _ := json.Marshal(map[string]any{"storage": user})
	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

func main() {
	dag := v2.NewDAG()
	dag.AddNode("A", handler1)
	dag.AddNode("B", handler2)
	dag.AddNode("C", handler3)
	dag.AddNode("D", handler4)
	dag.AddNode("E", handler5)
	dag.AddNode("F", handler6)
	dag.AddEdge("A", "B", v2.LoopEdge)
	dag.AddCondition("C", map[string]string{"PASS": "D", "FAIL": "E"})
	dag.AddEdge("B", "C")
	dag.AddEdge("D", "F")
	dag.AddEdge("E", "F")

	initialPayload, _ := json.Marshal([]map[string]any{
		{"user_id": 1, "age": 12},
		{"user_id": 2, "age": 34},
	})
	rs := dag.ProcessTask(context.Background(), "A", initialPayload)
	fmt.Println(string(rs.Payload))
}
