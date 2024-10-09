package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/oarkflow/mq"
)

func Node1(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

func Node2(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

func Node3(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	age := int(user["age"].(float64))
	status := "FAIL"
	if age > 20 {
		status = "PASS"
	}
	user["status"] = status
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload, Status: status}
}

func Node4(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	user["final"] = "D"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Node5(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	user["salary"] = "E"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Node6(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	resultPayload, _ := json.Marshal(map[string]any{"storage": user})
	return mq.Result{Payload: resultPayload}
}

func Callback(ctx context.Context, task mq.Result) mq.Result {
	fmt.Println("Received task", task.TaskID, "Payload", string(task.Payload), task.Error, task.Topic)
	return mq.Result{}
}

func NotifyResponse(ctx context.Context, result mq.Result) {
	log.Printf("DAG Final response: TaskID: %s, Payload: %s, Topic: %s", result.TaskID, result.Payload, result.Topic)
}
