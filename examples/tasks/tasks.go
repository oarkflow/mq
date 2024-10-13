package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/oarkflow/mq"
)

func Node1(_ context.Context, task *mq.Task) mq.Result {
	fmt.Println("Node 1", string(task.Payload))
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

func Node2(_ context.Context, task *mq.Task) mq.Result {
	fmt.Println("Node 2", string(task.Payload))
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

func Node3(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	age := int(user["age"].(float64))
	status := "FAIL"
	if age > 20 {
		status = "PASS"
	}
	user["status"] = status
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload, Status: status}
}

func Node4(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	user["node"] = "D"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Node5(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	user["node"] = "E"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Node6(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	resultPayload, _ := json.Marshal(map[string]any{"storage": user})
	return mq.Result{Payload: resultPayload}
}

func Node7(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	user["node"] = "G"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Node8(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	user["node"] = "H"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Callback(_ context.Context, task mq.Result) mq.Result {
	fmt.Println("Received task", task.TaskID, "Payload", string(task.Payload), task.Error, task.Topic)
	return mq.Result{}
}

func NotifyResponse(_ context.Context, result mq.Result) {
	log.Printf("DAG - FINAL_RESPONSE ~> TaskID: %s, Payload: %s, Topic: %s, Error: %s", result.TaskID, result.Payload, result.Topic, result.Error)
}
