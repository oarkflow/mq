package tasks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/mq"
)

func Node1(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

func Node2(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

func Node3(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	data["salary"] = fmt.Sprintf("12000%v", data["user_id"])
	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, TaskID: task.ID}
}

func Node4(ctx context.Context, task *mq.Task) mq.Result {
	var data []map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	payload := map[string]any{"storage": data}
	bt, _ := json.Marshal(payload)
	return mq.Result{Payload: bt, TaskID: task.ID}
}

func CheckCondition(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	var status string
	if data["user_id"].(float64) == 2 {
		status = "pass"
	} else {
		status = "fail"
	}
	return mq.Result{Status: status, Payload: task.Payload, TaskID: task.ID}
}

func Pass(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("Pass")
	return mq.Result{Payload: task.Payload}
}

func Fail(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("Fail")
	return mq.Result{Payload: []byte(`{"test2": "asdsa"}`)}
}

func Callback(ctx context.Context, task mq.Result) mq.Result {
	fmt.Println("Received task", task.TaskID, "Payload", string(task.Payload), task.Error, task.Topic)
	return mq.Result{}
}
