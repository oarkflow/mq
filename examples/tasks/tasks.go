package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	mq2 "github.com/oarkflow/mq"
)

func Node1(ctx context.Context, task mq2.Task) mq2.Result {
	return mq2.Result{Payload: task.Payload, MessageID: task.ID}
}

func Node2(ctx context.Context, task mq2.Task) mq2.Result {
	return mq2.Result{Payload: task.Payload, MessageID: task.ID}
}

func Node3(ctx context.Context, task mq2.Task) mq2.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq2.Result{Error: err}
	}
	data["salary"] = fmt.Sprintf("12000%v", data["user_id"])
	bt, _ := json.Marshal(data)
	return mq2.Result{Payload: bt, MessageID: task.ID}
}

func Node4(ctx context.Context, task mq2.Task) mq2.Result {
	var data []map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq2.Result{Error: err}
	}
	payload := map[string]any{"storage": data}
	bt, _ := json.Marshal(payload)
	return mq2.Result{Payload: bt, MessageID: task.ID}
}

func Callback(ctx context.Context, task mq2.Result) mq2.Result {
	fmt.Println("Received task", task.MessageID, "Payload", string(task.Payload), task.Error, task.Queue)
	return mq2.Result{}
}
