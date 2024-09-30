package tasks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/mq"
)

func Node1(ctx context.Context, task mq.Task) mq.Result {
	fmt.Println("Processing queue1")
	return mq.Result{Payload: task.Payload, MessageID: task.ID}
}

func Node2(ctx context.Context, task mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, MessageID: task.ID}
}

func Node3(ctx context.Context, task mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	data["salary"] = fmt.Sprintf("12000%v", data["user_id"])
	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, MessageID: task.ID}
}

func Node4(ctx context.Context, task mq.Task) mq.Result {
	var data []map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	payload := map[string]any{"storage": data}
	bt, _ := json.Marshal(payload)
	return mq.Result{Payload: bt, MessageID: task.ID}
}
