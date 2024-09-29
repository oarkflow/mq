package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

func main() {
	d := dag.New()
	d.AddNode("queue1", func(ctx context.Context, task mq.Task) mq.Result {
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})
	d.AddNode("queue2", func(ctx context.Context, task mq.Task) mq.Result {
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})
	d.AddNode("queue3", func(ctx context.Context, task mq.Task) mq.Result {
		var data map[string]any
		err := json.Unmarshal(task.Payload, &data)
		if err != nil {
			return mq.Result{Error: err}
		}
		data["salary"] = fmt.Sprintf("12000%v", data["user_id"])
		bt, _ := json.Marshal(data)
		return mq.Result{Payload: bt, MessageID: task.ID}
	})
	d.AddNode("queue4", func(ctx context.Context, task mq.Task) mq.Result {
		var data []map[string]any
		err := json.Unmarshal(task.Payload, &data)
		if err != nil {
			return mq.Result{Error: err}
		}
		payload := map[string]any{"storage": data}
		bt, _ := json.Marshal(payload)
		return mq.Result{Payload: bt, MessageID: task.ID}
	})

	d.AddEdge("queue1", "queue2")
	d.AddLoop("queue2", "queue3")
	d.AddEdge("queue2", "queue4")

	go func() {
		time.Sleep(2 * time.Second)
		finalResult := d.Send([]byte(`[{"user_id": 1}, {"user_id": 2}]`))
		log.Printf("Result received: %s %s", finalResult.MessageID, string(finalResult.Payload))
	}()

	err := d.Start(context.TODO())
	if err != nil {
		panic(err)
	}
}
