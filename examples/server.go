package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/mq"
)

func main() {
	b := mq.NewBroker(mq.WithCallback(func(ctx context.Context, task *mq.Task) error {
		fmt.Println("Received task", task.ID, "Payload", string(task.Payload), "Result", string(task.Result), task.Error, task.CurrentQueue)
		return nil
	}))
	b.NewQueue("queue1")
	b.NewQueue("queue2")
	b.Start(context.Background(), ":8080")
}
