package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/mq"
)

func main() {
	b := mq.NewBroker(mq.WithCallback(func(ctx context.Context, task mq.Result) mq.Result {
		fmt.Println("Received task", task.MessageID, "Payload", string(task.Payload), task.Error, task.Queue)
		return mq.Result{}
	}))
	b.NewQueue("queue1")
	b.NewQueue("queue2")
	b.Start(context.Background())
}
