package main

import (
	"context"
	"fmt"
	"github.com/oarkflow/mq"
)

func main() {
	consumer := mq.NewConsumer(":8080")
	consumer.RegisterHandler("queue1", func(ctx context.Context, task mq.Task) mq.Result {
		fmt.Println("Handling task for queue1:", task.ID)
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})
	consumer.RegisterHandler("queue2", func(ctx context.Context, task mq.Task) mq.Result {
		fmt.Println("Handling task for queue2:", task.ID)
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})
	consumer.Consume(context.Background(), "queue2", "queue1")
}
