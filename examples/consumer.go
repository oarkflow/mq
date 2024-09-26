package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/golong/broker"
)

func main() {
	consumer := broker.NewConsumer(":8080")
	consumer.RegisterHandler("queue1", func(ctx context.Context, task broker.Task) broker.Result {
		fmt.Println("Handling task for queue1:", task.ID)
		return broker.Result{Payload: task.Payload, MessageID: task.ID}
	})
	consumer.RegisterHandler("queue2", func(ctx context.Context, task broker.Task) broker.Result {
		fmt.Println("Handling task for queue2:", task.ID)
		return broker.Result{Payload: task.Payload, MessageID: task.ID}
	})
	consumer.Consume(context.Background(), "queue2", "queue1")
}
