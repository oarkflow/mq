package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/mq"
)

func main() {
	ctx := context.Background()
	publisher := mq.NewPublisher(":8080")
	task := mq.Task{
		ID:      "task-1",
		Payload: json.RawMessage(`{"message": "Hello World"}`),
	}
	if err := publisher.Publish(ctx, "queue1", task); err != nil {
		fmt.Println("Failed to publish task:", err)
	}
}
