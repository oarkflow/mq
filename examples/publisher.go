package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/mq"
)

func main() {
	publishAsync()
	publishSync()
}

func publishAsync() error {
	taskPayload := map[string]string{"message": "Fire-and-Forget \n Task"}
	payload, _ := json.Marshal(taskPayload)
	task := mq.Task{
		Payload: payload,
	}
	publisher := mq.NewPublisher("publish-1")
	err := publisher.Publish(context.Background(), "queue1", task)
	if err != nil {
		return fmt.Errorf("failed to publish async task: %w", err)
	}
	fmt.Println("Async task published successfully")
	return nil
}

func publishSync() error {
	taskPayload := map[string]string{"message": "Request/Response \n Task"}
	payload, _ := json.Marshal(taskPayload)
	task := mq.Task{
		Payload: payload,
	}
	publisher := mq.NewPublisher("publish-2")
	result, err := publisher.Request(context.Background(), "queue1", task)
	if err != nil {
		return fmt.Errorf("failed to publish sync task: %w", err)
	}
	fmt.Printf("Sync task published. Result: %v\n", string(result.Payload))
	return nil
}
