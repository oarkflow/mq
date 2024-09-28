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

// publishAsync sends a task in Fire-and-Forget (async) mode
func publishAsync() error {
	taskPayload := map[string]string{"message": "Fire-and-Forget \n Task"}
	payload, _ := json.Marshal(taskPayload)

	task := mq.Task{
		Payload: payload,
	}

	// Create publisher and send the task without waiting for a result
	publisher := mq.NewPublisher("publish-1")
	err := publisher.Publish(context.Background(), "queue1", task)
	if err != nil {
		return fmt.Errorf("failed to publish async task: %w", err)
	}

	fmt.Println("Async task published successfully")
	return nil
}

// publishSync sends a task in Request/Response (sync) mode
func publishSync() error {
	taskPayload := map[string]string{"message": "Request/Response \n Task"}
	payload, _ := json.Marshal(taskPayload)

	task := mq.Task{
		Payload: payload,
	}

	// Create publisher and send the task, waiting for the result
	publisher := mq.NewPublisher("publish-2")
	result, err := publisher.Request(context.Background(), "queue1", task)
	if err != nil {
		return fmt.Errorf("failed to publish sync task: %w", err)
	}

	fmt.Printf("Sync task published. Result: %v\n", string(result.Payload))
	return nil
}
