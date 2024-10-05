package main

import (
	"context"
	"fmt"

	mq "github.com/oarkflow/mq/v2"
)

func main() {
	payload := []byte(`{"message":"Message Publisher \n Task"}`)
	task := mq.Task{
		Payload: payload,
	}
	publisher := mq.NewPublisher("publish-1")
	// publisher := mq.NewPublisher("publish-1", mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"))
	err := publisher.Publish(context.Background(), "queue1", task)
	if err != nil {
		panic(err)
	}
	fmt.Println("Async task published successfully")
	payload = []byte(`{"message":"Fire-and-Forget \n Task"}`)
	task = mq.Task{
		Payload: payload,
	}
	result := publisher.Request(context.Background(), "queue1", task)
	if result.Error != nil {
		panic(result.Error)
	}
	fmt.Printf("Sync task published. Result: %v\n", string(result.Payload))
}
