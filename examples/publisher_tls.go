package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/mq"
)

func main() {
	payload := []byte(`{"message":"Message Publisher \n Task"}`)
	task := mq.Task{
		Payload: payload,
	}
	publisher := mq.NewPublisher("publish-1", mq.WithTLS(true, "publisher.crt", "publisher.key"))
	err := publisher.Publish(context.Background(), "queue1", task)
	if err != nil {
		panic(err)
	}
	fmt.Println("Async task published successfully")
	payload = []byte(`{"message":"Fire-and-Forget \n Task"}`)
	task = mq.Task{
		Payload: payload,
	}
	result, err := publisher.Request(context.Background(), "queue1", task)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Sync task published. Result: %v\n", string(result.Payload))
}
