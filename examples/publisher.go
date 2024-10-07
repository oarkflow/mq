package main

import (
	"context"
	"fmt"
	mq2 "github.com/oarkflow/mq"
	"time"
)

func main() {
	payload := []byte(`{"message":"Message Publisher \n Task"}`)
	task := mq2.Task{
		Payload: payload,
	}
	publisher := mq2.NewPublisher("publish-1")
	// publisher := mq.NewPublisher("publish-1", mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"))
	err := publisher.Publish(context.Background(), task, "queue1")
	if err != nil {
		panic(err)
	}
	fmt.Println("Async task published successfully")
	payload = []byte(`{"message":"Fire-and-Forget \n Task"}`)
	task = mq2.Task{
		Payload: payload,
	}
	for i := 0; i < 100; i++ {
		time.Sleep(500 * time.Millisecond)
		err := publisher.Publish(context.Background(), task, "queue1")
		if err != nil {
			panic(err)
		}
	}
}
