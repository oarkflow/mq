package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/mq"
)

func main() {
	payload := []byte(`{"phone": "+123456789", "email": "abc.xyz@gmail.com", "age": 12}`)
	task := mq.Task{
		Payload: payload,
	}
	publisher := mq.NewPublisher("publish-1")
	// publisher := mq.NewPublisher("publish-1", mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"))
	err := publisher.Publish(context.Background(), task, "queue")
	if err != nil {
		panic(err)
	}
	fmt.Println("Async task published successfully")
}
