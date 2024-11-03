package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/mq"
)

func main() {
	payload := []byte(`[{"phone": "+123456789", "email": "abc.xyz@gmail.com"}, {"phone": "+98765412", "email": "xyz.abc@gmail.com"}]`)
	task := mq.Task{
		Payload: payload,
	}
	publisher := mq.NewPublisher("publish-1", mq.WithBrokerURL(":8081"))
	// publisher := mq.NewPublisher("publish-1", mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"))
	err := publisher.Publish(context.Background(), task, "queue")
	if err != nil {
		panic(err)
	}
	fmt.Println("Async task published successfully")
}
