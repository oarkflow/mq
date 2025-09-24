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
	publisher := mq.NewPublisher("publish-1",
		mq.WithBrokerURL(":8081"),
		mq.WithSecurity(true),
		mq.WithUsername("publisher"),
		mq.WithPassword("pub123"),
	)
	for i := 0; i < 2; i++ {
		// publisher := mq.NewPublisher("publish-1", mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"))
		result := publisher.Request(context.Background(), task, "queue1")
		if result.Error != nil {
			panic(result.Error)
		}
		fmt.Println(string(result.Payload))
	}
	fmt.Println("Async task published successfully")
}
