package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/mq"
)

func main() {
	payload := []byte(`{"phone": "+123456789", "email": "abc.xyz@gmail.com", "age": 12}`)
	task := mq.Task{
		Payload: payload,
	}
	publisher := mq.NewPublisher("publish-1")
	for i := 0; i < 10000000; i++ {
		if i%1000 == 0 {
			time.Sleep(time.Second * 5)
		}
		// publisher := mq.NewPublisher("publish-1", mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"))
		err := publisher.Publish(context.Background(), task, "queue1")
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Async task published successfully")
}
