package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	v2 "github.com/oarkflow/mq/v2"
)

func main() {
	broker := v2.NewBroker()
	go broker.Start(context.Background())
	time.Sleep(1 * time.Second)

	consumer := v2.NewConsumer("consumer-1")
	consumer.RegisterHandler("queue-1", func(ctx context.Context, task v2.Task) v2.Result {
		fmt.Println("Handling on queue-1", string(task.Payload))
		return v2.Result{Payload: task.Payload}
	})
	go func() {
		err := consumer.Consume(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	publisher := v2.NewPublisher("publisher-1")
	data := map[string]interface{}{"temperature": 23.5, "humidity": 60}
	payload, _ := json.Marshal(data)
	rs := publisher.Request(context.Background(), "queue-1", v2.Task{Payload: payload})
	fmt.Println("Response:", string(rs.Payload), rs.Error)
}
