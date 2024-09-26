package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/golong/broker"
)

func main() {
	b := broker.NewBroker(func(ctx context.Context, task *broker.Task) error {
		fmt.Println("Received task", task.ID, string(task.Payload), string(task.Result), task.Error, task.CurrentQueue)
		return nil
	})
	b.NewQueue("queue1")
	b.NewQueue("queue2")
	go func() {
		for i := 0; i < 10; i++ {
			b.Publish(context.Background(), broker.Task{
				ID:      fmt.Sprint(i),
				Payload: []byte(`"Hello"`),
			}, "queue1")
			b.Publish(context.Background(), broker.Task{
				ID:      fmt.Sprint(i),
				Payload: []byte(`"World"`),
			}, "queue2")
			time.Sleep(time.Second)
		}
	}()
	b.Start(context.Background(), ":8080")
}
