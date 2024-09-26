package main

import (
	"context"
	"fmt"
	"github.com/oarkflow/mq"
	"time"
)

func main() {
	b := mq.NewBroker(func(ctx context.Context, task *mq.Task) error {
		fmt.Println("Received task", task.ID, string(task.Payload), string(task.Result), task.Error, task.CurrentQueue)
		return nil
	})
	b.NewQueue("queue1")
	b.NewQueue("queue2")
	go func() {
		for i := 0; i < 10; i++ {
			b.Publish(context.Background(), mq.Task{
				ID:      fmt.Sprint(i),
				Payload: []byte(`"Hello"`),
			}, "queue1")
			b.Publish(context.Background(), mq.Task{
				ID:      fmt.Sprint(i),
				Payload: []byte(`"World"`),
			}, "queue2")
			time.Sleep(time.Second)
		}
	}()
	b.Start(context.Background(), ":8080")
}
