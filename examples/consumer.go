package main

import (
	"context"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	n := &tasks.Node6{}
	consumer1 := mq.NewConsumer("F", "queue1", n.ProcessTask,
		mq.WithBrokerURL(":8081"),
		mq.WithHTTPApi(true),
		mq.WithWorkerPool(100, 4, 50000),
		mq.WithSecurity(true),
		mq.WithUsername("consumer"),
		mq.WithPassword("con123"),
	)
	consumer1.Consume(context.Background())
}
