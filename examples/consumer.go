package main

import (
	"context"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	consumer := mq.NewConsumer("consumer-1")
	consumer.RegisterHandler("queue1", tasks.Node1)
	consumer.RegisterHandler("queue2", tasks.Node2)
	consumer.Consume(context.Background())
}
