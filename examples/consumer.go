package main

import (
	"context"

	"github.com/oarkflow/mq"

	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	consumer1 := mq.NewConsumer("consumer-1", "queue1", tasks.Node1)
	consumer2 := mq.NewConsumer("consumer-2", "queue2", tasks.Node2)
	// consumer := mq.NewConsumer("consumer-1", mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"))
	go consumer1.Consume(context.Background())
	consumer2.Consume(context.Background())
}
