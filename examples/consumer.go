package main

import (
	"context"

	"github.com/oarkflow/mq"

	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	n := &tasks.Node6{}
	consumer1 := mq.NewConsumer("F", "queue1", n.ProcessTask, mq.WithWorkerPool(100, 4, 50000))
	consumer1.Consume(context.Background())
}
