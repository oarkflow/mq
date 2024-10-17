package main

import (
	"context"

	"github.com/oarkflow/mq"

	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	consumer1 := mq.NewConsumer("F", "queue1", tasks.Node6{}.ProcessTask, mq.WithWorkerPool(100, 4, 50000))
	consumer1.Consume(context.Background())
}
