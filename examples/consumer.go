package main

import (
	"context"

	"github.com/oarkflow/mq"

	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	consumer1 := mq.NewConsumer("F", "F", tasks.Node6)
	consumer1.Consume(context.Background())
}
