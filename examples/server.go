package main

import (
	"context"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	b := mq.NewBroker(mq.WithCallback(tasks.Callback))
	b.NewQueue("queue1")
	b.NewQueue("queue2")
	b.Start(context.Background())
}
