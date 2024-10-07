package main

import (
	"context"
	mq2 "github.com/oarkflow/mq"

	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	b := mq2.NewBroker(mq2.WithCallback(tasks.Callback))
	// b := mq.NewBroker(mq.WithCallback(tasks.Callback), mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"), mq.WithCAPath("./certs/ca.cert"))
	b.NewQueue("queue1")
	b.NewQueue("queue2")
	b.Start(context.Background())
}
