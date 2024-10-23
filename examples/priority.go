package main

import (
	"context"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	pool := mq.NewPool(2,
		mq.WithTaskQueueSize(5),
		mq.WithMaxMemoryLoad(1000),
		mq.WithHandler(tasks.SchedulerHandler),
		mq.WithPoolCallback(tasks.SchedulerCallback),
		mq.WithTaskStorage(mq.NewMemoryTaskStorage(10*time.Minute)),
	)

	for i := 0; i < 100; i++ {
		if i%10 == 0 {
			pool.EnqueueTask(context.Background(), &mq.Task{ID: "High Priority Task: I'm high"}, 10)
		} else if i%15 == 0 {
			pool.EnqueueTask(context.Background(), &mq.Task{ID: "Super High Priority Task: {}"}, 15)
		} else {
			pool.EnqueueTask(context.Background(), &mq.Task{ID: "Low Priority Task"}, 1)
		}
	}

	time.Sleep(15 * time.Second)
	pool.Metrics()
	pool.Stop()
}
