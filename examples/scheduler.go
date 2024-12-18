package main

import (
	"context"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	handler := tasks.SchedulerHandler
	callback := tasks.SchedulerCallback
	pool := mq.NewPool(3,
		mq.WithTaskQueueSize(5),
		mq.WithMaxMemoryLoad(1000),
		mq.WithHandler(handler),
		mq.WithPoolCallback(callback),
		mq.WithTaskStorage(mq.NewMemoryTaskStorage(10*time.Minute)),
	)
	ctx := context.Background()
	pool.EnqueueTask(context.Background(), &mq.Task{ID: "Task 1"}, 1)
	time.Sleep(1 * time.Second)
	pool.EnqueueTask(context.Background(), &mq.Task{ID: "Task 2"}, 5)
	pool.Scheduler().AddTask(ctx, &mq.Task{ID: "Every Minute Task"})
	time.Sleep(10 * time.Minute)
	pool.Scheduler().RemoveTask("Every Minute Task")
	time.Sleep(5 * time.Minute)
	pool.Metrics()
	pool.Stop()
}
