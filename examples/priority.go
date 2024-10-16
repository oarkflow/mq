package main

import (
	"context"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	pool := mq.NewPool(2, 5, 1000, tasks.SchedulerHandler, tasks.SchedulerCallback)

	time.Sleep(time.Millisecond)
	pool.AddTask(context.Background(), &mq.Task{ID: "Low Priority Task"}, 1)
	pool.AddTask(context.Background(), &mq.Task{ID: "Medium Priority Task"}, 5)
	pool.AddTask(context.Background(), &mq.Task{ID: "High Priority Task"}, 10)

	time.Sleep(5 * time.Second)
	pool.PrintMetrics()
	pool.Stop()
}
