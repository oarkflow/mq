package main

import (
	"context"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	pool := mq.NewPool(2, 5, 1000, tasks.SchedulerHandler, tasks.SchedulerCallback)

	// Add tasks with different priorities
	pool.AddTask(context.Background(), &mq.Task{ID: "Low Priority Task"}, 1)    // Lowest priority
	pool.AddTask(context.Background(), &mq.Task{ID: "Medium Priority Task"}, 5) // Medium priority
	pool.AddTask(context.Background(), &mq.Task{ID: "High Priority Task"}, 10)  // Highest priority
	// Let tasks run
	time.Sleep(5 * time.Second)
	pool.PrintMetrics()
	pool.Stop()
}
