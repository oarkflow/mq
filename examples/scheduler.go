package main

import (
	"context"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	pool := mq.NewPool(3, 5, 1000, tasks.SchedulerHandler, tasks.SchedulerCallback)
	pool.AddTask(context.Background(), &mq.Task{ID: "Task 1"}, 1)
	pool.AddTask(context.Background(), &mq.Task{ID: "Task 2"}, 5)
	pool.AddTask(context.Background(), &mq.Task{ID: "Task 3"}, 3)

	// Adding scheduled tasks
	pool.Scheduler.AddTask(
		context.Background(),
		tasks.SchedulerHandler,
		&mq.Task{ID: "Scheduled Task 1"},
		3*time.Second,
		mq.SchedulerConfig{WithCallback: tasks.SchedulerCallback, WithOverlap: true},
	)

	pool.Scheduler.AddTask(
		context.Background(),
		tasks.SchedulerHandler,
		&mq.Task{ID: "Scheduled Task 2"},
		5*time.Second,
		mq.SchedulerConfig{WithCallback: tasks.SchedulerCallback, WithOverlap: false},
	)

	// Let tasks run for a while
	time.Sleep(10 * time.Second)

	// Removing a scheduled task
	pool.Scheduler.RemoveTask("Scheduled Task 1")

	// Let remaining tasks run for a bit
	time.Sleep(5 * time.Second)
	pool.PrintMetrics()
	pool.Stop()
}
