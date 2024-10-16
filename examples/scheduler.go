package main

import (
	"context"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	pool := mq.NewPool(3, 5, 1000, tasks.SchedulerHandler, tasks.SchedulerCallback)

	// Add immediate tasks with priorities
	pool.AddTask(context.Background(), &mq.Task{ID: "Task 1"}, 1)
	pool.AddTask(context.Background(), &mq.Task{ID: "Task 2"}, 5)

	// Adding scheduled tasks
	pool.Scheduler.AddTask(
		context.Background(),
		tasks.SchedulerHandler,
		&mq.Task{ID: "Every Minute Task"},
		mq.SchedulerConfig{Callback: tasks.SchedulerCallback, Overlap: true},
		&mq.Schedule{Interval: time.Minute, Recurring: true},
	)

	pool.Scheduler.AddTask(
		context.Background(),
		tasks.SchedulerHandler,
		&mq.Task{ID: "Weekday Task"},
		mq.SchedulerConfig{Callback: tasks.SchedulerCallback, Overlap: false},
		&mq.Schedule{Interval: time.Hour * 24, DayOfWeek: []time.Weekday{time.Monday, time.Wednesday, time.Friday}, Recurring: true},
	)
	pool.Scheduler.AddTask(
		context.Background(),
		tasks.SchedulerHandler,
		&mq.Task{ID: "Mid of Month Task"},
		mq.SchedulerConfig{Callback: tasks.SchedulerCallback, Overlap: false},
		&mq.Schedule{DayOfMonth: []int{15}, Recurring: true},
	)

	// Let tasks run for a while
	time.Sleep(10 * time.Minute)

	// Removing a scheduled task
	pool.Scheduler.RemoveTask("Every Minute Task")

	// Let remaining tasks run for a bit
	time.Sleep(5 * time.Minute)

	pool.PrintMetrics()
	pool.Stop()
}
