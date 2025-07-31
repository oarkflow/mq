package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	handler := tasks.SchedulerHandler
	callback := tasks.SchedulerCallback

	// Initialize the pool with various parameters.
	pool := mq.NewPool(3,
		mq.WithTaskQueueSize(5),
		mq.WithMaxMemoryLoad(1000),
		mq.WithHandler(handler),
		mq.WithPoolCallback(callback),
		mq.WithTaskStorage(mq.NewMemoryTaskStorage(10*time.Minute)),
	)
	scheduler := mq.NewScheduler(pool)
	scheduler.Start()
	ctx := context.Background()

	// -------------------------------
	// Special String Examples
	// -------------------------------

	// Task scheduled with @every for a duration of 30 seconds.
	scheduler.AddTask(ctx, &mq.Task{ID: "Every 30 Seconds Task"},
		mq.WithScheduleSpec("@every 30s"),
		mq.WithRecurring(),
	)

	// Task scheduled with @every using a 1-minute duration.
	scheduler.AddTask(ctx, &mq.Task{ID: "Every Minute Task"},
		mq.WithScheduleSpec("@every 1m"),
		mq.WithRecurring(),
	)

	// Task scheduled with @daily (runs at midnight by default).
	scheduler.AddTask(ctx, &mq.Task{ID: "Daily Task"},
		mq.WithScheduleSpec("@daily"),
		mq.WithRecurring(),
	)

	// Task scheduled with @weekly (runs at midnight on Sundays).
	scheduler.AddTask(ctx, &mq.Task{ID: "Weekly Task"},
		mq.WithScheduleSpec("@weekly"),
		mq.WithRecurring(),
	)

	// Task scheduled with @monthly (runs on the 1st of every month at midnight).
	scheduler.AddTask(ctx, &mq.Task{ID: "Monthly Task"},
		mq.WithScheduleSpec("@monthly"),
		mq.WithRecurring(),
	)

	// Task scheduled with @yearly (or @annually) – runs on January 1st at midnight.
	scheduler.AddTask(ctx, &mq.Task{ID: "Yearly Task"},
		mq.WithScheduleSpec("@yearly"),
		mq.WithRecurring(),
	)

	// -------------------------------
	// Cron Spec Examples
	// -------------------------------

	// Example using a standard 5-field cron expression:
	// "0 * * * *" means at minute 0 of every hour.
	scheduler.AddTask(ctx, &mq.Task{ID: "Cron 5-field Task"},
		mq.WithScheduleSpec("0 * * * *"),
		mq.WithRecurring(),
	)

	// Example using an extended 6-field cron expression:
	// "30 * * * * *" means at 30 seconds past every minute.
	scheduler.AddTask(ctx, &mq.Task{ID: "Cron 6-field Task"},
		mq.WithScheduleSpec("30 * * * * *"),
		mq.WithRecurring(),
	)

	// -------------------------------
	// Example Task Enqueuing (Immediate Tasks)
	// -------------------------------
	// These tasks are enqueued immediately into the pool queue.
	pool.EnqueueTask(context.Background(), &mq.Task{ID: "Immediate Task 1"}, 1)
	time.Sleep(1 * time.Second)
	pool.EnqueueTask(context.Background(), &mq.Task{ID: "Immediate Task 2"}, 5)

	time.Sleep(10 * time.Minute)
	// Remove scheduled tasks after demonstration.
	scheduler.RemoveTask("Every 30 Seconds Task")
	scheduler.RemoveTask("Every Minute Task")
	scheduler.RemoveTask("Daily Task")
	scheduler.RemoveTask("Weekly Task")
	scheduler.RemoveTask("Monthly Task")
	scheduler.RemoveTask("Yearly Task")
	scheduler.RemoveTask("Cron 5-field Task")
	scheduler.RemoveTask("Cron 6-field Task")
	scheduler.Close()

	// Retrieve metrics, then stop the pool.
	fmt.Println(pool.FormattedMetrics())
}
