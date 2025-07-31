package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/json"

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

	// Example: Schedule an email task with deduplication.
	// DedupKey here is set to the recipient's email (e.g., "user@example.com") to avoid duplicate email tasks.
	emailPayload := json.RawMessage(`{"email": "user@example.com", "message": "Hello, Customer!"}`)
	scheduler.AddTask(ctx, mq.NewTask("Email Task", emailPayload, "email",
		mq.WithDedupKey("user@example.com"),
	),
		mq.WithScheduleSpec("@every 1m"), // runs every minute for demonstration
		mq.WithRecurring(),
	)

	scheduler.AddTask(ctx, mq.NewTask("Duplicate Email Task", emailPayload, "email",
		mq.WithDedupKey("user@example.com"),
	),
		mq.WithScheduleSpec("@every 1m"),
		mq.WithRecurring(),
	)

	go func() {
		for {
			for _, task := range scheduler.ListScheduledTasks() {
				fmt.Println("Scheduled.....", task)
			}
			time.Sleep(1 * time.Minute)
		}
	}()

	time.Sleep(10 * time.Minute)

}
