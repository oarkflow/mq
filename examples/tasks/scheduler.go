package tasks

import (
	"context"
	"fmt"

	"github.com/oarkflow/mq"
)

func SchedulerHandler(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Printf("Processing task: %s\n", task.ID)
	return mq.Result{Error: nil}
}

func SchedulerCallback(ctx context.Context, result mq.Result) error {
	if result.Error != nil {
		fmt.Println("Task failed!", result.Error.Error())
	}
	return nil
}
