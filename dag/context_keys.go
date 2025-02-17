// context_keys.go
package dag

import (
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
	"time"
)

type contextKey string

const (
	contextKeyTaskID      contextKey = "task_id"
	contextKeyMethod      contextKey = "method"
	contextKeyInitialNode contextKey = "initial_node"
)

// updateTaskMetrics is a placeholder for updating metrics for the task.
// In a real implementation, you could update a persistent store or an in-memory metrics structure.
func (tm *DAG) updateTaskMetrics(taskID string, result mq.Result, duration time.Duration) {
	// Example: Update last executed timestamp, last error, total execution count, success count, etc.
	// For demonstration, we simply log the KPI updates.
	var success bool
	if result.Error == nil {
		success = true
	}
	tm.Logger().Info("Updating task metrics",
		logger.Field{Key: "taskID", Value: taskID},
		logger.Field{Key: "lastExecuted", Value: time.Now()},
		logger.Field{Key: "duration", Value: duration},
		logger.Field{Key: "success", Value: success},
	)
}
