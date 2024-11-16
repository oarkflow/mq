package dag

import (
	"context"
	"time"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"

	"github.com/oarkflow/mq"
)

type TaskManager struct {
	createdAt   time.Time
	processedAt time.Time
	status      string
	dag         *DAG
	taskID      string
	wg          *WaitGroup
	topic       string
	result      mq.Result

	iteratorNodes  storage.IMap[string, []Edge]
	taskNodeStatus storage.IMap[string, *taskNodeStatus]
}

func NewTaskManager(d *DAG, taskID string, iteratorNodes storage.IMap[string, []Edge]) *TaskManager {
	return &TaskManager{
		dag:            d,
		taskNodeStatus: memory.New[string, *taskNodeStatus](),
		taskID:         taskID,
		iteratorNodes:  iteratorNodes,
		wg:             NewWaitGroup(),
	}
}

func (tm *TaskManager) dispatchFinalResult(ctx context.Context) mq.Result {
	tm.updateTS(&tm.result)
	tm.dag.callbackToConsumer(ctx, tm.result)
	if tm.dag.server.NotifyHandler() != nil {
		_ = tm.dag.server.NotifyHandler()(ctx, tm.result)
	}
	tm.dag.taskCleanupCh <- tm.taskID
	tm.topic = tm.result.Topic
	return tm.result
}

func (tm *TaskManager) reportNodeResult(result mq.Result, final bool) {
	if tm.dag.reportNodeResultCallback != nil {
		tm.dag.reportNodeResultCallback(result)
	}
}

func (tm *TaskManager) SetTotalItems(topic string, i int) {
	if nodeStatus, ok := tm.taskNodeStatus.Get(topic); ok {
		nodeStatus.totalItems = i
	}
}
