package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq"
)

type TaskManager struct {
	createdAt   time.Time
	processedAt time.Time
	status      string
	dag         *DAG
	nodeResults map[string]mq.Result
	wg          *WaitGroup
	taskID      string
	results     []mq.Result
	mutex       sync.Mutex
}

func NewTaskManager(d *DAG, taskID string) *TaskManager {
	return &TaskManager{
		dag:         d,
		nodeResults: make(map[string]mq.Result),
		results:     make([]mq.Result, 0),
		taskID:      taskID,
		wg:          NewWaitGroup(),
	}
}

func (tm *TaskManager) updateTS(result *mq.Result) {
	result.CreatedAt = tm.createdAt
	result.ProcessedAt = time.Now()
	result.Latency = fmt.Sprintf("%s", time.Since(tm.createdAt))
}

func (tm *TaskManager) dispatchFinalResult(ctx context.Context) mq.Result {
	var rs mq.Result
	if len(tm.results) == 1 {
		rs = tm.handleResult(ctx, tm.results[0])
	} else {
		rs = tm.handleResult(ctx, tm.results)
	}
	tm.updateTS(&rs)
	tm.dag.callbackToConsumer(ctx, rs)
	if tm.dag.server.NotifyHandler() != nil {
		_ = tm.dag.server.NotifyHandler()(ctx, rs)
	}
	tm.dag.taskCleanupCh <- tm.taskID
	return rs
}

func (tm *TaskManager) handleResult(ctx context.Context, results any) mq.Result {
	var rs mq.Result
	switch res := results.(type) {
	case []mq.Result:
		aggregatedOutput := make([]json.RawMessage, 0)
		var status, topic string
		for i, result := range res {
			if i == 0 {
				status = result.Status
				topic = result.Topic
			}
			if result.Error != nil {
				return mq.HandleError(ctx, result.Error)
			}
			var item json.RawMessage
			err := json.Unmarshal(result.Payload, &item)
			if err != nil {
				return mq.HandleError(ctx, err)
			}
			aggregatedOutput = append(aggregatedOutput, item)
		}
		finalOutput, err := json.Marshal(aggregatedOutput)
		if err != nil {
			return mq.HandleError(ctx, err)
		}
		return mq.Result{TaskID: tm.taskID, Payload: finalOutput, Status: status, Topic: topic, Ctx: ctx}
	case mq.Result:
		if res.Ctx == nil {
			res.Ctx = ctx
		}
		return res
	}
	if rs.Ctx == nil {
		rs.Ctx = ctx
	}
	return rs
}

func (tm *TaskManager) appendResult(result mq.Result, final bool) {
	tm.mutex.Lock()
	tm.updateTS(&result)
	if final {
		tm.results = append(tm.results, result)
	}
	tm.nodeResults[result.Topic] = result
	tm.mutex.Unlock()
	if tm.dag.reportNodeResultCallback != nil {
		tm.dag.reportNodeResultCallback(result)
	}
}

func isDAGNode(node *Node) (*DAG, bool) {
	switch node := node.processor.(type) {
	case *DAG:
		return node, true
	default:
		return nil, false
	}
}
