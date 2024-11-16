package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
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

	taskNodeStatus storage.IMap[string, *taskNodeStatus]
	results        []mq.Result
	iteratorNodes  map[string][]Edge

	wg     *WaitGroup
	mutex  sync.Mutex
	topic  string
	result mq.Result
}

func NewTaskManager(d *DAG, taskID string, iteratorNodes map[string][]Edge) *TaskManager {
	if iteratorNodes == nil {
		iteratorNodes = make(map[string][]Edge)
	}
	return &TaskManager{
		dag:            d,
		results:        make([]mq.Result, 0),
		taskNodeStatus: memory.New[string, *taskNodeStatus](),
		taskID:         taskID,
		iteratorNodes:  iteratorNodes,
		wg:             NewWaitGroup(),
	}
}

func (tm *TaskManager) updateTS(result *mq.Result) {
	result.CreatedAt = tm.createdAt
	result.ProcessedAt = time.Now()
	result.Latency = fmt.Sprintf("%s", time.Since(tm.createdAt))
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
	tm.mutex.Unlock()
	if tm.dag.reportNodeResultCallback != nil {
		tm.dag.reportNodeResultCallback(result)
	}
}

func (tm *TaskManager) SetTotalItems(topic string, i int) {
	if nodeStatus, ok := tm.taskNodeStatus.Get(topic); ok {
		nodeStatus.totalItems = i
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
