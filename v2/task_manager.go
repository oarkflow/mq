package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

type TaskManager struct {
	taskID          string
	dag             *DAG
	wg              sync.WaitGroup
	mutex           sync.Mutex
	results         []mq.Result
	waitingCallback int64
	nodeResults     map[string]mq.Result
	done            chan struct{}
	finalResult     chan mq.Result // Channel to collect final results
}

func NewTaskManager(d *DAG, taskID string) *TaskManager {
	return &TaskManager{
		dag:         d,
		nodeResults: make(map[string]mq.Result),
		results:     make([]mq.Result, 0),
		taskID:      taskID,
	}
}

func (tm *TaskManager) handleSyncTask(ctx context.Context, node *Node, payload json.RawMessage) mq.Result {
	tm.done = make(chan struct{})
	tm.wg.Add(1)
	go tm.processNode(ctx, node, payload)
	go func() {
		tm.wg.Wait()
		close(tm.done)
	}()
	select {
	case <-ctx.Done():
		return mq.Result{Error: ctx.Err()}
	case <-tm.done:
		tm.mutex.Lock()
		defer tm.mutex.Unlock()
		if len(tm.results) == 1 {
			return tm.handleResult(ctx, tm.results[0])
		}
		return tm.handleResult(ctx, tm.results)
	}
}

func (tm *TaskManager) handleAsyncTask(ctx context.Context, node *Node, payload json.RawMessage) mq.Result {
	tm.finalResult = make(chan mq.Result)
	tm.wg.Add(1)
	go tm.processNode(ctx, node, payload)
	go func() {
		tm.wg.Wait()
	}()
	select {
	case result := <-tm.finalResult: // Block until a result is available
		return result
	case <-ctx.Done(): // Handle context cancellation
		return mq.Result{Error: ctx.Err()}
	}
}

func (tm *TaskManager) processTask(ctx context.Context, nodeID string, payload json.RawMessage) mq.Result {
	node, ok := tm.dag.Nodes[nodeID]
	if !ok {
		return mq.Result{Error: fmt.Errorf("nodeID %s not found", nodeID)}
	}
	if tm.dag.server.SyncMode() {
		return tm.handleSyncTask(ctx, node, payload)
	}
	return tm.handleAsyncTask(ctx, node, payload)
}

func (tm *TaskManager) dispatchFinalResult(ctx context.Context) {
	if !tm.dag.server.SyncMode() {
		var rs mq.Result
		if len(tm.results) == 1 {
			rs = tm.handleResult(ctx, tm.results[0])
		} else {
			rs = tm.handleResult(ctx, tm.results)
		}
		if tm.waitingCallback == 0 {
			tm.finalResult <- rs
		}
	}
}

func (tm *TaskManager) handleCallback(ctx context.Context, result mq.Result) mq.Result {
	if result.Topic != "" {
		atomic.AddInt64(&tm.waitingCallback, -1)
	}
	node, ok := tm.dag.Nodes[result.Topic]
	if !ok {
		return result
	}
	edges := make([]Edge, len(node.Edges))
	copy(edges, node.Edges)
	if result.Status != "" {
		if conditions, ok := tm.dag.conditions[result.Topic]; ok {
			if targetNodeKey, ok := conditions[result.Status]; ok {
				if targetNode, ok := tm.dag.Nodes[targetNodeKey]; ok {
					edges = append(edges, Edge{From: node, To: targetNode})
				}
			}
		}
	}
	if len(edges) == 0 {
		tm.appendFinalResult(result)
		tm.dispatchFinalResult(ctx)
		return result
	}
	for _, edge := range edges {
		switch edge.Type {
		case LoopEdge:
			var items []json.RawMessage
			err := json.Unmarshal(result.Payload, &items)
			if err != nil {
				tm.appendFinalResult(mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: err})
				return result
			}
			for _, item := range items {
				tm.wg.Add(1)
				ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: edge.To.Key})
				go tm.processNode(ctx, edge.To, item)
			}
		case SimpleEdge:
			if edge.To != nil {
				tm.wg.Add(1)
				ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: edge.To.Key})
				go tm.processNode(ctx, edge.To, result.Payload)
			}
		}
	}
	return mq.Result{}
}

func (tm *TaskManager) handleResult(ctx context.Context, results any) mq.Result {
	var rs mq.Result
	switch res := results.(type) {
	case []mq.Result:
		aggregatedOutput := make([]json.RawMessage, 0)
		status := ""
		for i, result := range res {
			if i == 0 {
				status = result.Status
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
		return mq.Result{
			TaskID:  tm.taskID,
			Payload: finalOutput,
			Status:  status,
		}
	case mq.Result:
		return res
	}
	return rs
}

func (tm *TaskManager) appendFinalResult(result mq.Result) {
	tm.mutex.Lock()
	tm.results = append(tm.results, result)
	tm.nodeResults[result.Topic] = result
	tm.mutex.Unlock()
}

func (tm *TaskManager) processNode(ctx context.Context, node *Node, payload json.RawMessage) {
	atomic.AddInt64(&tm.waitingCallback, 1)
	defer tm.wg.Done()
	var result mq.Result
	select {
	case <-ctx.Done():
		result = mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: ctx.Err()}
		tm.appendFinalResult(result)
		return
	default:
		ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: node.Key})
		if tm.dag.server.SyncMode() {
			result = node.consumer.ProcessTask(ctx, NewTask(tm.taskID, payload, node.Key))
			result.Topic = node.Key
			result.TaskID = tm.taskID
			if result.Error != nil {
				tm.appendFinalResult(result)
				return
			}
		} else {
			err := tm.dag.server.Publish(ctx, NewTask(tm.taskID, payload, node.Key), node.Key)
			if err != nil {
				tm.appendFinalResult(mq.Result{Error: err})
				return
			}
		}
	}
	tm.mutex.Lock()
	tm.nodeResults[node.Key] = result
	tm.mutex.Unlock()
	tm.handleCallback(ctx, result)
}

func (tm *TaskManager) Clear() error {
	tm.waitingCallback = 0
	clear(tm.results)
	tm.nodeResults = make(map[string]mq.Result)
	return nil
}
