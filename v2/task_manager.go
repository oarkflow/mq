package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

type TaskManager struct {
	taskID      string
	dag         *DAG
	wg          sync.WaitGroup
	mutex       sync.Mutex
	results     []mq.Result
	nodeResults map[string]mq.Result
	done        chan struct{}
	finalResult chan mq.Result // Channel to collect final results
}

func NewTaskManager(d *DAG, taskID string) *TaskManager {
	return &TaskManager{
		dag:         d,
		nodeResults: make(map[string]mq.Result),
		results:     make([]mq.Result, 0),
		done:        make(chan struct{}),
		taskID:      taskID,
		finalResult: make(chan mq.Result), // Initialize finalResult channel
	}
}

func (tm *TaskManager) processTask(ctx context.Context, nodeID string, payload json.RawMessage) mq.Result {
	node, ok := tm.dag.Nodes[nodeID]
	if !ok {
		return mq.Result{Error: fmt.Errorf("nodeID %s not found", nodeID)}
	}
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

func (tm *TaskManager) handleCallback(ctx context.Context, result mq.Result) mq.Result {
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
		return
	}
	for _, edge := range edges {
		switch edge.Type {
		case LoopEdge:
			var items []json.RawMessage
			err := json.Unmarshal(result.Payload, &items)
			if err != nil {
				tm.appendFinalResult(mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: err})
				return
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
}
