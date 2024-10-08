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
}

func NewTaskManager(d *DAG, taskID string) *TaskManager {
	return &TaskManager{
		dag:         d,
		nodeResults: make(map[string]mq.Result),
		results:     make([]mq.Result, 0),
		done:        make(chan struct{}),
		taskID:      taskID,
	}
}

func (tm *TaskManager) processTask(ctx context.Context, nodeID string, task *mq.Task) mq.Result {
	node, ok := tm.dag.Nodes[nodeID]
	if !ok {
		return mq.Result{Error: fmt.Errorf("nodeID %s not found", nodeID)}
	}
	tm.wg.Add(1)
	go tm.processNode(ctx, node, task, nil)
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
	fmt.Println(string(result.Payload), result.Topic, result.TaskID)
	return mq.Result{}
}

func (tm *TaskManager) handleResult(ctx context.Context, results any) mq.Result {
	var rs mq.Result
	switch res := results.(type) {
	case []mq.Result:
		aggregatedOutput := make([]json.RawMessage, 0)
		for i, result := range res {
			if i == 0 {
				rs.TaskID = result.TaskID
				rs.Status = result.Status
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
		return mq.HandleError(ctx, err).WithData(rs.Status, finalOutput)
	case mq.Result:
		rs.TaskID = res.TaskID
		var item json.RawMessage
		err := json.Unmarshal(res.Payload, &item)
		if err != nil {
			return mq.HandleError(ctx, err)
		}
		finalOutput, err := json.Marshal(item)
		return mq.HandleError(ctx, err).WithData(res.Status, finalOutput)
	}
	return rs
}

func (tm *TaskManager) appendFinalResult(result mq.Result) {
	tm.mutex.Lock()
	tm.results = append(tm.results, result)
	tm.nodeResults[result.Topic] = result
	tm.mutex.Unlock()
}

func (tm *TaskManager) processNode(ctx context.Context, node *Node, task *mq.Task, parentNode *Node) {
	defer tm.wg.Done()
	var result mq.Result
	select {
	case <-ctx.Done():
		result = mq.Result{TaskID: task.ID, Topic: node.Key, Error: ctx.Err()}
		tm.appendFinalResult(result)
		return
	default:
		ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: node.Key})
		if tm.dag.server.SyncMode() {
			result = node.consumer.ProcessTask(ctx, task)
			result.Topic = node.Key
			if result.Error != nil {
				tm.appendFinalResult(result)
				return
			}
		} else {
			err := tm.dag.server.Publish(ctx, *task, node.Key)
			if err != nil {
				tm.appendFinalResult(mq.Result{Error: err})
				return
			}
		}
	}
	tm.mutex.Lock()
	task.Results[node.Key] = result
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
		if parentNode != nil {
			tm.appendFinalResult(result)
		}
		return
	}
	for _, edge := range edges {
		switch edge.Type {
		case LoopEdge:
			var items []json.RawMessage
			err := json.Unmarshal(result.Payload, &items)
			if err != nil {
				tm.appendFinalResult(mq.Result{TaskID: task.ID, Topic: node.Key, Error: err})
				return
			}
			for _, item := range items {
				loopTask := NewTask(task.ID, item, edge.From.Key, task.Results)
				tm.wg.Add(1)
				ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: edge.To.Key})
				go tm.processNode(ctx, edge.To, loopTask, node)
			}
		case SimpleEdge:
			if edge.To != nil {
				tm.wg.Add(1)
				t := NewTask(task.ID, result.Payload, edge.From.Key, task.Results)
				ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: edge.To.Key})
				go tm.processNode(ctx, edge.To, t, node)
			} else if parentNode != nil {
				tm.appendFinalResult(result)
			}
		}
	}
}
