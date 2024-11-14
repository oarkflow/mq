package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
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

func (tm *TaskManager) processTask(ctx context.Context, nodeID string, payload json.RawMessage) mq.Result {
	defer mq.RecoverPanic(mq.RecoverTitle)
	node, ok := tm.dag.nodes[nodeID]
	if !ok {
		return mq.Result{Error: fmt.Errorf("nodeID %s not found", nodeID)}
	}
	if tm.createdAt.IsZero() {
		tm.createdAt = time.Now()
	}
	tm.wg.Add(1)
	go func() {
		go tm.processNode(ctx, node, payload)
	}()
	tm.wg.Wait()
	return tm.dispatchFinalResult(ctx)
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

func (tm *TaskManager) getConditionalEdges(node *Node, result mq.Result) []Edge {
	edges := make([]Edge, len(node.Edges))
	copy(edges, node.Edges)
	if result.ConditionStatus != "" {
		if conditions, ok := tm.dag.conditions[FromNode(result.Topic)]; ok {
			if targetNodeKey, ok := conditions[When(result.ConditionStatus)]; ok {
				if targetNode, ok := tm.dag.nodes[string(targetNodeKey)]; ok {
					edges = append(edges, Edge{From: node, To: []*Node{targetNode}})
				}
			} else if targetNodeKey, ok = conditions["default"]; ok {
				if targetNode, ok := tm.dag.nodes[string(targetNodeKey)]; ok {
					edges = append(edges, Edge{From: node, To: []*Node{targetNode}})
				}
			}
		}
	}
	return edges
}

func (tm *TaskManager) handleNextTask(ctx context.Context, result mq.Result) mq.Result {
	if result.Ctx != nil {
		if headers, ok := mq.GetHeaders(ctx); ok {
			ctx = mq.SetHeaders(result.Ctx, headers.AsMap())
		}
	}
	defer func() {
		tm.wg.Done()
		mq.RecoverPanic(mq.RecoverTitle)
	}()
	node, ok := tm.dag.nodes[result.Topic]
	if !ok {
		return result
	}
	if result.Error != nil {
		tm.appendResult(result, true)
		return result
	}
	edges := tm.getConditionalEdges(node, result)
	if len(edges) == 0 {
		tm.appendResult(result, true)
		return result
	} else {
		tm.appendResult(result, false)
	}
	for _, edge := range edges {
		switch edge.Type {
		case Iterator:
			var items []json.RawMessage
			err := json.Unmarshal(result.Payload, &items)
			if err != nil {
				tm.appendResult(mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: err}, false)
				return result
			}
			for _, target := range edge.To {
				for _, item := range items {
					ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: target.Key})
					tm.wg.Add(1)
					go func(ctx context.Context, target *Node, item json.RawMessage) {
						tm.processNode(ctx, target, item)
					}(ctx, target, item)
				}
			}
		case Simple:
			for _, target := range edge.To {
				ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: target.Key})
				tm.wg.Add(1)
				go func(ctx context.Context, target *Node, result mq.Result) {
					tm.processNode(ctx, target, result.Payload)
				}(ctx, target, result)
			}
		}
	}
	return result
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

func (tm *TaskManager) processNode(ctx context.Context, node *Node, payload json.RawMessage) {
	defer mq.RecoverPanic(mq.RecoverTitle)
	dag, isDAG := isDAGNode(node)
	if isDAG {
		if tm.dag.server.SyncMode() && !dag.server.SyncMode() {
			dag.server.Options().SetSyncMode(true)
		}
	}

	var result mq.Result
	if tm.dag.server.SyncMode() {
		defer func() {
			result.Topic = node.Key
			tm.appendResult(result, false)
			tm.handleNextTask(ctx, result)
		}()
	}
	select {
	case <-ctx.Done():
		result = mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: ctx.Err(), Ctx: ctx}
		tm.appendResult(result, true)
		return
	default:
		ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: node.Key})
		if tm.dag.server.SyncMode() {
			result = node.ProcessTask(ctx, mq.NewTask(tm.taskID, payload, node.Key))
			if isDAG {
				result.Topic = dag.consumerTopic
				result.TaskID = tm.taskID
			}
			if result.Error != nil {
				tm.appendResult(result, true)
				return
			}
			return
		}
		err := tm.dag.server.Publish(ctx, mq.NewTask(tm.taskID, payload, node.Key), node.Key)
		if err != nil {
			tm.appendResult(mq.Result{Error: err}, true)
			return
		}
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
