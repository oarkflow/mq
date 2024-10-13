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
	taskID      string
	dag         *DAG
	mutex       sync.Mutex
	createdAt   time.Time
	processedAt time.Time
	results     []mq.Result
	nodeResults map[string]mq.Result
	finalResult chan mq.Result
	wg          *WaitGroup
}

func NewTaskManager(d *DAG, taskID string) *TaskManager {
	return &TaskManager{
		dag:         d,
		nodeResults: make(map[string]mq.Result),
		results:     make([]mq.Result, 0),
		taskID:      taskID,
		finalResult: make(chan mq.Result, 1),
		wg:          NewWaitGroup(),
	}
}

func (tm *TaskManager) updateTS(result *mq.Result) {
	result.CreatedAt = tm.createdAt
	result.ProcessedAt = time.Now()
}

func (tm *TaskManager) processTask(ctx context.Context, nodeID string, payload json.RawMessage) mq.Result {
	node, ok := tm.dag.nodes[nodeID]
	if !ok {
		return mq.Result{Error: fmt.Errorf("nodeID %s not found", nodeID)}
	}
	tm.createdAt = time.Now()
	go tm.processNode(ctx, node, payload)
	awaitResponse, ok := mq.GetAwaitResponse(ctx)
	if awaitResponse != "true" {
		go func() {
			finalResult := <-tm.finalResult
			tm.updateTS(&finalResult)
			tm.dag.callbackToConsumer(ctx, finalResult)
			if tm.dag.server.NotifyHandler() != nil {
				tm.dag.server.NotifyHandler()(ctx, finalResult)
			}
		}()
		return mq.Result{CreatedAt: tm.createdAt, TaskID: tm.taskID, Topic: nodeID, Status: "PENDING"}
	} else {
		finalResult := <-tm.finalResult
		tm.updateTS(&finalResult)
		tm.dag.callbackToConsumer(ctx, finalResult)
		if tm.dag.server.NotifyHandler() != nil {
			tm.dag.server.NotifyHandler()(ctx, finalResult)
		}
		return finalResult
	}
}

func (tm *TaskManager) dispatchFinalResult(ctx context.Context) mq.Result {
	var rs mq.Result
	if len(tm.results) == 1 {
		rs = tm.handleResult(ctx, tm.results[0])
	} else {
		rs = tm.handleResult(ctx, tm.results)
	}
	return rs
}

func (tm *TaskManager) getConditionalEdges(node *Node, result mq.Result) []Edge {
	edges := make([]Edge, len(node.Edges))
	copy(edges, node.Edges)
	if result.Status != "" {
		if conditions, ok := tm.dag.conditions[FromNode(result.Topic)]; ok {
			if targetNodeKey, ok := conditions[When(result.Status)]; ok {
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

func (tm *TaskManager) handleCallback(ctx context.Context, result mq.Result) mq.Result {
	defer tm.wg.Done()
	node, ok := tm.dag.nodes[result.Topic]
	if !ok {
		return result
	}
	edges := tm.getConditionalEdges(node, result)
	if len(edges) == 0 {
		tm.appendFinalResult(result)
		return result
	}
	for _, edge := range edges {
		switch edge.Type {
		case Iterator:
			var items []json.RawMessage
			err := json.Unmarshal(result.Payload, &items)
			if err != nil {
				tm.appendFinalResult(mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: err})
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
					go tm.processNode(ctx, target, result.Payload)
				}(ctx, target, result)
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
		return mq.Result{TaskID: tm.taskID, Payload: finalOutput, Status: status, Topic: topic}
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
	var result mq.Result
	defer func() {
		tm.mutex.Lock()
		tm.nodeResults[node.Key] = result
		tm.mutex.Unlock()
		tm.handleCallback(ctx, result)
	}()
	select {
	case <-ctx.Done():
		result = mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: ctx.Err()}
		tm.appendFinalResult(result)
		return
	default:
		ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: node.Key})
		if tm.dag.server.SyncMode() {
			result = node.ProcessTask(ctx, NewTask(tm.taskID, payload, node.Key))
			if result.Error != nil {
				tm.appendFinalResult(result)
				return
			}
			return
		}
		err := tm.dag.server.Publish(ctx, NewTask(tm.taskID, payload, node.Key), node.Key)
		if err != nil {
			tm.appendFinalResult(mq.Result{Error: err})
			return
		}
	}
}

func (tm *TaskManager) Clear() error {
	clear(tm.results)
	tm.nodeResults = make(map[string]mq.Result)
	return nil
}
