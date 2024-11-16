package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

func (tm *TaskManager) processNode(ctx context.Context, node *Node, payload json.RawMessage) {
	topic := getTopic(ctx, node.Key)
	tm.taskNodeStatus.Set(topic, newNodeStatus(topic))
	defer mq.RecoverPanic(mq.RecoverTitle)
	dag, isDAG := isDAGNode(node)
	if isDAG {
		if tm.dag.server.SyncMode() && !dag.server.SyncMode() {
			dag.server.Options().SetSyncMode(true)
		}
	}
	tm.ChangeNodeStatus(ctx, node.Key, Processing, mq.Result{Payload: payload, Topic: node.Key})
	var result mq.Result
	if tm.dag.server.SyncMode() {
		defer func() {
			if isDAG {
				result.Topic = dag.consumerTopic
				result.TaskID = tm.taskID
				tm.appendResult(result, false)
				tm.handleNextTask(result.Ctx, result)
			} else {
				result.Topic = node.Key
				tm.appendResult(result, false)
				tm.handleNextTask(ctx, result)
			}
		}()
	}
	select {
	case <-ctx.Done():
		result = mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: ctx.Err(), Ctx: ctx}
		tm.appendResult(result, true)
		tm.ChangeNodeStatus(ctx, node.Key, Failed, result)
		return
	default:
		ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: node.Key})
		if tm.dag.server.SyncMode() {
			result = node.ProcessTask(ctx, mq.NewTask(tm.taskID, payload, node.Key))
			if result.Error != nil {
				tm.appendResult(result, true)
				tm.ChangeNodeStatus(ctx, node.Key, Failed, result)
				return
			}
			return
		}
		err := tm.dag.server.Publish(ctx, mq.NewTask(tm.taskID, payload, node.Key), node.Key)
		if err != nil {
			tm.appendResult(mq.Result{Error: err}, true)
			tm.ChangeNodeStatus(ctx, node.Key, Failed, result)
			return
		}
	}
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
		ctxx := context.Background()
		if headers, ok := mq.GetHeaders(ctx); ok {
			headers.Set(consts.QueueKey, node.Key)
			headers.Set("index", fmt.Sprintf("%s__%d", node.Key, 0))
			ctxx = mq.SetHeaders(ctxx, headers.AsMap())
		}
		go tm.processNode(ctx, node, payload)
	}()
	tm.wg.Wait()
	requestType, ok := mq.GetHeader(ctx, "request_type")
	if ok && requestType == "render" {
		return tm.renderResult(ctx)
	}
	return tm.dispatchFinalResult(ctx)
}

func (tm *TaskManager) handleNextTask(ctx context.Context, result mq.Result) mq.Result {
	tm.topic = result.Topic
	defer func() {
		tm.wg.Done()
		mq.RecoverPanic(mq.RecoverTitle)
	}()
	if result.Ctx != nil {
		if headers, ok := mq.GetHeaders(ctx); ok {
			ctx = mq.SetHeaders(result.Ctx, headers.AsMap())
		}
	}
	node, ok := tm.dag.nodes[result.Topic]
	if !ok {
		return result
	}
	if result.Error != nil {
		tm.appendResult(result, true)
		tm.ChangeNodeStatus(ctx, node.Key, Failed, result)
		return result
	}
	edges := tm.getConditionalEdges(node, result)
	if len(edges) == 0 {
		tm.appendResult(result, true)
		tm.ChangeNodeStatus(ctx, node.Key, Completed, result)
		return result
	} else {
		tm.appendResult(result, false)
	}
	if node.Type == Page {
		return result
	}
	for _, edge := range edges {
		switch edge.Type {
		case Iterator:
			var items []json.RawMessage
			err := json.Unmarshal(result.Payload, &items)
			if err != nil {
				tm.appendResult(mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: err}, false)
				result.Error = err
				tm.ChangeNodeStatus(ctx, node.Key, Failed, result)
				return result
			}
			tm.SetTotalItems(getTopic(ctx, edge.From.Key), len(items)*len(edge.To))
			for _, target := range edge.To {
				for i, item := range items {
					tm.wg.Add(1)
					go func(ctx context.Context, target *Node, item json.RawMessage, i int) {
						ctxx := context.Background()
						if headers, ok := mq.GetHeaders(ctx); ok {
							headers.Set(consts.QueueKey, target.Key)
							headers.Set("index", fmt.Sprintf("%s__%d", target.Key, i))
							ctxx = mq.SetHeaders(ctxx, headers.AsMap())
						}
						tm.processNode(ctxx, target, item)
					}(ctx, target, item, i)
				}
			}
		}
	}
	for _, edge := range edges {
		switch edge.Type {
		case Simple:
			tm.SetTotalItems(getTopic(ctx, edge.From.Key), len(edge.To))
			index, _ := mq.GetHeader(ctx, "index")
			if index != "" && strings.Contains(index, "__") {
				index = strings.Split(index, "__")[1]
			} else {
				index = "0"
			}
			for _, target := range edge.To {
				tm.wg.Add(1)
				go func(ctx context.Context, target *Node, result mq.Result) {
					ctxx := context.Background()
					if headers, ok := mq.GetHeaders(ctx); ok {
						headers.Set(consts.QueueKey, target.Key)
						headers.Set("index", fmt.Sprintf("%s__%s", target.Key, index))
						ctxx = mq.SetHeaders(ctxx, headers.AsMap())
					}
					tm.processNode(ctxx, target, result.Payload)
				}(ctx, target, result)
			}
		}
	}
	return result
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

func (tm *TaskManager) renderResult(ctx context.Context) mq.Result {
	var rs mq.Result
	tm.updateTS(&rs)
	tm.dag.callbackToConsumer(ctx, rs)
	tm.topic = rs.Topic
	return rs
}
