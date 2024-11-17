package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/mq/consts"
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
				tm.reportNodeResult(result, false)
				tm.handleNextTask(result.Ctx, result)
			} else {
				result.Topic = node.Key
				tm.reportNodeResult(result, false)
				tm.handleNextTask(ctx, result)
			}
		}()
	}
	select {
	case <-ctx.Done():
		result = mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: ctx.Err(), Ctx: ctx}
		tm.reportNodeResult(result, true)
		tm.ChangeNodeStatus(ctx, node.Key, Failed, result)
		return
	default:
		ctx = mq.SetHeaders(ctx, map[string]string{consts.QueueKey: node.Key})
		if tm.dag.server.SyncMode() {
			result = node.ProcessTask(ctx, mq.NewTask(tm.taskID, payload, node.Key))
			if result.Error != nil {
				tm.reportNodeResult(result, true)
				tm.ChangeNodeStatus(ctx, node.Key, Failed, result)
				return
			}
			return
		}
		err := tm.dag.server.Publish(ctx, mq.NewTask(tm.taskID, payload, node.Key), node.Key)
		if err != nil {
			tm.reportNodeResult(mq.Result{Error: err}, true)
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
		tm.reportNodeResult(result, true)
		tm.ChangeNodeStatus(ctx, node.Key, Failed, result)
		return result
	}
	edges := tm.getConditionalEdges(node, result)
	if len(edges) == 0 {
		tm.reportNodeResult(result, true)
		tm.ChangeNodeStatus(ctx, node.Key, Completed, result)
		return result
	} else {
		tm.reportNodeResult(result, false)
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
				tm.reportNodeResult(mq.Result{TaskID: tm.taskID, Topic: node.Key, Error: err}, false)
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
			if _, ok := tm.iteratorNodes.Get(edge.From.Key); ok {
				continue
			}
			tm.processEdge(ctx, edge, result)
		}
	}
	return result
}

func (tm *TaskManager) processEdge(ctx context.Context, edge Edge, result mq.Result) {
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

func (tm *TaskManager) ChangeNodeStatus(ctx context.Context, nodeID string, status NodeStatus, rs mq.Result) {
	topic := nodeID
	if !strings.Contains(nodeID, "__") {
		nodeID = getTopic(ctx, nodeID)
	} else {
		topic = strings.Split(nodeID, "__")[0]
	}
	nodeStatus, ok := tm.taskNodeStatus.Get(nodeID)
	if !ok || nodeStatus == nil {
		return
	}

	nodeStatus.markAs(rs, status)
	switch status {
	case Completed:
		canProceed := false
		edges, ok := tm.iteratorNodes.Get(topic)
		if ok {
			if len(edges) == 0 {
				canProceed = true
			} else {
				nodeStatus.status = Processing
				nodeStatus.totalItems = 1
				nodeStatus.itemResults.Clear()
				for _, edge := range edges {
					tm.processEdge(ctx, edge, rs)
				}
				tm.iteratorNodes.Del(topic)
			}
		}
		if canProceed || !ok {
			if topic == tm.dag.startNode {
				tm.result = rs
			} else {
				tm.markParentTask(ctx, topic, nodeID, status, rs)
			}
		}
	case Failed:
		if topic == tm.dag.startNode {
			tm.result = rs
		} else {
			tm.markParentTask(ctx, topic, nodeID, status, rs)
		}
	}
}

func (tm *TaskManager) markParentTask(ctx context.Context, topic, nodeID string, status NodeStatus, rs mq.Result) {
	parentNodes, err := tm.dag.GetPreviousNodes(topic)
	if err != nil {
		return
	}
	var index string
	nodeParts := strings.Split(nodeID, "__")
	if len(nodeParts) == 2 {
		index = nodeParts[1]
	}
	for _, parentNode := range parentNodes {
		parentKey := fmt.Sprintf("%s__%s", parentNode.Key, index)
		parentNodeStatus, exists := tm.taskNodeStatus.Get(parentKey)
		if !exists {
			parentKey = fmt.Sprintf("%s__%s", parentNode.Key, "0")
			parentNodeStatus, exists = tm.taskNodeStatus.Get(parentKey)
		}
		if exists {
			parentNodeStatus.itemResults.Set(nodeID, rs)
			if parentNodeStatus.IsDone() {
				rt := tm.prepareResult(ctx, parentNodeStatus)
				tm.ChangeNodeStatus(ctx, parentKey, status, rt)
			}
		}
	}
}

func (tm *TaskManager) prepareResult(ctx context.Context, nodeStatus *taskNodeStatus) mq.Result {
	aggregatedOutput := make([]json.RawMessage, 0)
	var status, topic string
	var err1 error
	if nodeStatus.totalItems == 1 {
		rs := nodeStatus.itemResults.Values()[0]
		if rs.Ctx == nil {
			rs.Ctx = ctx
		}
		return rs
	}
	nodeStatus.itemResults.ForEach(func(key string, result mq.Result) bool {
		if topic == "" {
			topic = result.Topic
			status = result.Status
		}
		if result.Error != nil {
			err1 = result.Error
			return false
		}
		var item json.RawMessage
		err := json.Unmarshal(result.Payload, &item)
		if err != nil {
			err1 = err
			return false
		}
		aggregatedOutput = append(aggregatedOutput, item)
		return true
	})
	if err1 != nil {
		return mq.HandleError(ctx, err1)
	}
	finalOutput, err := json.Marshal(aggregatedOutput)
	if err != nil {
		return mq.HandleError(ctx, err)
	}
	return mq.Result{TaskID: tm.taskID, Payload: finalOutput, Status: status, Topic: topic, Ctx: ctx}
}
