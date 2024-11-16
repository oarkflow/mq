package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
	"strings"
)

type NodeStatus int

func (c NodeStatus) IsValid() bool { return c >= Pending && c <= Failed }

func (c NodeStatus) String() string {
	switch c {
	case Pending:
		return "Pending"
	case Processing:
		return "Processing"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	}
	return ""
}

const (
	Pending NodeStatus = iota
	Processing
	Completed
	Failed
)

type taskNodeStatus struct {
	node        string
	itemResults storage.IMap[string, mq.Result]
	status      NodeStatus
	result      mq.Result
	totalItems  int
}

func newNodeStatus(node string) *taskNodeStatus {
	return &taskNodeStatus{
		node:        node,
		itemResults: memory.New[string, mq.Result](),
		status:      Pending,
	}
}

func (t *taskNodeStatus) IsDone() bool {
	return t.itemResults.Size() >= t.totalItems
}

func (t *taskNodeStatus) markAs(rs mq.Result, status NodeStatus) {
	t.result = rs
	t.status = status
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
	case Completed, Failed:
		// tm.markParentTask(ctx, topic, nodeID, status, rs)
	}
	fmt.Println(topic, "Topic", status, nodeStatus.node, string(nodeStatus.result.Payload))
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
				fmt.Println("Parent result", parentKey, nodeID, string(rt.Payload), status)
				tm.ChangeNodeStatus(ctx, parentKey, status, rt)
			} else {

				fmt.Println("Parent result Nto done", parentKey, nodeID, string(rs.Payload), status)
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

func getTopic(ctx context.Context, topic string) string {
	if index, ok := mq.GetHeader(ctx, "index"); ok && index != "" {
		topic = index
	}
	return topic
}
