package v1

import (
	"context"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
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

func isDAGNode(node *Node) (*DAG, bool) {
	switch node := node.processor.(type) {
	case *DAG:
		return node, true
	default:
		return nil, false
	}
}

func (tm *TaskManager) updateTS(result *mq.Result) {
	result.CreatedAt = tm.createdAt
	result.ProcessedAt = time.Now()
	result.Latency = time.Since(tm.createdAt).String()
}

func getTopic(ctx context.Context, topic string) string {
	if index, ok := mq.GetHeader(ctx, "index"); ok && index != "" {
		topic = index
	}
	return topic
}
