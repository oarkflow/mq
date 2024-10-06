package dag

import (
	"context"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

func setTaskHeader(ctx context.Context, triggerNode, target string) context.Context {
	return mq.SetHeaders(ctx, map[string]string{
		consts.QueueKey:    target,
		consts.TriggerNode: triggerNode,
	})
}
