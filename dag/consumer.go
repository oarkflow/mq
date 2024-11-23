package dag

import (
	"context"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
	"log"
)

func (tm *DAG) Consume(ctx context.Context) error {
	if tm.consumer != nil {
		tm.server.Options().SetSyncMode(true)
		return tm.consumer.Consume(ctx)
	}
	return nil
}

func (tm *DAG) AssignTopic(topic string) {
	tm.consumer = mq.NewConsumer(topic, topic, tm.ProcessTask, mq.WithRespondPendingResult(false), mq.WithBrokerURL(tm.server.URL()))
	tm.consumerTopic = topic
}

func (tm *DAG) callbackToConsumer(ctx context.Context, result mq.Result) {
	if tm.consumer != nil {
		result.Topic = tm.consumerTopic
		if tm.consumer.Conn() == nil {
			tm.onTaskCallback(ctx, result)
		} else {
			tm.consumer.OnResponse(ctx, result)
		}
	}
}

func (tm *DAG) onConsumerJoin(_ context.Context, topic, _ string) {
	if node, ok := tm.nodes.Get(topic); ok {
		log.Printf("DAG - CONSUMER ~> ready on %s", topic)
		node.isReady = true
	}
}

func (tm *DAG) onConsumerClose(_ context.Context, topic, _ string) {
	if node, ok := tm.nodes.Get(topic); ok {
		log.Printf("DAG - CONSUMER ~> down on %s", topic)
		node.isReady = false
	}
}

func (tm *DAG) Pause(_ context.Context) error {
	tm.paused = true
	return nil
}

func (tm *DAG) Resume(_ context.Context) error {
	tm.paused = false
	return nil
}

func (tm *DAG) Close() error {
	var err error
	tm.nodes.ForEach(func(_ string, n *Node) bool {
		err = n.processor.Close()
		if err != nil {
			return false
		}
		return true
	})
	return nil
}

func (tm *DAG) PauseConsumer(ctx context.Context, id string) {
	tm.doConsumer(ctx, id, consts.CONSUMER_PAUSE)
}

func (tm *DAG) ResumeConsumer(ctx context.Context, id string) {
	tm.doConsumer(ctx, id, consts.CONSUMER_RESUME)
}

func (tm *DAG) doConsumer(ctx context.Context, id string, action consts.CMD) {
	if node, ok := tm.nodes.Get(id); ok {
		switch action {
		case consts.CONSUMER_PAUSE:
			err := node.processor.Pause(ctx)
			if err == nil {
				node.isReady = false
				log.Printf("[INFO] - Consumer %s paused successfully", node.ID)
			} else {
				log.Printf("[ERROR] - Failed to pause consumer %s: %v", node.ID, err)
			}
		case consts.CONSUMER_RESUME:
			err := node.processor.Resume(ctx)
			if err == nil {
				node.isReady = true
				log.Printf("[INFO] - Consumer %s resumed successfully", node.ID)
			} else {
				log.Printf("[ERROR] - Failed to resume consumer %s: %v", node.ID, err)
			}
		}
	} else {
		log.Printf("[WARNING] - Consumer %s not found", id)
	}
}
