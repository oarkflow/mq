package mq

import (
	"context"
	"fmt"
	"net"

	"github.com/oarkflow/mq/utils"
)

type Publisher struct {
	brokerAddr string
}

func NewPublisher(brokerAddr string) *Publisher {
	return &Publisher{brokerAddr: brokerAddr}
}

func (p *Publisher) Publish(ctx context.Context, queue string, task Task) error {
	conn, err := net.Dial("tcp", p.brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()
	cmd := Command{
		Command:   PUBLISH,
		Queue:     queue,
		MessageID: task.ID,
		Error:     string(task.Payload),
	}
	return utils.Write(ctx, conn, cmd)
}
