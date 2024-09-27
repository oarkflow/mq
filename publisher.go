package mq

import (
	"context"
	"fmt"
	"net"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/mq/utils"
)

type Publisher struct {
	id         string
	brokerAddr string
}

func NewPublisher(id, brokerAddr string) *Publisher {
	return &Publisher{brokerAddr: brokerAddr, id: id}
}

func (p *Publisher) Publish(ctx context.Context, queue string, task Task) error {
	conn, err := net.Dial("tcp", p.brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	cmd := Command{
		ID:        xid.New().String(),
		Command:   PUBLISH,
		Queue:     queue,
		MessageID: task.ID,
		Payload:   task.Payload,
		Options: map[string]any{
			"publisher_id": p.id,
		},
	}

	// Fire and forget: No need to wait for response
	return utils.Write(ctx, conn, cmd)
}

func (p *Publisher) Request(ctx context.Context, queue string, task Task) (Result, error) {
	conn, err := net.Dial("tcp", p.brokerAddr)
	if err != nil {
		return Result{}, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()
	cmd := Command{
		ID:        xid.New().String(),
		Command:   REQUEST,
		Queue:     queue,
		MessageID: task.ID,
		Payload:   task.Payload,
		Options: map[string]any{
			"publisher_id": p.id,
		},
	}
	var result Result
	err = utils.Write(ctx, conn, cmd)
	if err != nil {
		return result, err
	}
	utils.ReadFromConn(ctx, conn, func(ctx context.Context, conn net.Conn, bytes []byte) error {
		fmt.Println(string(bytes))
		return nil
	})
	return result, nil
}
