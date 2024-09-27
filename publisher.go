package mq

import (
	"bufio"
	"context"
	"encoding/json"
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

func (p *Publisher) PublishAsync(ctx context.Context, queue string, task Task) error {
	conn, err := net.Dial("tcp", p.brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	cmd := Command{
		Command:   PUBLISH,
		Queue:     queue,
		MessageID: task.ID,
		Payload:   task.Payload,
	}

	// Fire and forget: No need to wait for response
	return utils.Write(ctx, conn, cmd)
}

func (p *Publisher) PublishSync(ctx context.Context, queue string, task Task) (Result, error) {
	conn, err := net.Dial("tcp", p.brokerAddr)
	if err != nil {
		return Result{}, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	cmd := Command{
		Command:   PUBLISH,
		Queue:     queue,
		MessageID: task.ID,
		Payload:   task.Payload,
	}

	err = utils.Write(ctx, conn, cmd)
	if err != nil {
		return Result{}, err
	}

	// Wait for response from broker/consumer
	resultBytes, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		return Result{}, fmt.Errorf("failed to read response: %w", err)
	}

	var result Result
	err = json.Unmarshal(resultBytes, &result)
	if err != nil {
		return Result{}, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return result, nil
}
