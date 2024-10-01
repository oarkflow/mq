package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
)

type Publisher struct {
	id   string
	opts Options
}

func NewPublisher(id string, opts ...Option) *Publisher {
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}
	b := &Publisher{id: id}
	b.opts = defaultHandlers(options, nil, b.onClose, b.onError)
	return b
}

func (p *Publisher) send(ctx context.Context, queue string, task Task, conn net.Conn, command CMD) error {
	ctx = SetHeaders(ctx, map[string]string{
		PublisherKey: p.id,
		ContentType:  TypeJson,
	})
	cmd := Command{
		ID:        NewID(),
		Command:   command,
		Queue:     queue,
		MessageID: task.ID,
		Payload:   task.Payload,
	}
	return Write(ctx, conn, cmd)
}

func (p *Publisher) Publish(ctx context.Context, queue string, task Task) error {
	conn, err := GetConnection(p.opts.brokerAddr, p.opts.tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()
	return p.send(ctx, queue, task, conn, PUBLISH)
}

func (p *Publisher) onClose(ctx context.Context, conn net.Conn) error {
	fmt.Println("Publisher Connection closed", p.id, conn.RemoteAddr())
	return nil
}

func (p *Publisher) onError(ctx context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from publisher connection:", err, conn.RemoteAddr())
}

func (p *Publisher) Request(ctx context.Context, queue string, task Task) (Result, error) {
	conn, err := GetConnection(p.opts.brokerAddr, p.opts.tlsConfig)
	if err != nil {
		return Result{Error: err}, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()
	var result Result
	err = p.send(ctx, queue, task, conn, REQUEST)
	if err != nil {
		return result, err
	}
	if p.opts.messageHandler == nil {
		p.opts.messageHandler = func(ctx context.Context, conn net.Conn, message []byte) error {
			err := json.Unmarshal(message, &result)
			if err != nil {
				return err
			}
			return conn.Close()
		}
	}
	ReadFromConn(ctx, conn, p.opts.messageHandler, p.opts.closeHandler, p.opts.errorHandler)
	return result, nil
}
