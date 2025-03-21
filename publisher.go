package mq

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/json/jsonparser"
	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
)

type Publisher struct {
	opts *Options
	id   string
}

func NewPublisher(id string, opts ...Option) *Publisher {
	options := SetupOptions(opts...)
	return &Publisher{id: id, opts: options}
}

func (p *Publisher) send(ctx context.Context, queue string, task Task, conn net.Conn, command consts.CMD) error {
	headers := WithHeaders(ctx, map[string]string{
		consts.PublisherKey: p.id,
		consts.ContentType:  consts.TypeJson,
	})
	if task.ID == "" {
		task.ID = NewID()
	}
	task.CreatedAt = time.Now()
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}
	msg := codec.NewMessage(command, payload, queue, headers)
	if err := codec.SendMessage(ctx, conn, msg); err != nil {
		return err
	}

	return p.waitForAck(ctx, conn)
}

func (p *Publisher) waitForAck(ctx context.Context, conn net.Conn) error {
	msg, err := codec.ReadMessage(ctx, conn)
	if err != nil {
		return err
	}
	if msg.Command == consts.PUBLISH_ACK {
		taskID, _ := jsonparser.GetString(msg.Payload, "id")
		log.Printf("PUBLISHER - PUBLISH_ACK ~> from %s on %s for Task %s", p.id, msg.Queue, taskID)
		return nil
	}
	return fmt.Errorf("expected PUBLISH_ACK, got: %v", msg.Command)
}

func (p *Publisher) waitForResponse(ctx context.Context, conn net.Conn) Result {
	msg, err := codec.ReadMessage(ctx, conn)
	if err != nil {
		return Result{Error: err}
	}
	if msg.Command == consts.RESPONSE {
		var result Result
		err = json.Unmarshal(msg.Payload, &result)
		return result
	}
	err = fmt.Errorf("expected RESPONSE, got: %v", msg.Command)
	return Result{Error: err}
}

func (p *Publisher) Publish(ctx context.Context, task Task, queue string) error {
	conn, err := GetConnection(p.opts.brokerAddr, p.opts.tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()
	return p.send(ctx, queue, task, conn, consts.PUBLISH)
}

func (p *Publisher) onClose(ctx context.Context, conn net.Conn) error {
	fmt.Println("Publisher Connection closed", p.id, conn.RemoteAddr())
	return nil
}

func (p *Publisher) onError(ctx context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from publisher connection:", err, conn.RemoteAddr())
}

func (p *Publisher) Request(ctx context.Context, task Task, queue string) Result {
	ctx = SetHeaders(ctx, map[string]string{
		consts.AwaitResponseKey: "true",
	})
	conn, err := GetConnection(p.opts.brokerAddr, p.opts.tlsConfig)
	if err != nil {
		err = fmt.Errorf("failed to connect to broker: %w", err)
		return Result{Error: err}
	}
	defer conn.Close()
	err = p.send(ctx, queue, task, conn, consts.PUBLISH)
	resultCh := make(chan Result)
	go func() {
		defer close(resultCh)
		resultCh <- p.waitForResponse(ctx, conn)
	}()
	finalResult := <-resultCh
	return finalResult
}
