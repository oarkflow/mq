package mq

import (
	"context"
	"fmt"
	"net"
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
	ctx = SetHeaders(ctx, map[string]string{
		PublisherKey: p.id,
		ContentType:  TypeJson,
	})
	cmd := Command{
		ID:        NewID(),
		Command:   PUBLISH,
		Queue:     queue,
		MessageID: task.ID,
		Payload:   task.Payload,
	}
	return Write(ctx, conn, cmd)
}

func (p *Publisher) readConn(ctx context.Context, conn net.Conn, message []byte) error {
	fmt.Println(string(message), conn.RemoteAddr())
	return conn.Close()
}

func (p *Publisher) onClose(ctx context.Context, conn net.Conn) error {
	fmt.Println("Publisher Connection closed", p.id, conn.RemoteAddr())
	return nil
}

func (p *Publisher) onError(ctx context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from publisher connection:", err, conn.RemoteAddr())
}

func (p *Publisher) Request(ctx context.Context, queue string, task Task) (Result, error) {
	conn, err := net.Dial("tcp", p.brokerAddr)
	if err != nil {
		return Result{}, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()
	ctx = SetHeaders(ctx, map[string]string{
		PublisherKey: p.id,
		ContentType:  TypeJson,
	})
	cmd := Command{
		ID:        NewID(),
		Command:   REQUEST,
		Queue:     queue,
		MessageID: task.ID,
		Payload:   task.Payload,
	}
	var result Result
	err = Write(ctx, conn, cmd)
	if err != nil {
		return result, err
	}
	ReadFromConn(ctx, conn, p.readConn, p.onClose, p.onError)
	return result, nil
}
