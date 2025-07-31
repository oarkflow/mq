package mq

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/json/jsonparser"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/utils"
)

type Publisher struct {
	opts     *Options
	id       string
	conn     net.Conn
	connLock sync.Mutex
}

func NewPublisher(id string, opts ...Option) *Publisher {
	options := SetupOptions(opts...)
	return &Publisher{
		id:   id,
		opts: options,
		conn: nil,
	}
}

// New method to ensure a persistent connection.
func (p *Publisher) ensureConnection(ctx context.Context) error {
	p.connLock.Lock()
	defer p.connLock.Unlock()
	if p.conn != nil {
		return nil
	}
	var err error
	delay := p.opts.initialDelay
	for i := 0; i < p.opts.maxRetries; i++ {
		var conn net.Conn
		conn, err = GetConnection(p.opts.brokerAddr, p.opts.tlsConfig)
		if err == nil {
			p.conn = conn
			return nil
		}
		sleepDuration := utils.CalculateJitter(delay, p.opts.jitterPercent)
		log.Printf("PUBLISHER - ensureConnection failed: %v, attempt %d/%d, retrying in %v...", err, i+1, p.opts.maxRetries, sleepDuration)
		time.Sleep(sleepDuration)
		delay *= 2
		if delay > p.opts.maxBackoff {
			delay = p.opts.maxBackoff
		}
	}
	return fmt.Errorf("failed to connect to broker after retries: %w", err)
}

// Publish method that uses the persistent connection.
func (p *Publisher) Publish(ctx context.Context, task Task, queue string) error {
	// Ensure connection is established.
	if err := p.ensureConnection(ctx); err != nil {
		return err
	}
	delay := p.opts.initialDelay
	for i := 0; i < p.opts.maxRetries; i++ {
		// Use the persistent connection.
		p.connLock.Lock()
		conn := p.conn
		p.connLock.Unlock()
		err := p.send(ctx, queue, task, conn, consts.PUBLISH)
		if err == nil {
			return nil
		}
		log.Printf("PUBLISHER - Failed publishing: %v, attempt %d/%d, retrying...", err, i+1, p.opts.maxRetries)
		// On error, close and reset the connection.
		p.connLock.Lock()
		if p.conn != nil {
			p.conn.Close()
			p.conn = nil
		}
		p.connLock.Unlock()
		sleepDuration := utils.CalculateJitter(delay, p.opts.jitterPercent)
		time.Sleep(sleepDuration)
		delay *= 2
		if delay > p.opts.maxBackoff {
			delay = p.opts.maxBackoff
		}
		// Ensure connection is re-established.
		if err := p.ensureConnection(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("failed to publish after retries")
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
	msg, err := codec.NewMessage(command, payload, queue, headers)
	if err != nil {
		return err
	}
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

func (p *Publisher) onClose(_ context.Context, conn net.Conn) error {
	fmt.Println("Publisher Connection closed", p.id, conn.RemoteAddr())
	return nil
}

func (p *Publisher) onError(_ context.Context, conn net.Conn, err error) {
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
	defer func() {
		_ = conn.Close()
	}()
	err = p.send(ctx, queue, task, conn, consts.PUBLISH)
	resultCh := make(chan Result)
	go func() {
		defer close(resultCh)
		resultCh <- p.waitForResponse(ctx, conn)
	}()
	finalResult := <-resultCh
	return finalResult
}
