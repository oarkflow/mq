package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/oarkflow/mq/utils"
)

type Consumer struct {
	id       string
	handlers map[string]Handler
	conn     net.Conn
	queues   []string
	opts     Options
}

func NewConsumer(id string, opts ...Option) *Consumer {
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}
	con := &Consumer{
		handlers: make(map[string]Handler),
		id:       id,
	}
	if options.messageHandler == nil {
		options.messageHandler = con.readConn
	}

	if options.closeHandler == nil {
		options.closeHandler = con.onClose
	}

	if options.errorHandler == nil {
		options.errorHandler = con.onError
	}
	con.opts = options
	return con
}

func (c *Consumer) Close() error {
	return c.conn.Close()
}

func (c *Consumer) subscribe(queue string) error {
	ctx := context.Background()
	ctx = SetHeaders(ctx, map[string]string{
		ConsumerKey: c.id,
		ContentType: TypeJson,
	})
	subscribe := Command{
		Command: SUBSCRIBE,
		Queue:   queue,
		ID:      NewID(),
	}
	return Write(ctx, c.conn, subscribe)
}

func (c *Consumer) ProcessTask(ctx context.Context, msg Task) Result {
	handler, exists := c.handlers[msg.CurrentQueue]
	if !exists {
		return Result{Error: errors.New("No handler for queue " + msg.CurrentQueue)}
	}
	return handler(ctx, msg)
}

func (c *Consumer) handleCommandMessage(msg Command) error {
	switch msg.Command {
	case STOP:
		return c.Close()
	default:
		return fmt.Errorf("unknown command in consumer %d", msg.Command)
	}
}

func (c *Consumer) handleTaskMessage(ctx context.Context, msg Task) error {
	response := c.ProcessTask(ctx, msg)
	response.Queue = msg.CurrentQueue
	if msg.ID == "" {
		response.Error = errors.New("task ID is empty")
		response.Command = "error"
	} else {
		response.Command = "completed"
		response.MessageID = msg.ID
	}
	return c.sendResult(ctx, response)
}

func (c *Consumer) sendResult(ctx context.Context, response Result) error {
	return Write(ctx, c.conn, response)
}

func (c *Consumer) readMessage(ctx context.Context, message []byte) error {
	var cmdMsg Command
	var task Task
	err := json.Unmarshal(message, &cmdMsg)
	if err == nil && cmdMsg.Command != 0 {
		return c.handleCommandMessage(cmdMsg)
	}
	err = json.Unmarshal(message, &task)
	if err == nil {
		return c.handleTaskMessage(ctx, task)
	}
	return nil
}

func (c *Consumer) AttemptConnect() error {
	var conn net.Conn
	var err error
	delay := c.opts.initialDelay
	for i := 0; i < c.opts.maxRetries; i++ {
		conn, err = net.Dial("tcp", c.opts.brokerAddr)
		if err == nil {
			c.conn = conn
			return nil
		}
		sleepDuration := utils.CalculateJitter(delay, c.opts.jitterPercent)
		fmt.Printf("Failed connecting to %s (attempt %d/%d): %v, Retrying in %v...\n", c.opts.brokerAddr, i+1, c.opts.maxRetries, err, sleepDuration)
		time.Sleep(sleepDuration)
		delay *= 2
		if delay > c.opts.maxBackoff {
			delay = c.opts.maxBackoff
		}
	}

	return fmt.Errorf("could not connect to server %s after %d attempts: %w", c.opts.brokerAddr, c.opts.maxRetries, err)
}

func (c *Consumer) readConn(ctx context.Context, conn net.Conn, message []byte) error {
	return c.readMessage(ctx, message)
}

func (c *Consumer) onClose(ctx context.Context, conn net.Conn) error {
	fmt.Println("Consumer Connection closed", c.id, conn.RemoteAddr())
	return nil
}

func (c *Consumer) onError(ctx context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from consumer connection:", err, conn.RemoteAddr())
}

func (c *Consumer) Consume(ctx context.Context) error {
	err := c.AttemptConnect()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ReadFromConn(ctx, c.conn, c.opts.messageHandler, c.opts.closeHandler, c.opts.errorHandler)
		fmt.Println("Stopping consumer")
	}()
	for _, q := range c.queues {
		if err := c.subscribe(q); err != nil {
			return fmt.Errorf("failed to connect to server for queue %s: %v", q, err)
		}
	}
	wg.Wait()
	return nil
}

func (c *Consumer) RegisterHandler(queue string, handler Handler) {
	c.queues = append(c.queues, queue)
	c.handlers[queue] = handler
}
