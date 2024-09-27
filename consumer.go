package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/mq/utils"
)

type Consumer struct {
	id         string
	serverAddr string
	handlers   map[string]Handler
	queues     []string
	conn       net.Conn
}

func NewConsumer(id, serverAddr string, queues ...string) *Consumer {
	return &Consumer{
		handlers:   make(map[string]Handler),
		serverAddr: serverAddr,
		queues:     queues,
		id:         id,
	}
}

func (c *Consumer) Close() error {
	return c.conn.Close()
}

func (c *Consumer) subscribe(queue string) error {
	ctx := context.Background()
	subscribe := Command{
		Command: SUBSCRIBE,
		Queue:   queue,
		ID:      xid.New().String(),
		Options: map[string]any{
			"consumer_id": c.id,
		},
	}
	return utils.Write(ctx, c.conn, subscribe)
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
	return utils.Write(ctx, c.conn, response)
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

const (
	maxRetries    = 5
	initialDelay  = 2 * time.Second
	maxBackoff    = 30 * time.Second // Upper limit for backoff delay
	jitterPercent = 0.5              // 50% jitter
)

func (c *Consumer) AttemptConnect() error {
	var conn net.Conn
	var err error
	delay := initialDelay
	for i := 0; i < maxRetries; i++ {
		conn, err = net.Dial("tcp", c.serverAddr)
		if err == nil {
			c.conn = conn
			return nil
		}
		sleepDuration := calculateJitter(delay)
		fmt.Printf("Failed connecting to %s (attempt %d/%d): %v, Retrying in %v...\n", c.serverAddr, i+1, maxRetries, err, sleepDuration)
		time.Sleep(sleepDuration)
		delay *= 2
		if delay > maxBackoff {
			delay = maxBackoff
		}
	}

	return fmt.Errorf("could not connect to server %s after %d attempts: %w", c.serverAddr, maxRetries, err)
}

func calculateJitter(baseDelay time.Duration) time.Duration {
	jitter := time.Duration(rand.Float64()*jitterPercent*float64(baseDelay)) - time.Duration(jitterPercent*float64(baseDelay)/2)
	return baseDelay + jitter
}

func (c *Consumer) Consume(ctx context.Context, queues ...string) error {
	err := c.AttemptConnect()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		utils.ReadFromConn(ctx, c.conn, func(ctx context.Context, conn net.Conn, message []byte) error {
			return c.readMessage(ctx, message)
		})
		fmt.Println("Stopping consumer")
	}()
	c.queues = slices.Compact(append(c.queues, queues...))
	for _, q := range c.queues {
		if err := c.subscribe(q); err != nil {
			return fmt.Errorf("failed to connect to server for queue %s: %v", q, err)
		}
	}
	wg.Wait()
	return nil
}

func (c *Consumer) RegisterHandler(queue string, handler Handler) {
	c.handlers[queue] = handler
}
