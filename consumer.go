package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/jsonparser"
	"github.com/oarkflow/mq/utils"
)

// Consumer defines the structure for a message consumer.
type Consumer struct {
	id      string
	handler Handler
	conn    net.Conn
	queue   string
	opts    Options
	pool    *Pool // Use the worker pool in the consumer
}

// NewConsumer initializes a new consumer and the worker pool.
func NewConsumer(id string, queue string, handler Handler, opts ...Option) *Consumer {
	options := setupOptions(opts...)

	// Initialize the consumer with worker pool
	conn, _ := GetConnection(options.brokerAddr, options.tlsConfig)
	c := &Consumer{
		id:      id,
		opts:    options,
		queue:   queue,
		handler: handler,
		conn:    conn,
	}

	// Create the worker pool, using ProcessTask as handler and MessageResponseCallback as callback
	c.pool = NewPool(options.numOfWorkers, options.queueSize, options.maxMemoryLoad, c.ProcessTask, c.MessageResponseCallback, conn)
	c.pool.Start(options.numOfWorkers)

	return c
}

func (c *Consumer) send(conn net.Conn, msg *codec.Message) error {
	return codec.SendMessage(conn, msg, c.opts.aesKey, c.opts.hmacKey, c.opts.enableEncryption)
}

func (c *Consumer) receive(conn net.Conn) (*codec.Message, error) {
	return codec.ReadMessage(conn, c.opts.aesKey, c.opts.hmacKey, c.opts.enableEncryption)
}

// Close closes the consumer's connection and stops the worker pool.
func (c *Consumer) Close() error {
	c.pool.Stop()
	return c.conn.Close()
}

// Subscribe to a specific queue.
func (c *Consumer) subscribe(ctx context.Context, queue string) error {
	headers := WithHeaders(ctx, map[string]string{
		consts.ConsumerKey: c.id,
		consts.ContentType: consts.TypeJson,
	})
	msg := codec.NewMessage(consts.SUBSCRIBE, nil, queue, headers)
	if err := c.send(c.conn, msg); err != nil {
		return err
	}

	return c.waitForAck(c.conn)
}

func (c *Consumer) OnClose(ctx context.Context, _ net.Conn) error {
	fmt.Println("Consumer closed")
	return nil
}

func (c *Consumer) OnError(_ context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from connection:", err, conn.RemoteAddr())
}

// OnMessage processes incoming messages and adds them to the pool.
func (c *Consumer) OnMessage(ctx context.Context, msg *codec.Message, conn net.Conn) {
	headers := WithHeaders(ctx, map[string]string{
		consts.ConsumerKey: c.id,
		consts.ContentType: consts.TypeJson,
	})
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	reply := codec.NewMessage(consts.MESSAGE_ACK, []byte(fmt.Sprintf(`{"id":"%s"}`, taskID)), msg.Queue, headers)
	if err := c.send(conn, reply); err != nil {
		fmt.Printf("failed to send MESSAGE_ACK for queue %s: %v", msg.Queue, err)
	}

	var task Task
	err := json.Unmarshal(msg.Payload, &task)
	if err != nil {
		log.Println("Error unmarshalling message:", err)
		return
	}

	ctx = SetHeaders(ctx, map[string]string{consts.QueueKey: msg.Queue})

	// Add the task to the worker pool
	if err := c.pool.AddTask(ctx, &task); err != nil {
		c.sendDenyMessage(ctx, taskID, msg.Queue, err)
		return
	}
}

// ProcessTask processes the task and returns the result.
func (c *Consumer) ProcessTask(ctx context.Context, task *Task) Result {
	return c.handler(ctx, task)
}

// MessageResponseCallback sends the result back to the broker.
func (c *Consumer) MessageResponseCallback(ctx context.Context, taskID string, result Result, queue string) error {
	headers := WithHeaders(ctx, map[string]string{
		consts.ConsumerKey: c.id,
		consts.QueueKey:    queue,
		consts.ContentType: consts.TypeJson,
	})

	if result.Status == "" {
		if result.Error != nil {
			result.Status = "FAILED"
		} else {
			result.Status = "SUCCESS"
		}
	}
	result.TaskID = taskID
	result.Topic = queue

	bt, _ := json.Marshal(result)
	reply := codec.NewMessage(consts.MESSAGE_RESPONSE, bt, queue, headers)
	if err := codec.SendMessage(c.conn, reply, c.opts.aesKey, c.opts.hmacKey, c.opts.enableEncryption); err != nil {
		return fmt.Errorf("failed to send MESSAGE_RESPONSE: %v", err)
	}
	return nil
}

func (c *Consumer) sendDenyMessage(ctx context.Context, taskID, queue string, err error) {
	headers := WithHeaders(ctx, map[string]string{
		consts.ConsumerKey: c.id,
		consts.ContentType: consts.TypeJson,
	})
	reply := codec.NewMessage(consts.MESSAGE_DENY, []byte(fmt.Sprintf(`{"id":"%s", "error":"%s"}`, taskID, err.Error())), queue, headers)
	if sendErr := c.send(c.conn, reply); sendErr != nil {
		log.Printf("failed to send MESSAGE_DENY for task %s: %v", taskID, sendErr)
	}
}

// AttemptConnect tries to establish a connection to the server, with TLS or without, based on the configuration.
func (c *Consumer) AttemptConnect() error {
	var err error
	delay := c.opts.initialDelay
	for i := 0; i < c.opts.maxRetries; i++ {
		conn, err := GetConnection(c.opts.brokerAddr, c.opts.tlsConfig)
		if err == nil {
			c.conn = conn
			return nil
		}
		sleepDuration := utils.CalculateJitter(delay, c.opts.jitterPercent)
		log.Printf("CONSUMER - SUBSCRIBE ~> Failed connecting to %s (attempt %d/%d): %v, Retrying in %v...\n", c.opts.brokerAddr, i+1, c.opts.maxRetries, err, sleepDuration)
		time.Sleep(sleepDuration)
		delay *= 2
		if delay > c.opts.maxBackoff {
			delay = c.opts.maxBackoff
		}
	}

	return fmt.Errorf("could not connect to server %s after %d attempts: %w", c.opts.brokerAddr, c.opts.maxRetries, err)
}

func (c *Consumer) readMessage(ctx context.Context, conn net.Conn) error {
	msg, err := c.receive(conn)
	if err == nil {
		ctx = SetHeaders(ctx, msg.Headers)
		c.OnMessage(ctx, msg, conn)
		return nil
	}
	if err.Error() == "EOF" || strings.Contains(err.Error(), "closed network connection") {
		c.OnClose(ctx, conn)
		return err
	}
	c.OnError(ctx, conn, err)
	return err
}

// Consume starts the consumer to consume tasks from the queues.
func (c *Consumer) Consume(ctx context.Context) error {
	err := c.AttemptConnect()
	if err != nil {
		return err
	}

	if err := c.subscribe(ctx, c.queue); err != nil {
		return fmt.Errorf("failed to connect to server for queue %s: %v", c.queue, err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := c.readMessage(ctx, c.conn); err != nil {
				log.Println("Error reading message:", err)
				break
			}
		}
	}()

	wg.Wait()
	return nil
}

func (c *Consumer) waitForAck(conn net.Conn) error {
	msg, err := c.receive(conn)
	if err != nil {
		return err
	}
	if msg.Command == consts.SUBSCRIBE_ACK {
		log.Printf("CONSUMER - SUBSCRIBE_ACK ~> %s on %s", c.id, msg.Queue)
		return nil
	}
	return fmt.Errorf("expected SUBSCRIBE_ACK, got: %v", msg.Command)
}

// Additional methods for Pause, Resume, and Stop

func (c *Consumer) Pause() error {
	if err := c.sendPauseMessage(); err != nil {
		return err
	}
	c.pool.Pause()
	return nil
}

func (c *Consumer) sendPauseMessage() error {
	headers := WithHeaders(context.Background(), map[string]string{
		consts.ConsumerKey: c.id,
	})
	msg := codec.NewMessage(consts.CONSUMER_PAUSE, nil, c.queue, headers)
	return c.send(c.conn, msg)
}

func (c *Consumer) Resume() error {
	if err := c.sendResumeMessage(); err != nil {
		return err
	}
	c.pool.Resume()
	return nil
}

func (c *Consumer) sendResumeMessage() error {
	headers := WithHeaders(context.Background(), map[string]string{
		consts.ConsumerKey: c.id,
	})
	msg := codec.NewMessage(consts.CONSUMER_RESUME, nil, c.queue, headers)
	return c.send(c.conn, msg)
}

func (c *Consumer) Stop() error {
	if err := c.sendStopMessage(); err != nil {
		return err
	}
	c.pool.Stop()
	return nil
}

func (c *Consumer) sendStopMessage() error {
	headers := WithHeaders(context.Background(), map[string]string{
		consts.ConsumerKey: c.id,
	})
	msg := codec.NewMessage(consts.CONSUMER_STOP, nil, c.queue, headers)
	return c.send(c.conn, msg)
}
