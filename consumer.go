package mq

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/json/jsonparser"
	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/utils"
)

type Processor interface {
	ProcessTask(ctx context.Context, msg *Task) Result
	Consume(ctx context.Context) error
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	Stop(ctx context.Context) error
	Close() error
	GetKey() string
	SetKey(key string)
	GetType() string
}

type Consumer struct {
	conn    net.Conn
	handler Handler
	pool    *Pool
	opts    *Options
	id      string
	queue   string
}

func NewConsumer(id string, queue string, handler Handler, opts ...Option) *Consumer {
	options := SetupOptions(opts...)
	return &Consumer{
		id:      id,
		opts:    options,
		queue:   queue,
		handler: handler,
	}
}

func (c *Consumer) send(ctx context.Context, conn net.Conn, msg *codec.Message) error {
	return codec.SendMessage(ctx, conn, msg)
}

func (c *Consumer) receive(ctx context.Context, conn net.Conn) (*codec.Message, error) {
	return codec.ReadMessage(ctx, conn)
}

func (c *Consumer) Close() error {
	c.pool.Stop()
	return c.conn.Close()
}

func (c *Consumer) GetKey() string {
	return c.id
}

func (c *Consumer) GetType() string {
	return "consumer"
}

func (c *Consumer) SetKey(key string) {
	c.id = key
}

func (c *Consumer) Metrics() Metrics {
	return c.pool.Metrics()
}

func (c *Consumer) subscribe(ctx context.Context, queue string) error {
	headers := HeadersWithConsumerID(ctx, c.id)
	msg := codec.NewMessage(consts.SUBSCRIBE, utils.ToByte("{}"), queue, headers)
	if err := c.send(ctx, c.conn, msg); err != nil {
		return fmt.Errorf("error while trying to subscribe: %v", err)
	}
	return c.waitForAck(ctx, c.conn)
}

func (c *Consumer) OnClose(_ context.Context, _ net.Conn) error {
	fmt.Println("Consumer closed")
	return nil
}

func (c *Consumer) OnError(_ context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from connection:", err, conn.RemoteAddr())
}

func (c *Consumer) OnMessage(ctx context.Context, msg *codec.Message, conn net.Conn) error {
	switch msg.Command {
	case consts.PUBLISH:
		c.ConsumeMessage(ctx, msg, conn)
	case consts.CONSUMER_PAUSE:
		err := c.Pause(ctx)
		if err != nil {
			log.Printf("Unable to pause consumer: %v", err)
		}
		return err
	case consts.CONSUMER_RESUME:
		err := c.Resume(ctx)
		if err != nil {
			log.Printf("Unable to resume consumer: %v", err)
		}
		return err
	case consts.CONSUMER_STOP:
		err := c.Stop(ctx)
		if err != nil {
			log.Printf("Unable to stop consumer: %v", err)
		}
		return err
	default:
		log.Printf("CONSUMER - UNKNOWN_COMMAND ~> %s on %s", msg.Command, msg.Queue)
	}
	return nil
}

func (c *Consumer) sendMessageAck(ctx context.Context, msg *codec.Message, conn net.Conn) {
	headers := HeadersWithConsumerIDAndQueue(ctx, c.id, msg.Queue)
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	reply := codec.NewMessage(consts.MESSAGE_ACK, utils.ToByte(fmt.Sprintf(`{"id":"%s"}`, taskID)), msg.Queue, headers)
	if err := c.send(ctx, conn, reply); err != nil {
		fmt.Printf("failed to send MESSAGE_ACK for queue %s: %v", msg.Queue, err)
	}
}

func (c *Consumer) ConsumeMessage(ctx context.Context, msg *codec.Message, conn net.Conn) {
	c.sendMessageAck(ctx, msg, conn)
	if msg.Payload == nil {
		log.Printf("Received empty message payload")
		return
	}
	var task Task
	err := json.Unmarshal(msg.Payload, &task)
	if err != nil {
		log.Printf("Error unmarshalling message: %v", err)
		return
	}
	ctx = SetHeaders(ctx, map[string]string{consts.QueueKey: msg.Queue})
	if err := c.pool.EnqueueTask(ctx, &task, 1); err != nil {
		c.sendDenyMessage(ctx, task.ID, msg.Queue, err)
		return
	}
}

func (c *Consumer) ProcessTask(ctx context.Context, msg *Task) Result {
	defer RecoverPanic(RecoverTitle)
	queue, _ := GetQueue(ctx)
	if msg.Topic == "" && queue != "" {
		msg.Topic = queue
	}
	result := c.handler(ctx, msg)
	result.Topic = msg.Topic
	result.TaskID = msg.ID
	return result
}

func (c *Consumer) OnResponse(ctx context.Context, result Result) error {
	if result.Status == "PENDING" && c.opts.respondPendingResult {
		return nil
	}
	headers := HeadersWithConsumerIDAndQueue(ctx, c.id, result.Topic)
	if result.Status == "" {
		if result.Error != nil {
			result.Status = "FAILED"
		} else {
			result.Status = "SUCCESS"
		}
	}
	bt, _ := json.Marshal(result)
	reply := codec.NewMessage(consts.MESSAGE_RESPONSE, bt, result.Topic, headers)
	if err := c.send(ctx, c.conn, reply); err != nil {
		return fmt.Errorf("failed to send MESSAGE_RESPONSE: %v", err)
	}
	return nil
}

func (c *Consumer) sendDenyMessage(ctx context.Context, taskID, queue string, err error) {
	headers := HeadersWithConsumerID(ctx, c.id)
	reply := codec.NewMessage(consts.MESSAGE_DENY, utils.ToByte(fmt.Sprintf(`{"id":"%s", "error":"%s"}`, taskID, err.Error())), queue, headers)
	if sendErr := c.send(ctx, c.conn, reply); sendErr != nil {
		log.Printf("failed to send MESSAGE_DENY for task %s: %v", taskID, sendErr)
	}
}

func (c *Consumer) attemptConnect() error {
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
	msg, err := c.receive(ctx, conn)
	if err == nil {
		ctx = SetHeaders(ctx, msg.Headers)
		return c.OnMessage(ctx, msg, conn)
	}
	if err.Error() == "EOF" || strings.Contains(err.Error(), "closed network connection") {
		err1 := c.OnClose(ctx, conn)
		if err1 != nil {
			return err1
		}
		return err
	}
	c.OnError(ctx, conn, err)
	return err
}

func (c *Consumer) Consume(ctx context.Context) error {
	err := c.attemptConnect()
	if err != nil {
		return err
	}
	c.pool = NewPool(
		c.opts.numOfWorkers,
		WithTaskQueueSize(c.opts.queueSize),
		WithMaxMemoryLoad(c.opts.maxMemoryLoad),
		WithHandler(c.ProcessTask),
		WithPoolCallback(c.OnResponse),
		WithTaskStorage(c.opts.storage),
	)
	if err := c.subscribe(ctx, c.queue); err != nil {
		return fmt.Errorf("failed to connect to server for queue %s: %v", c.queue, err)
	}
	c.pool.Start(c.opts.numOfWorkers)
	stopChan := make(chan struct{})
	go func() {
		defer close(stopChan) // Signal completion when done
		for {
			select {
			case <-ctx.Done():
				log.Println("Context canceled, stopping message reading.")
				return
			default:
				if err := c.readMessage(ctx, c.conn); err != nil {
					log.Println("Error reading message:", err)
					return // Exit the goroutine on error
				}
			}
		}
	}()
	select {
	case <-stopChan:
	case <-ctx.Done():
		log.Println("Context canceled, performing cleanup.")
	}
	return nil
}

func (c *Consumer) waitForAck(ctx context.Context, conn net.Conn) error {
	msg, err := c.receive(ctx, conn)
	if err != nil {
		return err
	}
	if msg.Command == consts.SUBSCRIBE_ACK {
		log.Printf("CONSUMER - SUBSCRIBE_ACK ~> %s on %s", c.id, msg.Queue)
		return nil
	}
	return fmt.Errorf("expected SUBSCRIBE_ACK, got: %v", msg.Command)
}

func (c *Consumer) Pause(ctx context.Context) error {
	return c.operate(ctx, consts.CONSUMER_PAUSED, c.pool.Pause)
}

func (c *Consumer) Resume(ctx context.Context) error {
	return c.operate(ctx, consts.CONSUMER_RESUMED, c.pool.Resume)
}

func (c *Consumer) Stop(ctx context.Context) error {
	return c.operate(ctx, consts.CONSUMER_STOPPED, c.pool.Stop)
}

func (c *Consumer) operate(ctx context.Context, cmd consts.CMD, poolOperation func()) error {
	if err := c.sendOpsMessage(ctx, cmd); err != nil {
		return err
	}
	poolOperation()
	return nil
}

func (c *Consumer) sendOpsMessage(ctx context.Context, cmd consts.CMD) error {
	headers := HeadersWithConsumerID(ctx, c.id)
	msg := codec.NewMessage(cmd, nil, c.queue, headers)
	return c.send(ctx, c.conn, msg)
}

func (c *Consumer) Conn() net.Conn {
	return c.conn
}
