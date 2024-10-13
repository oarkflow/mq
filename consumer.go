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

type Processor interface {
	ProcessTask(ctx context.Context, msg *Task) Result
	Consume(ctx context.Context) error
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	Stop(ctx context.Context) error
	GetKey() string
	Close() error
}

type Consumer struct {
	conn    net.Conn
	handler Handler
	pool    *Pool
	id      string
	queue   string
	opts    Options
}

func NewConsumer(id string, queue string, handler Handler, opts ...Option) *Consumer {
	options := setupOptions(opts...)
	return &Consumer{
		id:      id,
		opts:    options,
		queue:   queue,
		handler: handler,
	}
}

func (c *Consumer) send(conn net.Conn, msg *codec.Message) error {
	return codec.SendMessage(conn, msg, c.opts.aesKey, c.opts.hmacKey, c.opts.enableEncryption)
}

func (c *Consumer) receive(conn net.Conn) (*codec.Message, error) {
	return codec.ReadMessage(conn, c.opts.aesKey, c.opts.hmacKey, c.opts.enableEncryption)
}

func (c *Consumer) Close() error {
	c.pool.Stop()
	return c.conn.Close()
}

func (c *Consumer) GetKey() string {
	return c.id
}

func (c *Consumer) subscribe(ctx context.Context, queue string) error {
	headers := HeadersWithConsumerID(ctx, c.id)
	msg := codec.NewMessage(consts.SUBSCRIBE, utils.ToByte("{}"), queue, headers)
	if err := c.send(c.conn, msg); err != nil {
		return err
	}
	return c.waitForAck(c.conn)
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
	if err := c.pool.AddTask(ctx, &task); err != nil {
		c.sendDenyMessage(ctx, task.ID, msg.Queue, err)
		return
	}
}

func (c *Consumer) sendMessageAck(ctx context.Context, msg *codec.Message, conn net.Conn) {
	headers := HeadersWithConsumerIDAndQueue(ctx, c.id, msg.Queue)
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	reply := codec.NewMessage(consts.MESSAGE_ACK, utils.ToByte(fmt.Sprintf(`{"id":"%s"}`, taskID)), msg.Queue, headers)
	if err := c.send(conn, reply); err != nil {
		fmt.Printf("failed to send MESSAGE_ACK for queue %s: %v", msg.Queue, err)
	}
}

func (c *Consumer) ProcessTask(ctx context.Context, msg *Task) Result {
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
	if err := c.send(c.conn, reply); err != nil {
		return fmt.Errorf("failed to send MESSAGE_RESPONSE: %v", err)
	}
	return nil
}

func (c *Consumer) sendDenyMessage(ctx context.Context, taskID, queue string, err error) {
	headers := HeadersWithConsumerID(ctx, c.id)
	reply := codec.NewMessage(consts.MESSAGE_DENY, utils.ToByte(fmt.Sprintf(`{"id":"%s", "error":"%s"}`, taskID, err.Error())), queue, headers)
	if sendErr := c.send(c.conn, reply); sendErr != nil {
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
	msg, err := c.receive(conn)
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
	c.pool = NewPool(c.opts.numOfWorkers, c.opts.queueSize, c.opts.maxMemoryLoad, c.ProcessTask, c.OnResponse, c.conn)
	if err := c.subscribe(ctx, c.queue); err != nil {
		return fmt.Errorf("failed to connect to server for queue %s: %v", c.queue, err)
	}
	c.pool.Start(c.opts.numOfWorkers)
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
	return c.send(c.conn, msg)
}
