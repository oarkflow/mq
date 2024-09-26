package broker

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
	"github.com/oarkflow/xsync"

	"github.com/oarkflow/golong/utils"
)

type Handler func(context.Context, Task) Result

type Broker struct {
	queues       *xsync.MapOf[string, *Queue]
	taskCallback func(context.Context, *Task) error
}

type Queue struct {
	name     string
	messages *xsync.MapOf[string, *Task]
	deferred *xsync.MapOf[string, *Task]
	conn     map[net.Conn]struct{}
}

type Task struct {
	ID           string          `json:"id"`
	Payload      json.RawMessage `json:"payload"`
	CreatedAt    time.Time       `json:"created_at"`
	ProcessedAt  time.Time       `json:"processed_at"`
	CurrentQueue string          `json:"current_queue"`
	Result       json.RawMessage `json:"result"`
	Status       string          `json:"status"`
	Error        error           `json:"error"`
}

type CMD int

const (
	SUBSCRIBE CMD = iota + 1
	STOP
)

type Command struct {
	Command   CMD    `json:"command"`
	Queue     string `json:"queue"`
	MessageID string `json:"message_id"`
	Error     string `json:"error,omitempty"`
}

type Result struct {
	Command   string          `json:"command"`
	Payload   json.RawMessage `json:"payload"`
	Queue     string          `json:"queue"`
	MessageID string          `json:"message_id"`
	Error     error           `json:"error"`
	Status    string          `json:"status"`
}

func NewBroker(callback ...func(context.Context, *Task) error) *Broker {
	broker := &Broker{
		queues: xsync.NewMap[string, *Queue](),
	}
	if len(callback) > 0 {
		broker.taskCallback = callback[0]
	}
	return broker
}

func (b *Broker) NewQueue(qName string) {
	if _, ok := b.queues.Get(qName); !ok {
		b.queues.Set(qName, &Queue{
			name:     qName,
			messages: xsync.NewMap[string, *Task](),
			deferred: xsync.NewMap[string, *Task](),
		})
	}
}

func (b *Broker) Send(ctx context.Context, cmd Command) {
	queue, ok := b.queues.Get(cmd.Queue)
	if !ok || queue == nil {
		return
	}
	for client := range queue.conn {
		utils.Write(ctx, client, cmd)
	}
}

func (b *Broker) Publish(ctx context.Context, message Task, queueName string) error {
	queue, ok := b.queues.Get(queueName)
	if !ok {
		return fmt.Errorf("queue %s not found", queueName)
	}
	if queueName != "" {
		message.CurrentQueue = queueName
	}
	message.CreatedAt = time.Now()
	queue.messages.Set(message.ID, &message)
	if len(queue.conn) == 0 {
		queue.deferred.Set(xid.New().String(), &message)
		fmt.Println("task deferred as no conn are connected", queueName)
		return nil
	}
	for client := range queue.conn {
		utils.Write(ctx, client, message)
	}
	return nil
}

func (b *Broker) handleCommandMessage(ctx context.Context, conn net.Conn, msg Command) error {
	switch msg.Command {
	case SUBSCRIBE:
		b.subscribe(ctx, msg.Queue, conn)
	default:
		return fmt.Errorf("unknown command: %d", msg.Command)
	}
	return nil
}

func (b *Broker) handleTaskMessage(ctx context.Context, _ net.Conn, msg Result) error {
	b.handleProcessedMessage(ctx, msg)
	return nil
}

func (b *Broker) readMessage(ctx context.Context, conn net.Conn, message []byte) error {
	var cmdMsg Command
	var resultMsg Result
	err := json.Unmarshal(message, &cmdMsg)
	if err == nil {
		return b.handleCommandMessage(ctx, conn, cmdMsg)
	}
	err = json.Unmarshal(message, &resultMsg)
	if err == nil {
		return b.handleTaskMessage(ctx, conn, resultMsg)
	}
	return nil
}

func (b *Broker) handleProcessedMessage(ctx context.Context, clientMsg Result) error {
	if queue, ok := b.queues.Get(clientMsg.Queue); ok {
		if msg, ok := queue.messages.Get(clientMsg.MessageID); ok {
			msg.ProcessedAt = time.Now()
			msg.Status = clientMsg.Status
			msg.Result = clientMsg.Payload
			msg.Error = clientMsg.Error
			msg.CurrentQueue = clientMsg.Queue
			if clientMsg.Error != nil {
				msg.Status = "error"
			}
			if b.taskCallback != nil {
				return b.taskCallback(ctx, msg)
			}
		}
	}
	return nil
}

func (b *Broker) subscribe(ctx context.Context, queueName string, conn net.Conn) {
	q, ok := b.queues.Get(queueName)
	if !ok {
		q = &Queue{
			conn: make(map[net.Conn]struct{}),
		}
		q.conn[conn] = struct{}{}
		b.queues.Set(queueName, q)
	}
	if q.conn == nil {
		q.conn = make(map[net.Conn]struct{})
	}
	q.conn[conn] = struct{}{}
	if q.deferred == nil {
		q.deferred = xsync.NewMap[string, *Task]()
	}
	q.deferred.ForEach(func(_ string, message *Task) bool {
		b.Publish(ctx, *message, queueName)
		return true
	})
	q.deferred = nil
}

func (b *Broker) Start(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Println("Broker server started on", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go utils.ReadFromConn(ctx, conn, b.readMessage)
	}
}

type Consumer struct {
	serverAddr string
	handlers   map[string]Handler
	queues     []string
	conn       net.Conn
}

func NewConsumer(serverAddr string, queues ...string) *Consumer {
	return &Consumer{
		handlers:   make(map[string]Handler),
		serverAddr: serverAddr,
		queues:     queues,
	}
}

func (c *Consumer) Close() error {
	return c.conn.Close()
}

func (c *Consumer) subscribe(queue string) error {
	ctx := context.Background()
	subscribe := Command{Command: SUBSCRIBE, Queue: queue}
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
		return fmt.Errorf("unknown command in consumer %s", msg.Command)
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
	}()
	c.queues = slices.Compact(append(c.queues, queues...))
	for _, q := range c.queues {
		if err := c.subscribe(q); err != nil {
			return fmt.Errorf("failed to connect to server for queue %s: %v", q, err)
		}
		fmt.Println("Consumer started on", q)
	}
	wg.Wait()
	fmt.Println("Consumer stopped.")
	return nil
}

func (c *Consumer) RegisterHandler(queue string, handler Handler) {
	c.handlers[queue] = handler
}
