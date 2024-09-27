package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/oarkflow/xsync"
)

type Handler func(context.Context, Task) Result

type Broker struct {
	queues       *xsync.MapOf[string, *Queue]
	taskCallback func(context.Context, *Task) error
}

type Queue struct {
	name     string
	conn     map[net.Conn]struct{}
	messages *xsync.MapOf[string, *Task]
	deferred *xsync.MapOf[string, *Task]
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

type Command struct {
	ID        string          `json:"id"`
	Command   CMD             `json:"command"`
	Queue     string          `json:"queue"`
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload,omitempty"` // Used for carrying the task payload
	Error     string          `json:"error,omitempty"`
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

func (b *Broker) Send(ctx context.Context, cmd Command) error {
	queue, ok := b.queues.Get(cmd.Queue)
	if !ok || queue == nil {
		return errors.New("invalid queue or not exists")
	}
	for client := range queue.conn {
		err := Write(ctx, client, cmd)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) Start(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer func() {
		_ = listener.Close()
	}()
	fmt.Println("Broker server started on", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go ReadFromConn(ctx, conn, b.readMessage)
	}
}

func (b *Broker) Publish(ctx context.Context, message Task, queueName string) error {
	queue, err := b.AddMessageToQueue(&message, queueName)
	if err != nil {
		return err
	}
	if len(queue.conn) == 0 {
		queue.deferred.Set(NewID(), &message)
		fmt.Println("task deferred as no conn are connected", queueName)
		return nil
	}
	for client := range queue.conn {
		err = Write(ctx, client, message)
		if err != nil {
			return err
		}
	}
	return nil
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

func (b *Broker) AddMessageToQueue(message *Task, queueName string) (*Queue, error) {
	queue, ok := b.queues.Get(queueName)
	if !ok {
		return nil, fmt.Errorf("queue %s not found", queueName)
	}
	if message.ID == "" {
		message.ID = NewID()
	}
	if queueName != "" {
		message.CurrentQueue = queueName
	}
	message.CreatedAt = time.Now()
	queue.messages.Set(message.ID, message)
	return queue, nil
}

func (b *Broker) HandleProcessedMessage(ctx context.Context, clientMsg Result) error {
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
	go func() {
		select {
		case <-ctx.Done():
			b.removeConnection(queueName, conn)
		}
	}()
}

// Removes connection from the queue and broker
func (b *Broker) removeConnection(queueName string, conn net.Conn) {
	if queue, ok := b.queues.Get(queueName); ok {
		delete(queue.conn, conn)
		if len(queue.conn) == 0 {
			b.queues.Del(queueName)
		}
		conn.Close()
	}
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

func (b *Broker) handleTaskMessage(ctx context.Context, _ net.Conn, msg Result) error {
	return b.HandleProcessedMessage(ctx, msg)
}

func (b *Broker) publish(ctx context.Context, conn net.Conn, msg Command) error {
	task := Task{
		ID:           msg.MessageID,
		Payload:      msg.Payload,
		CreatedAt:    time.Now(),
		CurrentQueue: msg.Queue,
	}
	err := b.Publish(ctx, task, msg.Queue)
	if err != nil {
		return err
	}
	if task.ID != "" {
		result := Result{
			Command:   "PUBLISH",
			MessageID: task.ID,
			Status:    "success",
			Queue:     msg.Queue,
		}
		return Write(ctx, conn, result)
	}
	return nil
}

func (b *Broker) request(ctx context.Context, conn net.Conn, msg Command) error {
	task := Task{
		ID:           msg.MessageID,
		Payload:      msg.Payload,
		CreatedAt:    time.Now(),
		CurrentQueue: msg.Queue,
	}
	err := b.Publish(ctx, task, msg.Queue)
	if err != nil {
		return err
	}
	if task.ID != "" {
		result := Result{
			Command:   "REQUEST",
			MessageID: task.ID,
			Status:    "success",
			Queue:     msg.Queue,
		}
		return Write(ctx, conn, result)
	}
	return nil
}

func (b *Broker) handleCommandMessage(ctx context.Context, conn net.Conn, msg Command) error {
	switch msg.Command {
	case SUBSCRIBE:
		b.subscribe(ctx, msg.Queue, conn)
		return nil
	case PUBLISH:
		return b.publish(ctx, conn, msg)
	case REQUEST:
		return b.request(ctx, conn, msg)
	default:
		return fmt.Errorf("unknown command: %d", msg.Command)
	}
	return nil
}
