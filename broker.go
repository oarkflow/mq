package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/oarkflow/xsync"
)

type consumer struct {
	id   string
	conn net.Conn
}

func (p *consumer) send(ctx context.Context, cmd any) error {
	return Write(ctx, p.conn, cmd)
}

type publisher struct {
	id   string
	conn net.Conn
}

func (p *publisher) send(ctx context.Context, cmd any) error {
	return Write(ctx, p.conn, cmd)
}

type Handler func(context.Context, Task) Result

type Broker struct {
	queues     xsync.IMap[string, *Queue]
	consumers  xsync.IMap[string, *consumer]
	publishers xsync.IMap[string, *publisher]
	opts       Options
}

type Queue struct {
	name      string
	consumers xsync.IMap[string, *consumer]
	messages  xsync.IMap[string, *Task]
	deferred  xsync.IMap[string, *Task]
}

func newQueue(name string) *Queue {
	return &Queue{
		name:      name,
		consumers: xsync.NewMap[string, *consumer](),
		messages:  xsync.NewMap[string, *Task](),
		deferred:  xsync.NewMap[string, *Task](),
	}
}

func (queue *Queue) send(ctx context.Context, cmd any) {
	queue.consumers.ForEach(func(_ string, client *consumer) bool {
		err := client.send(ctx, cmd)
		if err != nil {
			return false
		}
		return true
	})
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

func NewBroker(opts ...Option) *Broker {
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}
	broker := &Broker{
		queues:     xsync.NewMap[string, *Queue](),
		publishers: xsync.NewMap[string, *publisher](),
		consumers:  xsync.NewMap[string, *consumer](),
	}

	if options.messageHandler == nil {
		options.messageHandler = broker.readMessage
	}

	if options.closeHandler == nil {
		options.closeHandler = broker.onClose
	}

	if options.errorHandler == nil {
		options.errorHandler = broker.onError
	}
	broker.opts = options
	return broker
}

func (b *Broker) Send(ctx context.Context, cmd Command) error {
	queue, ok := b.queues.Get(cmd.Queue)
	if !ok || queue == nil {
		return errors.New("invalid queue or not exists")
	}
	queue.send(ctx, cmd)
	return nil
}

func (b *Broker) sendToPublisher(ctx context.Context, publisherID string, result Result) error {
	pub, ok := b.publishers.Get(publisherID)
	if !ok {
		return nil
	}
	return pub.send(ctx, result)
}

func (b *Broker) onClose(ctx context.Context, conn net.Conn) error {
	consumerID, ok := GetConsumerID(ctx)
	if ok && consumerID != "" {
		if con, exists := b.consumers.Get(consumerID); exists {
			con.conn.Close()
			b.consumers.Del(consumerID)
		}
		b.queues.ForEach(func(_ string, queue *Queue) bool {
			queue.consumers.Del(consumerID)
			return true
		})
	}
	publisherID, ok := GetPublisherID(ctx)
	if ok && publisherID != "" {
		if con, exists := b.publishers.Get(publisherID); exists {
			con.conn.Close()
			b.publishers.Del(publisherID)
		}
	}
	return nil
}

func (b *Broker) onError(ctx context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from connection:", err, conn.RemoteAddr())
}

func (b *Broker) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", b.opts.brokerAddr)
	if err != nil {
		return err
	}
	defer func() {
		_ = listener.Close()
	}()
	log.Println("Server started on", b.opts.brokerAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go ReadFromConn(ctx, conn, b.opts.messageHandler, b.opts.closeHandler, b.opts.errorHandler)
	}
}

func (b *Broker) Publish(ctx context.Context, message Task, queueName string) (*Task, error) {
	queue, task, err := b.AddMessageToQueue(&message, queueName)
	if err != nil {
		return nil, err
	}
	if queue.consumers.Size() == 0 {
		queue.deferred.Set(NewID(), &message)
		fmt.Println("task deferred as no consumers are connected", queueName)
		return task, nil
	}
	queue.send(ctx, message)
	return task, nil
}

func (b *Broker) NewQueue(qName string) *Queue {
	q, ok := b.queues.Get(qName)
	if ok {
		return q
	}
	q = newQueue(qName)
	b.queues.Set(qName, q)
	return q
}

func (b *Broker) AddMessageToQueue(message *Task, queueName string) (*Queue, *Task, error) {
	queue, ok := b.queues.Get(queueName)
	if !ok {
		return nil, nil, fmt.Errorf("queue %s not found", queueName)
	}
	if message.ID == "" {
		message.ID = NewID()
	}
	if queueName != "" {
		message.CurrentQueue = queueName
	}
	message.CreatedAt = time.Now()
	queue.messages.Set(message.ID, message)
	return queue, message, nil
}

func (b *Broker) HandleProcessedMessage(ctx context.Context, clientMsg Result) error {
	publisherID, ok := GetPublisherID(ctx)
	if ok && publisherID != "" {
		err := b.sendToPublisher(ctx, publisherID, clientMsg)
		if err != nil {
			return err
		}
	}
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
			for _, callback := range b.opts.callback {
				if callback != nil {
					err := callback(ctx, msg)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (b *Broker) addConsumer(ctx context.Context, queueName string, conn net.Conn) string {
	consumerID, ok := GetConsumerID(ctx)
	defer func() {
		cmd := Command{
			Command: SUBSCRIBE_ACK,
			Queue:   queueName,
			Error:   "",
		}
		Write(ctx, conn, cmd)
		log.Printf("Consumer %s joined server on queue %s", consumerID, queueName)
	}()
	q, ok := b.queues.Get(queueName)
	if !ok {
		q = b.NewQueue(queueName)
	}
	con := &consumer{id: consumerID, conn: conn}
	b.consumers.Set(consumerID, con)
	q.consumers.Set(consumerID, con)
	return consumerID
}

func (b *Broker) addPublisher(ctx context.Context, queueName string, conn net.Conn) string {
	publisherID, ok := GetPublisherID(ctx)
	_, ok = b.queues.Get(queueName)
	if !ok {
		b.NewQueue(queueName)
	}
	con := &publisher{id: publisherID, conn: conn}
	b.publishers.Set(publisherID, con)
	return publisherID
}

func (b *Broker) subscribe(ctx context.Context, queueName string, conn net.Conn) {
	consumerID := b.addConsumer(ctx, queueName, conn)
	go func() {
		select {
		case <-ctx.Done():
			b.removeConsumer(queueName, consumerID)
		}
	}()
}

// Removes connection from the queue and broker
func (b *Broker) removeConsumer(queueName, consumerID string) {
	if queue, ok := b.queues.Get(queueName); ok {
		con, ok := queue.consumers.Get(consumerID)
		if ok {
			con.conn.Close()
			queue.consumers.Del(consumerID)
		}
		b.queues.Del(queueName)
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
	b.addPublisher(ctx, msg.Queue, conn)
	task := Task{
		ID:           msg.MessageID,
		Payload:      msg.Payload,
		CreatedAt:    time.Now(),
		CurrentQueue: msg.Queue,
	}
	_, err := b.Publish(ctx, task, msg.Queue)
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
	b.addPublisher(ctx, msg.Queue, conn)
	task := Task{
		ID:           msg.MessageID,
		Payload:      msg.Payload,
		CreatedAt:    time.Now(),
		CurrentQueue: msg.Queue,
	}
	_, err := b.Publish(ctx, task, msg.Queue)
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
}
