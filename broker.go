package mq

import (
	"context"
	"crypto/tls"
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
	b := &Broker{
		queues:     xsync.NewMap[string, *Queue](),
		publishers: xsync.NewMap[string, *publisher](),
		consumers:  xsync.NewMap[string, *consumer](),
	}
	b.opts = defaultHandlers(options, b.onMessage, b.onClose, b.onError)
	return b
}

func (b *Broker) Send(ctx context.Context, cmd Command) error {
	queue, ok := b.queues.Get(cmd.Queue)
	if !ok || queue == nil {
		return errors.New("invalid queue or not exists")
	}
	queue.send(ctx, cmd)
	return nil
}

func (b *Broker) TLSConfig() TLSConfig {
	return b.opts.tlsConfig
}

func (b *Broker) SyncMode() bool {
	return b.opts.syncMode
}

func (b *Broker) sendToPublisher(ctx context.Context, publisherID string, result Result) error {
	pub, ok := b.publishers.Get(publisherID)
	if !ok {
		return nil
	}
	return pub.send(ctx, result)
}

func (b *Broker) onClose(ctx context.Context, _ net.Conn) error {
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

func (b *Broker) onError(_ context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from connection:", err, conn.RemoteAddr())
}

// Start the broker server with optional TLS support
func (b *Broker) Start(ctx context.Context) error {
	var listener net.Listener
	var err error

	if b.opts.tlsConfig.UseTLS {
		cert, err := tls.LoadX509KeyPair(b.opts.tlsConfig.CertPath, b.opts.tlsConfig.KeyPath)
		if err != nil {
			return fmt.Errorf("failed to load TLS certificates: %v", err)
		}
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		listener, err = tls.Listen("tcp", b.opts.brokerAddr, tlsConfig)
		if err != nil {
			return fmt.Errorf("failed to start TLS listener: %v", err)
		}
		log.Println("TLS server started on", b.opts.brokerAddr)
	} else {
		listener, err = net.Listen("tcp", b.opts.brokerAddr)
		if err != nil {
			return fmt.Errorf("failed to start TCP listener: %v", err)
		}
		log.Println("TCP server started on", b.opts.brokerAddr)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go ReadFromConn(ctx, conn, b.opts.messageHandler, b.opts.closeHandler, b.opts.errorHandler)
	}
}

func (b *Broker) Publish(ctx context.Context, message Task, queueName string) Result {
	queue, task, err := b.AddMessageToQueue(&message, queueName)
	if err != nil {
		return Result{Error: err}
	}
	result := Result{
		Command:   "PUBLISH",
		Payload:   message.Payload,
		Queue:     queueName,
		MessageID: task.ID,
	}
	if queue.consumers.Size() == 0 {
		queue.deferred.Set(NewID(), &message)
		fmt.Println("task deferred as no consumers are connected", queueName)
		return result
	}
	queue.send(ctx, message)
	return result
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

func (b *Broker) AddMessageToQueue(task *Task, queueName string) (*Queue, *Task, error) {
	queue := b.NewQueue(queueName)
	if task.ID == "" {
		task.ID = NewID()
	}
	if queueName != "" {
		task.CurrentQueue = queueName
	}
	task.CreatedAt = time.Now()
	queue.messages.Set(task.ID, task)
	return queue, task, nil
}

func (b *Broker) HandleProcessedMessage(ctx context.Context, result Result) error {
	publisherID, ok := GetPublisherID(ctx)
	if ok && publisherID != "" {
		err := b.sendToPublisher(ctx, publisherID, result)
		if err != nil {
			return err
		}
	}
	for _, callback := range b.opts.callback {
		if callback != nil {
			rs := callback(ctx, result)
			if rs.Error != nil {
				return rs.Error
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

func (b *Broker) onMessage(ctx context.Context, conn net.Conn, message []byte) error {
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
	status := "PUBLISH"
	if msg.Command == REQUEST {
		status = "REQUEST"
	}
	b.addPublisher(ctx, msg.Queue, conn)
	task := Task{
		ID:           msg.MessageID,
		Payload:      msg.Payload,
		CreatedAt:    time.Now(),
		CurrentQueue: msg.Queue,
	}
	result := b.Publish(ctx, task, msg.Queue)
	if result.Error != nil {
		return result.Error
	}
	if task.ID != "" {
		result.Status = status
		result.MessageID = task.ID
		result.Queue = msg.Queue
		return Write(ctx, conn, result)
	}
	return nil
}

func (b *Broker) handleCommandMessage(ctx context.Context, conn net.Conn, msg Command) error {
	switch msg.Command {
	case SUBSCRIBE:
		b.subscribe(ctx, msg.Queue, conn)
		return nil
	case PUBLISH, REQUEST:
		return b.publish(ctx, conn, msg)
	default:
		return fmt.Errorf("unknown command: %d", msg.Command)
	}
}
