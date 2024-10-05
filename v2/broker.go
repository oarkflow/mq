package v2

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/oarkflow/xsync"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
)

type consumer struct {
	id   string
	conn net.Conn
}

func (p *consumer) send(ctx context.Context, cmd any) error {
	return nil
}

type publisher struct {
	id   string
	conn net.Conn
}

func (p *publisher) send(ctx context.Context, cmd any) error {
	return nil
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
	ID          string          `json:"id"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	ProcessedAt time.Time       `json:"processed_at"`
	Status      string          `json:"status"`
	Error       error           `json:"error"`
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
		opts:       options,
	}
	return b
}

func (b *Broker) TLSConfig() TLSConfig {
	return b.opts.tlsConfig
}

func (b *Broker) SyncMode() bool {
	return b.opts.syncMode
}

func (b *Broker) OnClose(ctx context.Context, _ net.Conn) error {
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

func (b *Broker) OnError(_ context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from connection:", err, conn.RemoteAddr())
}

func (b *Broker) OnMessage(ctx context.Context, msg *codec.Message, conn net.Conn) {
	switch msg.Command {
	case consts.PUBLISH:
		b.publish(ctx, conn, msg)
	case consts.SUBSCRIBE:
		b.subscribe(ctx, conn, msg)
	case consts.MESSAGE_RESPONSE:
		headers, ok := GetHeaders(ctx)
		if ok {
			if awaitResponse, ok := headers[consts.AwaitResponseKey]; ok && awaitResponse == "true" {
				publisherID, exists := headers[consts.PublisherKey]
				if exists {
					con, ok := b.publishers.Get(publisherID)
					if ok {
						msg.Command = consts.RESPONSE
						err := codec.SendMessage(con.conn, msg, b.opts.aesKey, b.opts.hmacKey, b.opts.enableEncryption)
						if err != nil {
							panic(err)
						}
					}
				}
			}
		}
		fmt.Println("consumer confirmed", headers, ok, string(msg.Payload))
	case consts.MESSAGE_ACK:
		fmt.Println("consumer confirmed")
	}
}

func (b *Broker) readMessage(ctx context.Context, c net.Conn) error {
	msg, err := codec.ReadMessage(c, b.opts.aesKey, b.opts.hmacKey, b.opts.enableEncryption)
	if err == nil {
		ctx = SetHeaders(ctx, msg.Headers)
		b.OnMessage(ctx, msg, c)
		return nil
	}
	if err.Error() == "EOF" || strings.Contains(err.Error(), "closed network connection") {
		b.OnClose(ctx, c)
		return err
	}
	b.OnError(ctx, c, err)
	return err
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
			b.OnError(ctx, conn, err)
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			for {
				err := b.readMessage(ctx, c)
				if err != nil {
					break
				}
			}
		}(conn)
	}
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
	task.CreatedAt = time.Now()
	queue.messages.Set(task.ID, task)
	return queue, task, nil
}

func (b *Broker) addConsumer(ctx context.Context, queueName string, conn net.Conn) string {
	consumerID, ok := GetConsumerID(ctx)
	q, ok := b.queues.Get(queueName)
	if !ok {
		q = b.NewQueue(queueName)
	}
	con := &consumer{id: consumerID, conn: conn}
	b.consumers.Set(consumerID, con)
	q.consumers.Set(consumerID, con)
	log.Printf("Consumer %s joined server on queue %s", consumerID, queueName)
	return consumerID
}

func (b *Broker) addPublisher(ctx context.Context, queueName string, conn net.Conn) *publisher {
	publisherID, ok := GetPublisherID(ctx)
	_, ok = b.queues.Get(queueName)
	if !ok {
		b.NewQueue(queueName)
	}
	con := &publisher{id: publisherID, conn: conn}
	b.publishers.Set(publisherID, con)
	return con
}

func (b *Broker) broadcastToConsumers(ctx context.Context, msg *codec.Message) {
	if queue, ok := b.queues.Get(msg.Queue); ok {
		queue.consumers.ForEach(func(_ string, con *consumer) bool {
			msg.Command = consts.MESSAGE_SEND
			if err := codec.SendMessage(con.conn, msg, b.opts.aesKey, b.opts.hmacKey, b.opts.enableEncryption); err != nil {
				log.Printf("Error sending Message: %v\n", err)
			}
			return true
		})
	}
}

func (b *Broker) waitForAck(conn net.Conn) error {
	msg, err := codec.ReadMessage(conn, b.opts.aesKey, b.opts.hmacKey, b.opts.enableEncryption)
	if err != nil {
		return err
	}
	if msg.Command == consts.MESSAGE_ACK {
		log.Println("Received CONSUMER_ACK: Subscribed successfully")
		return nil
	}
	return fmt.Errorf("expected CONSUMER_ACK, got: %v", msg.Command)
}

func (b *Broker) publish(ctx context.Context, conn net.Conn, msg *codec.Message) {
	pub := b.addPublisher(ctx, msg.Queue, conn)
	ack := &codec.Message{
		Headers: msg.Headers,
		Queue:   msg.Queue,
		Command: consts.PUBLISH_ACK,
	}
	if err := codec.SendMessage(conn, ack, b.opts.aesKey, b.opts.hmacKey, b.opts.enableEncryption); err != nil {
		log.Printf("Error sending PUBLISH_ACK: %v\n", err)
	}
	b.broadcastToConsumers(ctx, msg)
	go func() {
		select {
		case <-ctx.Done():
			b.publishers.Del(pub.id)
		}
	}()
}

func (b *Broker) subscribe(ctx context.Context, conn net.Conn, msg *codec.Message) {
	consumerID := b.addConsumer(ctx, msg.Queue, conn)
	ack := &codec.Message{
		Headers: msg.Headers,
		Queue:   msg.Queue,
		Command: consts.SUBSCRIBE_ACK,
	}
	if err := codec.SendMessage(conn, ack, b.opts.aesKey, b.opts.hmacKey, b.opts.enableEncryption); err != nil {
		log.Printf("Error sending SUBSCRIBE_ACK: %v\n", err)
	}
	go func() {
		select {
		case <-ctx.Done():
			b.removeConsumer(msg.Queue, consumerID)
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
