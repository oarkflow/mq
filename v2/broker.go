package v2

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/oarkflow/xsync"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
)

type consumer struct {
	id   string
	conn net.Conn
}

type publisher struct {
	id   string
	conn net.Conn
}

type Broker struct {
	queues     xsync.IMap[string, *Queue]
	consumers  xsync.IMap[string, *consumer]
	publishers xsync.IMap[string, *publisher]
	opts       Options
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
		b.PublishHandler(ctx, conn, msg)
	case consts.SUBSCRIBE:
		b.SubscribeHandler(ctx, conn, msg)
	case consts.MESSAGE_RESPONSE:
		b.MessageResponseHandler(ctx, msg)
	case consts.MESSAGE_ACK:
		b.MessageAck(ctx, msg)
	}
}

func (b *Broker) MessageAck(ctx context.Context, msg *codec.Message) {
	consumerID, _ := GetConsumerID(ctx)
	log.Printf("BROKER - MESSAGE_ACK ~> %s on %s", consumerID, msg.Queue)
}

func (b *Broker) MessageResponseHandler(ctx context.Context, msg *codec.Message) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return
	}
	awaitResponse, ok := headers[consts.AwaitResponseKey]
	if !(ok && awaitResponse == "true") {
		return
	}
	publisherID, exists := headers[consts.PublisherKey]
	if !exists {
		return
	}
	con, ok := b.publishers.Get(publisherID)
	if !ok {
		return
	}
	msg.Command = consts.RESPONSE
	err := b.send(con.conn, msg)
	if err != nil {
		panic(err)
	}
}

func (b *Broker) PublishHandler(ctx context.Context, conn net.Conn, msg *codec.Message) {
	pub := b.addPublisher(ctx, msg.Queue, conn)
	log.Printf("BROKER - PUBLISH ~> from %s on %s", pub.id, msg.Queue)
	ack := codec.NewMessage(consts.PUBLISH_ACK, nil, msg.Queue, msg.Headers)
	if err := b.send(conn, ack); err != nil {
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

func (b *Broker) SubscribeHandler(ctx context.Context, conn net.Conn, msg *codec.Message) {
	consumerID := b.addConsumer(ctx, msg.Queue, conn)
	ack := codec.NewMessage(consts.SUBSCRIBE_ACK, nil, msg.Queue, msg.Headers)
	if err := b.send(conn, ack); err != nil {
		log.Printf("Error sending SUBSCRIBE_ACK: %v\n", err)
	}
	go func() {
		select {
		case <-ctx.Done():
			b.removeConsumer(msg.Queue, consumerID)
		}
	}()
}

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
		log.Println("BROKER - RUNNING_TLS ~> started on", b.opts.brokerAddr)
	} else {
		listener, err = net.Listen("tcp", b.opts.brokerAddr)
		if err != nil {
			return fmt.Errorf("failed to start TCP listener: %v", err)
		}
		log.Println("BROKER - RUNNING ~> started on", b.opts.brokerAddr)
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

func (b *Broker) send(conn net.Conn, msg *codec.Message) error {
	return codec.SendMessage(conn, msg, b.opts.aesKey, b.opts.hmacKey, b.opts.enableEncryption)
}

func (b *Broker) receive(c net.Conn) (*codec.Message, error) {
	return codec.ReadMessage(c, b.opts.aesKey, b.opts.hmacKey, b.opts.enableEncryption)
}

func (b *Broker) broadcastToConsumers(ctx context.Context, msg *codec.Message) {
	if queue, ok := b.queues.Get(msg.Queue); ok {
		queue.consumers.ForEach(func(_ string, con *consumer) bool {
			msg.Command = consts.MESSAGE_SEND
			if err := b.send(con.conn, msg); err != nil {
				log.Printf("Error sending Message: %v\n", err)
			}
			return true
		})
	}
}

func (b *Broker) waitForConsumerAck(conn net.Conn) error {
	msg, err := b.receive(conn)
	if err != nil {
		return err
	}
	if msg.Command == consts.MESSAGE_ACK {
		log.Println("Received CONSUMER_ACK: Subscribed successfully")
		return nil
	}
	return fmt.Errorf("expected CONSUMER_ACK, got: %v", msg.Command)
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

func (b *Broker) addConsumer(ctx context.Context, queueName string, conn net.Conn) string {
	consumerID, ok := GetConsumerID(ctx)
	q, ok := b.queues.Get(queueName)
	if !ok {
		q = b.NewQueue(queueName)
	}
	con := &consumer{id: consumerID, conn: conn}
	b.consumers.Set(consumerID, con)
	q.consumers.Set(consumerID, con)
	log.Printf("BROKER - SUBSCRIBE ~> %s on %s", consumerID, queueName)
	return consumerID
}

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

func (b *Broker) readMessage(ctx context.Context, c net.Conn) error {
	msg, err := b.receive(c)
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
