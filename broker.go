package mq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/jsonparser"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
	"github.com/oarkflow/mq/utils"
)

type QueuedTask struct {
	Message    *codec.Message
	RetryCount int
}

type consumer struct {
	conn  net.Conn
	id    string
	state consts.ConsumerState
}

type publisher struct {
	conn net.Conn
	id   string
}

type Broker struct {
	queues     storage.IMap[string, *Queue]
	consumers  storage.IMap[string, *consumer]
	publishers storage.IMap[string, *publisher]
	deadLetter storage.IMap[string, *Queue]
	opts       *Options
	listener   net.Listener
}

func NewBroker(opts ...Option) *Broker {
	options := SetupOptions(opts...)
	return &Broker{
		queues:     memory.New[string, *Queue](),
		publishers: memory.New[string, *publisher](),
		consumers:  memory.New[string, *consumer](),
		deadLetter: memory.New[string, *Queue](),
		opts:       options,
	}
}

func (b *Broker) Options() *Options {
	return b.opts
}

func (b *Broker) OnClose(ctx context.Context, conn net.Conn) error {
	consumerID, ok := GetConsumerID(ctx)
	if ok && consumerID != "" {
		if con, exists := b.consumers.Get(consumerID); exists {
			con.conn.Close()
			b.consumers.Del(consumerID)
		}
		b.queues.ForEach(func(_ string, queue *Queue) bool {
			if _, ok := queue.consumers.Get(consumerID); ok {
				if b.opts.consumerOnClose != nil {
					b.opts.consumerOnClose(ctx, queue.name, consumerID)
				}
				queue.consumers.Del(consumerID)
			}
			return true
		})
	} else {
		b.consumers.ForEach(func(consumerID string, con *consumer) bool {
			if utils.ConnectionsEqual(conn, con.conn) {
				con.conn.Close()
				b.consumers.Del(consumerID)
				b.queues.ForEach(func(_ string, queue *Queue) bool {
					queue.consumers.Del(consumerID)
					if _, ok := queue.consumers.Get(consumerID); ok {
						if b.opts.consumerOnClose != nil {
							b.opts.consumerOnClose(ctx, queue.name, consumerID)
						}
					}
					return true
				})
			}
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
	if conn != nil {
		fmt.Println("Error reading from connection:", err, conn.RemoteAddr())
	}
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
	case consts.MESSAGE_DENY:
		b.MessageDeny(ctx, msg)
	case consts.CONSUMER_PAUSED:
		b.OnConsumerPause(ctx, msg)
	case consts.CONSUMER_RESUMED:
		b.OnConsumerResume(ctx, msg)
	case consts.CONSUMER_STOPPED:
		b.OnConsumerStop(ctx, msg)
	default:
		log.Printf("BROKER - UNKNOWN_COMMAND ~> %s on %s", msg.Command, msg.Queue)
	}
}

func (b *Broker) MessageAck(ctx context.Context, msg *codec.Message) {
	consumerID, _ := GetConsumerID(ctx)
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	log.Printf("BROKER - MESSAGE_ACK ~> %s on %s for Task %s", consumerID, msg.Queue, taskID)
}

func (b *Broker) MessageDeny(ctx context.Context, msg *codec.Message) {
	consumerID, _ := GetConsumerID(ctx)
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	taskError, _ := jsonparser.GetString(msg.Payload, "error")
	log.Printf("BROKER - MESSAGE_DENY ~> %s on %s for Task %s, Error: %s", consumerID, msg.Queue, taskID, taskError)
}

func (b *Broker) OnConsumerPause(ctx context.Context, _ *codec.Message) {
	consumerID, _ := GetConsumerID(ctx)
	if consumerID != "" {
		if con, exists := b.consumers.Get(consumerID); exists {
			con.state = consts.ConsumerStatePaused
			log.Printf("BROKER - CONSUMER ~> Paused %s", consumerID)
		}
	}
}

func (b *Broker) OnConsumerStop(ctx context.Context, _ *codec.Message) {
	consumerID, _ := GetConsumerID(ctx)
	if consumerID != "" {
		if con, exists := b.consumers.Get(consumerID); exists {
			con.state = consts.ConsumerStateStopped
			log.Printf("BROKER - CONSUMER ~> Stopped %s", consumerID)
		}
	}
}

func (b *Broker) OnConsumerResume(ctx context.Context, _ *codec.Message) {
	consumerID, _ := GetConsumerID(ctx)
	if consumerID != "" {
		if con, exists := b.consumers.Get(consumerID); exists {
			con.state = consts.ConsumerStateActive
			log.Printf("BROKER - CONSUMER ~> Resumed %s", consumerID)
		}
	}
}

func (b *Broker) MessageResponseHandler(ctx context.Context, msg *codec.Message) {
	msg.Command = consts.RESPONSE
	b.HandleCallback(ctx, msg)
	awaitResponse, ok := GetAwaitResponse(ctx)
	if !(ok && awaitResponse == "true") {
		return
	}
	publisherID, exists := GetPublisherID(ctx)
	if !exists {
		return
	}
	con, ok := b.publishers.Get(publisherID)
	if !ok {
		return
	}
	err := b.send(ctx, con.conn, msg)
	if err != nil {
		panic(err)
	}
}

func (b *Broker) Publish(ctx context.Context, task *Task, queue string) error {
	headers, _ := GetHeaders(ctx)
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}
	msg := codec.NewMessage(consts.PUBLISH, payload, queue, headers.AsMap())
	b.broadcastToConsumers(msg)
	return nil
}

func (b *Broker) PublishHandler(ctx context.Context, conn net.Conn, msg *codec.Message) {
	pub := b.addPublisher(ctx, msg.Queue, conn)
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	log.Printf("BROKER - PUBLISH ~> received from %s on %s for Task %s", pub.id, msg.Queue, taskID)

	ack := codec.NewMessage(consts.PUBLISH_ACK, utils.ToByte(fmt.Sprintf(`{"id":"%s"}`, taskID)), msg.Queue, msg.Headers)
	if err := b.send(ctx, conn, ack); err != nil {
		log.Printf("Error sending PUBLISH_ACK: %v\n", err)
	}
	b.broadcastToConsumers(msg)
	go func() {
		select {
		case <-ctx.Done():
			b.publishers.Del(pub.id)
		}
	}()
}

func (b *Broker) SubscribeHandler(ctx context.Context, conn net.Conn, msg *codec.Message) {
	consumerID := b.AddConsumer(ctx, msg.Queue, conn)
	ack := codec.NewMessage(consts.SUBSCRIBE_ACK, nil, msg.Queue, msg.Headers)
	if err := b.send(ctx, conn, ack); err != nil {
		log.Printf("Error sending SUBSCRIBE_ACK: %v\n", err)
	}
	if b.opts.consumerOnSubscribe != nil {
		b.opts.consumerOnSubscribe(ctx, msg.Queue, consumerID)
	}
	go func() {
		select {
		case <-ctx.Done():
			b.RemoveConsumer(consumerID, msg.Queue)
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
	b.listener = listener
	defer b.Close()
	const maxConcurrentConnections = 100
	sem := make(chan struct{}, maxConcurrentConnections)
	for {
		conn, err := listener.Accept()
		if err != nil {
			b.OnError(ctx, conn, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		sem <- struct{}{}
		go func(c net.Conn) {
			defer func() {
				<-sem
				c.Close()
			}()
			for {
				err := b.readMessage(ctx, c)
				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
						log.Println("Temporary network error, retrying:", netErr)
						continue
					}
					log.Println("Connection closed due to error:", err)
					break
				}
			}
		}(conn)
	}
}

func (b *Broker) send(ctx context.Context, conn net.Conn, msg *codec.Message) error {
	return codec.SendMessage(ctx, conn, msg)
}

func (b *Broker) receive(ctx context.Context, c net.Conn) (*codec.Message, error) {
	return codec.ReadMessage(ctx, c)
}

func (b *Broker) broadcastToConsumers(msg *codec.Message) {
	if queue, ok := b.queues.Get(msg.Queue); ok {
		task := &QueuedTask{Message: msg, RetryCount: 0}
		queue.tasks <- task
	}
}

func (b *Broker) waitForConsumerAck(ctx context.Context, conn net.Conn) error {
	msg, err := b.receive(ctx, conn)
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

func (b *Broker) AddConsumer(ctx context.Context, queueName string, conn net.Conn) string {
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

func (b *Broker) RemoveConsumer(consumerID string, queues ...string) {
	if len(queues) > 0 {
		for _, queueName := range queues {
			if queue, ok := b.queues.Get(queueName); ok {
				con, ok := queue.consumers.Get(consumerID)
				if ok {
					con.conn.Close()
					queue.consumers.Del(consumerID)
				}
				b.queues.Del(queueName)
			}
		}
		return
	}
	b.queues.ForEach(func(queueName string, queue *Queue) bool {
		con, ok := queue.consumers.Get(consumerID)
		if ok {
			con.conn.Close()
			queue.consumers.Del(consumerID)
		}
		b.queues.Del(queueName)
		return true
	})
}

func (b *Broker) handleConsumer(ctx context.Context, cmd consts.CMD, state consts.ConsumerState, consumerID string, queues ...string) {
	fn := func(queue *Queue) {
		con, ok := queue.consumers.Get(consumerID)
		if ok {
			ack := codec.NewMessage(cmd, utils.ToByte("{}"), queue.name, map[string]string{consts.ConsumerKey: consumerID})
			err := b.send(ctx, con.conn, ack)
			if err == nil {
				con.state = state
			}
		}
	}
	if len(queues) > 0 {
		for _, queueName := range queues {
			if queue, ok := b.queues.Get(queueName); ok {
				fn(queue)
			}
		}
		return
	}
	b.queues.ForEach(func(queueName string, queue *Queue) bool {
		fn(queue)
		return true
	})
}

func (b *Broker) PauseConsumer(ctx context.Context, consumerID string, queues ...string) {
	b.handleConsumer(ctx, consts.CONSUMER_PAUSE, consts.ConsumerStatePaused, consumerID, queues...)
}

func (b *Broker) ResumeConsumer(ctx context.Context, consumerID string, queues ...string) {
	b.handleConsumer(ctx, consts.CONSUMER_RESUME, consts.ConsumerStateActive, consumerID, queues...)
}

func (b *Broker) StopConsumer(ctx context.Context, consumerID string, queues ...string) {
	b.handleConsumer(ctx, consts.CONSUMER_STOP, consts.ConsumerStateStopped, consumerID, queues...)
}

func (b *Broker) readMessage(ctx context.Context, c net.Conn) error {
	msg, err := b.receive(ctx, c)
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

func (b *Broker) dispatchWorker(ctx context.Context, queue *Queue) {
	delay := b.opts.initialDelay
	for task := range queue.tasks {
		success := false
		for !success && task.RetryCount <= b.opts.maxRetries {
			if b.dispatchTaskToConsumer(ctx, queue, task) {
				success = true
			} else {
				task.RetryCount++
				delay = b.backoffRetry(queue, task, delay)
			}
		}
		if task.RetryCount > b.opts.maxRetries {
			b.sendToDLQ(queue, task)
		}
	}
}

func (b *Broker) sendToDLQ(queue *Queue, task *QueuedTask) {
	id, _ := jsonparser.GetString(task.Message.Payload, "id")
	if dlq, ok := b.deadLetter.Get(queue.name); ok {
		log.Printf("Sending task %s to dead-letter queue for %s", id, queue.name)
		dlq.tasks <- task
	} else {
		log.Printf("No dead-letter queue for %s, discarding task %s", queue.name, id)
	}
}

func (b *Broker) dispatchTaskToConsumer(ctx context.Context, queue *Queue, task *QueuedTask) bool {
	var consumerFound bool
	var err error
	queue.consumers.ForEach(func(_ string, con *consumer) bool {
		if con.state != consts.ConsumerStateActive {
			err = fmt.Errorf("consumer %s is not active", con.id)
			return true
		}
		if err := b.send(ctx, con.conn, task.Message); err == nil {
			consumerFound = true
			return false
		}
		return true
	})
	if err != nil {
		log.Println(err.Error())
		return false
	}
	if !consumerFound {
		log.Printf("No available consumers for queue %s, retrying...", queue.name)
	}
	return consumerFound
}

func (b *Broker) backoffRetry(queue *Queue, task *QueuedTask, delay time.Duration) time.Duration {
	backoffDuration := utils.CalculateJitter(delay, b.opts.jitterPercent)
	log.Printf("Backing off for %v before retrying task for queue %s", backoffDuration, task.Message.Queue)
	time.Sleep(backoffDuration)
	queue.tasks <- task
	delay *= 2
	if delay > b.opts.maxBackoff {
		delay = b.opts.maxBackoff
	}
	return delay
}

func (b *Broker) URL() string {
	return b.opts.brokerAddr
}

func (b *Broker) Close() error {
	log.Printf("Broker is closing...")
	return b.listener.Close()
}
