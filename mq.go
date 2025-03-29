package mq

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/json"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/jsonparser"
	"github.com/oarkflow/mq/logger"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
	"github.com/oarkflow/mq/utils"
)

type Status string

const (
	Pending    Status = "Pending"
	Processing Status = "Processing"
	Completed  Status = "Completed"
	Failed     Status = "Failed"
)

type Result struct {
	CreatedAt       time.Time       `json:"created_at"`
	ProcessedAt     time.Time       `json:"processed_at,omitempty"`
	Latency         string          `json:"latency"`
	Error           error           `json:"-"` // Keep error as an error type
	Topic           string          `json:"topic"`
	TaskID          string          `json:"task_id"`
	Status          Status          `json:"status"`
	ConditionStatus string          `json:"condition_status"`
	Ctx             context.Context `json:"-"`
	Payload         json.RawMessage `json:"payload"`
	Last            bool
}

func (r Result) MarshalJSON() ([]byte, error) {
	type Alias Result
	aux := &struct {
		ErrorMsg string `json:"error,omitempty"`
		Alias
	}{
		Alias: (Alias)(r),
	}
	if r.Error != nil {
		aux.ErrorMsg = r.Error.Error()
	}
	return json.Marshal(aux)
}

func (r *Result) UnmarshalJSON(data []byte) error {
	type Alias Result
	aux := &struct {
		*Alias
		ErrMsg string `json:"error,omitempty"`
	}{
		Alias: (*Alias)(r),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.ErrMsg != "" {
		r.Error = errors.New(aux.ErrMsg)
	} else {
		r.Error = nil
	}

	return nil
}

func (r Result) Unmarshal(data any) error {
	if r.Payload == nil {
		return fmt.Errorf("payload is nil")
	}
	return json.Unmarshal(r.Payload, data)
}

func HandleError(ctx context.Context, err error, status ...Status) Result {
	st := Failed
	if len(status) > 0 {
		st = status[0]
	}
	if err == nil {
		return Result{Ctx: ctx}
	}
	return Result{
		Ctx:    ctx,
		Status: st,
		Error:  err,
	}
}

func (r Result) WithData(status Status, data []byte) Result {
	if r.Error != nil {
		return r
	}
	return Result{
		Status:  status,
		Payload: data,
		Ctx:     r.Ctx,
	}
}

type TLSConfig struct {
	CertPath string
	KeyPath  string
	CAPath   string
	UseTLS   bool
}

type Options struct {
	storage              TaskStorage
	consumerOnSubscribe  func(ctx context.Context, topic, consumerName string)
	consumerOnClose      func(ctx context.Context, topic, consumerName string)
	notifyResponse       func(context.Context, Result) error
	brokerAddr           string
	tlsConfig            TLSConfig
	callback             []func(context.Context, Result) Result
	queueSize            int
	initialDelay         time.Duration
	maxBackoff           time.Duration
	jitterPercent        float64
	maxRetries           int
	numOfWorkers         int
	maxMemoryLoad        int64
	syncMode             bool
	cleanTaskOnComplete  bool
	enableWorkerPool     bool
	respondPendingResult bool
	logger               logger.Logger
}

func (o *Options) SetSyncMode(sync bool) {
	o.syncMode = sync
}

func (o *Options) NumOfWorkers() int {
	return o.numOfWorkers
}

func (o *Options) Logger() logger.Logger {
	return o.logger
}

func (o *Options) Storage() TaskStorage {
	return o.storage
}

func (o *Options) CleanTaskOnComplete() bool {
	return o.cleanTaskOnComplete
}

func (o *Options) QueueSize() int {
	return o.queueSize
}

func (o *Options) MaxMemoryLoad() int64 {
	return o.maxMemoryLoad
}

func (o *Options) BrokerAddr() string {
	return o.brokerAddr
}

func HeadersWithConsumerID(ctx context.Context, id string) map[string]string {
	return WithHeaders(ctx, map[string]string{consts.ConsumerKey: id, consts.ContentType: consts.TypeJson})
}

func HeadersWithConsumerIDAndQueue(ctx context.Context, id, queue string) map[string]string {
	return WithHeaders(ctx, map[string]string{
		consts.ConsumerKey: id,
		consts.ContentType: consts.TypeJson,
		consts.QueueKey:    queue,
	})
}

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

// Modified backoffRetry: Removed re‑insertion of the task into queue.tasks.
func (b *Broker) backoffRetry(queue *Queue, task *QueuedTask, delay time.Duration) time.Duration {
	backoffDuration := utils.CalculateJitter(delay, b.opts.jitterPercent)
	log.Printf("Backing off for %v before retrying task for queue %s", backoffDuration, task.Message.Queue)
	time.Sleep(backoffDuration)
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
	if b != nil && b.listener != nil {
		log.Printf("Broker is closing...")
		return b.listener.Close()
	}
	return nil
}

func (b *Broker) SetURL(url string) {
	b.opts.brokerAddr = url
}

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
	// Infinite loop to continuously read messages and reconnect if needed.
	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping consumer.")
			return nil
		default:
			if err := c.readMessage(ctx, c.conn); err != nil {
				log.Printf("Error reading message: %v, attempting reconnection...", err)
				// Attempt reconnection loop.
				for {
					if ctx.Err() != nil {
						return nil
					}
					if rErr := c.attemptConnect(); rErr != nil {
						log.Printf("Reconnection attempt failed: %v", rErr)
						time.Sleep(c.opts.initialDelay)
					} else {
						break
					}
				}
				if err := c.subscribe(ctx, c.queue); err != nil {
					log.Printf("Failed to re-subscribe on reconnection: %v", err)
					time.Sleep(c.opts.initialDelay)
				}
			}
		}
	}
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

type Publisher struct {
	opts     *Options
	id       string
	conn     net.Conn
	connLock sync.Mutex
}

func NewPublisher(id string, opts ...Option) *Publisher {
	options := SetupOptions(opts...)
	return &Publisher{
		id:   id,
		opts: options,
		conn: nil,
	}
}

// New method to ensure a persistent connection.
func (p *Publisher) ensureConnection(ctx context.Context) error {
	p.connLock.Lock()
	defer p.connLock.Unlock()
	if p.conn != nil {
		return nil
	}
	var err error
	delay := p.opts.initialDelay
	for i := 0; i < p.opts.maxRetries; i++ {
		var conn net.Conn
		conn, err = GetConnection(p.opts.brokerAddr, p.opts.tlsConfig)
		if err == nil {
			p.conn = conn
			return nil
		}
		sleepDuration := utils.CalculateJitter(delay, p.opts.jitterPercent)
		log.Printf("PUBLISHER - ensureConnection failed: %v, attempt %d/%d, retrying in %v...", err, i+1, p.opts.maxRetries, sleepDuration)
		time.Sleep(sleepDuration)
		delay *= 2
		if delay > p.opts.maxBackoff {
			delay = p.opts.maxBackoff
		}
	}
	return fmt.Errorf("failed to connect to broker after retries: %w", err)
}

// Modified Publish method that uses the persistent connection.
func (p *Publisher) Publish(ctx context.Context, task Task, queue string) error {
	// Ensure connection is established.
	if err := p.ensureConnection(ctx); err != nil {
		return err
	}
	delay := p.opts.initialDelay
	for i := 0; i < p.opts.maxRetries; i++ {
		// Use the persistent connection.
		p.connLock.Lock()
		conn := p.conn
		p.connLock.Unlock()
		err := p.send(ctx, queue, task, conn, consts.PUBLISH)
		if err == nil {
			return nil
		}
		log.Printf("PUBLISHER - Failed publishing: %v, attempt %d/%d, retrying...", err, i+1, p.opts.maxRetries)
		// On error, close and reset the connection.
		p.connLock.Lock()
		if p.conn != nil {
			p.conn.Close()
			p.conn = nil
		}
		p.connLock.Unlock()
		sleepDuration := utils.CalculateJitter(delay, p.opts.jitterPercent)
		time.Sleep(sleepDuration)
		delay *= 2
		if delay > p.opts.maxBackoff {
			delay = p.opts.maxBackoff
		}
		// Ensure connection is re-established.
		if err := p.ensureConnection(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("failed to publish after retries")
}

func (p *Publisher) send(ctx context.Context, queue string, task Task, conn net.Conn, command consts.CMD) error {
	headers := WithHeaders(ctx, map[string]string{
		consts.PublisherKey: p.id,
		consts.ContentType:  consts.TypeJson,
	})
	if task.ID == "" {
		task.ID = NewID()
	}
	task.CreatedAt = time.Now()
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}
	msg := codec.NewMessage(command, payload, queue, headers)
	if err := codec.SendMessage(ctx, conn, msg); err != nil {
		return err
	}

	return p.waitForAck(ctx, conn)
}

func (p *Publisher) waitForAck(ctx context.Context, conn net.Conn) error {
	msg, err := codec.ReadMessage(ctx, conn)
	if err != nil {
		return err
	}
	if msg.Command == consts.PUBLISH_ACK {
		taskID, _ := jsonparser.GetString(msg.Payload, "id")
		log.Printf("PUBLISHER - PUBLISH_ACK ~> from %s on %s for Task %s", p.id, msg.Queue, taskID)
		return nil
	}
	return fmt.Errorf("expected PUBLISH_ACK, got: %v", msg.Command)
}

func (p *Publisher) waitForResponse(ctx context.Context, conn net.Conn) Result {
	msg, err := codec.ReadMessage(ctx, conn)
	if err != nil {
		return Result{Error: err}
	}
	if msg.Command == consts.RESPONSE {
		var result Result
		err = json.Unmarshal(msg.Payload, &result)
		return result
	}
	err = fmt.Errorf("expected RESPONSE, got: %v", msg.Command)
	return Result{Error: err}
}

func (p *Publisher) onClose(_ context.Context, conn net.Conn) error {
	fmt.Println("Publisher Connection closed", p.id, conn.RemoteAddr())
	return nil
}

func (p *Publisher) onError(_ context.Context, conn net.Conn, err error) {
	fmt.Println("Error reading from publisher connection:", err, conn.RemoteAddr())
}

func (p *Publisher) Request(ctx context.Context, task Task, queue string) Result {
	ctx = SetHeaders(ctx, map[string]string{
		consts.AwaitResponseKey: "true",
	})
	conn, err := GetConnection(p.opts.brokerAddr, p.opts.tlsConfig)
	if err != nil {
		err = fmt.Errorf("failed to connect to broker: %w", err)
		return Result{Error: err}
	}
	defer func() {
		_ = conn.Close()
	}()
	err = p.send(ctx, queue, task, conn, consts.PUBLISH)
	resultCh := make(chan Result)
	go func() {
		defer close(resultCh)
		resultCh <- p.waitForResponse(ctx, conn)
	}()
	finalResult := <-resultCh
	return finalResult
}

type Queue struct {
	consumers storage.IMap[string, *consumer]
	tasks     chan *QueuedTask // channel to hold tasks
	name      string
}

func newQueue(name string, queueSize int) *Queue {
	return &Queue{
		name:      name,
		consumers: memory.New[string, *consumer](),
		tasks:     make(chan *QueuedTask, queueSize), // buffer size for tasks
	}
}

func (b *Broker) NewQueue(name string) *Queue {
	q := &Queue{
		name:      name,
		tasks:     make(chan *QueuedTask, b.opts.queueSize),
		consumers: memory.New[string, *consumer](),
	}
	b.queues.Set(name, q)

	// Create DLQ for the queue
	dlq := &Queue{
		name:      name + "_dlq",
		tasks:     make(chan *QueuedTask, b.opts.queueSize),
		consumers: memory.New[string, *consumer](),
	}
	b.deadLetter.Set(name, dlq)
	ctx := context.Background()
	go b.dispatchWorker(ctx, q)
	go b.dispatchWorker(ctx, dlq)
	return q
}

type QueueTask struct {
	ctx        context.Context
	payload    *Task
	priority   int
	retryCount int
	index      int
}

type PriorityQueue []*QueueTask

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	task := x.(*QueueTask)
	task.index = n
	*pq = append(*pq, task)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	task := old[n-1]
	task.index = -1
	*pq = old[0 : n-1]
	return task
}

type Task struct {
	CreatedAt   time.Time       `json:"created_at"`
	ProcessedAt time.Time       `json:"processed_at"`
	Expiry      time.Time       `json:"expiry"`
	Error       error           `json:"error"`
	ID          string          `json:"id"`
	Topic       string          `json:"topic"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
	dag         any
}

func (t *Task) GetFlow() any {
	return t.dag
}

func NewTask(id string, payload json.RawMessage, nodeKey string, opts ...TaskOption) *Task {
	if id == "" {
		id = NewID()
	}
	task := &Task{ID: id, Payload: payload, Topic: nodeKey, CreatedAt: time.Now()}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

// TaskOption defines a function type for setting options.
type TaskOption func(*Task)

func WithDAG(dag any) TaskOption {
	return func(opts *Task) {
		opts.dag = dag
	}
}

func (b *Broker) TLSConfig() TLSConfig {
	return b.opts.tlsConfig
}

func (b *Broker) SyncMode() bool {
	return b.opts.syncMode
}

func (b *Broker) NotifyHandler() func(context.Context, Result) error {
	return b.opts.notifyResponse
}

func (b *Broker) SetNotifyHandler(callback Callback) {
	b.opts.notifyResponse = callback
}

func (b *Broker) HandleCallback(ctx context.Context, msg *codec.Message) {
	if b.opts.callback != nil {
		var result Result
		err := json.Unmarshal(msg.Payload, &result)
		if err == nil {
			for _, callback := range b.opts.callback {
				callback(ctx, result)
			}
		}
	}
}
