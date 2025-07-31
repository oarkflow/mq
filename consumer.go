package mq

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/json/jsonparser"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/logger"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
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
	conn                 net.Conn
	handler              Handler
	pool                 *Pool
	opts                 *Options
	id                   string
	queue                string
	pIDs                 storage.IMap[string, bool]
	connMutex            sync.RWMutex
	isConnected          int32 // atomic flag
	isShutdown           int32 // atomic flag
	shutdown             chan struct{}
	reconnectCh          chan struct{}
	healthTicker         *time.Ticker
	logger               logger.Logger
	reconnectAttempts    int32      // track consecutive reconnection attempts
	lastReconnectAttempt time.Time  // track when last reconnect was attempted
	reconnectMutex       sync.Mutex // protect reconnection attempt tracking
}

func NewConsumer(id string, queue string, handler Handler, opts ...Option) *Consumer {
	options := SetupOptions(opts...)
	return &Consumer{
		id:          id,
		opts:        options,
		queue:       queue,
		handler:     handler,
		pIDs:        memory.New[string, bool](),
		shutdown:    make(chan struct{}),
		reconnectCh: make(chan struct{}, 1),
		logger:      options.Logger(),
	}
}

func (c *Consumer) send(ctx context.Context, conn net.Conn, msg *codec.Message) error {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	if atomic.LoadInt32(&c.isShutdown) == 1 {
		return fmt.Errorf("consumer is shutdown")
	}

	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	return codec.SendMessage(ctx, conn, msg)
}

func (c *Consumer) receive(ctx context.Context, conn net.Conn) (*codec.Message, error) {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	if atomic.LoadInt32(&c.isShutdown) == 1 {
		return nil, fmt.Errorf("consumer is shutdown")
	}

	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	return codec.ReadMessage(ctx, conn)
}

func (c *Consumer) Close() error {
	// Signal shutdown
	if !atomic.CompareAndSwapInt32(&c.isShutdown, 0, 1) {
		return nil // Already shutdown
	}

	close(c.shutdown)

	// Stop health checker
	if c.healthTicker != nil {
		c.healthTicker.Stop()
	}

	// Stop pool gracefully
	if c.pool != nil {
		c.pool.Stop()
	}

	// Close connection
	c.connMutex.Lock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		atomic.StoreInt32(&c.isConnected, 0)
		c.connMutex.Unlock()
		c.logger.Info("Connection closed for consumer", logger.Field{Key: "consumer_id", Value: c.id})
		return err
	}
	c.connMutex.Unlock()

	c.logger.Info("Consumer closed successfully", logger.Field{Key: "consumer_id", Value: c.id})
	return nil
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
	msg, err := codec.NewMessage(consts.SUBSCRIBE, utils.ToByte("{}"), queue, headers)
	if err != nil {
		return fmt.Errorf("error creating subscribe message: %v", err)
	}
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
		// Handle message consumption asynchronously to prevent blocking
		go c.ConsumeMessage(ctx, msg, conn)
		return nil
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
	case consts.CONSUMER_UPDATE:
		err := c.Update(ctx, msg.Payload)
		if err != nil {
			log.Printf("Unable to update consumer: %v", err)
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
	reply, err := codec.NewMessage(consts.MESSAGE_ACK, utils.ToByte(fmt.Sprintf(`{"id":"%s"}`, taskID)), msg.Queue, headers)
	if err != nil {
		c.logger.Error("Failed to create MESSAGE_ACK",
			logger.Field{Key: "queue", Value: msg.Queue},
			logger.Field{Key: "task_id", Value: taskID},
			logger.Field{Key: "error", Value: err.Error()})
		return
	}

	// Send with timeout to avoid blocking
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := c.send(sendCtx, conn, reply); err != nil {
		c.logger.Error("Failed to send MESSAGE_ACK",
			logger.Field{Key: "queue", Value: msg.Queue},
			logger.Field{Key: "task_id", Value: taskID},
			logger.Field{Key: "error", Value: err.Error()})
	}
}

func (c *Consumer) ConsumeMessage(ctx context.Context, msg *codec.Message, conn net.Conn) {
	// Send acknowledgment asynchronously
	go c.sendMessageAck(ctx, msg, conn)

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

	// Check if the task has already been processed
	if _, exists := c.pIDs.Get(task.ID); exists {
		log.Printf("Task %s already processed, skipping...", task.ID)
		return
	}

	// Process the task asynchronously to avoid blocking the main consumer loop
	go c.processTaskAsync(ctx, &task, msg.Queue)
}

func (c *Consumer) processTaskAsync(ctx context.Context, task *Task, queue string) {
	ctx = SetHeaders(ctx, map[string]string{consts.QueueKey: queue})

	// Try to enqueue the task with timeout
	enqueueDone := make(chan error, 1)
	go func() {
		err := c.pool.EnqueueTask(ctx, task, 1)
		enqueueDone <- err
	}()

	// Wait for enqueue with timeout
	select {
	case err := <-enqueueDone:
		if err == nil {
			// Mark the task as processed
			c.pIDs.Set(task.ID, true)
			return
		}
		// Handle enqueue error with retry logic
		c.retryTaskEnqueue(ctx, task, queue, err)
	case <-time.After(30 * time.Second): // Enqueue timeout
		c.logger.Error("Task enqueue timeout",
			logger.Field{Key: "task_id", Value: task.ID},
			logger.Field{Key: "queue", Value: queue})
		c.sendDenyMessage(ctx, task.ID, queue, fmt.Errorf("enqueue timeout"))
	}
}

func (c *Consumer) retryTaskEnqueue(ctx context.Context, task *Task, queue string, initialErr error) {
	retryCount := 0

	for retryCount < c.opts.maxRetries {
		retryCount++

		// Calculate backoff duration
		backoffDuration := utils.CalculateJitter(
			c.opts.initialDelay*time.Duration(1<<retryCount),
			c.opts.jitterPercent,
		)

		c.logger.Warn("Retrying task enqueue",
			logger.Field{Key: "task_id", Value: task.ID},
			logger.Field{Key: "attempt", Value: fmt.Sprintf("%d/%d", retryCount, c.opts.maxRetries)},
			logger.Field{Key: "backoff", Value: backoffDuration.String()},
			logger.Field{Key: "error", Value: initialErr.Error()})

		// Sleep in goroutine to avoid blocking
		time.Sleep(backoffDuration)

		// Try enqueue again
		if err := c.pool.EnqueueTask(ctx, task, 1); err == nil {
			c.pIDs.Set(task.ID, true)
			c.logger.Info("Task enqueue successful after retry",
				logger.Field{Key: "task_id", Value: task.ID},
				logger.Field{Key: "attempts", Value: retryCount})
			return
		}
	}

	// All retries failed
	c.logger.Error("Task enqueue failed after all retries",
		logger.Field{Key: "task_id", Value: task.ID},
		logger.Field{Key: "max_retries", Value: c.opts.maxRetries})
	c.sendDenyMessage(ctx, task.ID, queue, fmt.Errorf("enqueue failed after %d retries", c.opts.maxRetries))
}

func (c *Consumer) ProcessTask(ctx context.Context, msg *Task) Result {
	defer RecoverPanic(RecoverTitle)
	queue, _ := GetQueue(ctx)
	if msg.Topic == "" && queue != "" {
		msg.Topic = queue
	}

	// Apply timeout to individual task processing (not consumer connection)
	timeout := c.opts.ConsumerTimeout()
	if timeout > 0 {
		// Use configurable timeout for task processing
		taskCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Run task with timeout in a goroutine
		resultCh := make(chan Result, 1)
		go func() {
			result := c.handler(taskCtx, msg)
			result.Topic = msg.Topic
			result.TaskID = msg.ID
			resultCh <- result
		}()

		// Wait for result or timeout
		select {
		case result := <-resultCh:
			return result
		case <-taskCtx.Done():
			// Task processing timeout
			return Result{
				Error:  fmt.Errorf("task processing timeout after %v", timeout),
				Topic:  msg.Topic,
				TaskID: msg.ID,
				Status: "FAILED",
				Ctx:    ctx,
			}
		}
	}

	// No timeout - for page nodes that need unlimited time for user input
	result := c.handler(ctx, msg)
	result.Topic = msg.Topic
	result.TaskID = msg.ID
	return result
}

func (c *Consumer) OnResponse(ctx context.Context, result Result) error {
	if result.Status == "PENDING" && c.opts.respondPendingResult {
		return nil
	}

	// Send response asynchronously to avoid blocking task processing
	go func() {
		headers := HeadersWithConsumerIDAndQueue(ctx, c.id, result.Topic)
		if result.Status == "" {
			if result.Error != nil {
				result.Status = "FAILED"
			} else {
				result.Status = "SUCCESS"
			}
		}
		bt, _ := json.Marshal(result)
		reply, err := codec.NewMessage(consts.MESSAGE_RESPONSE, bt, result.Topic, headers)
		if err != nil {
			c.logger.Error("Failed to create MESSAGE_RESPONSE",
				logger.Field{Key: "topic", Value: result.Topic},
				logger.Field{Key: "task_id", Value: result.TaskID},
				logger.Field{Key: "error", Value: err.Error()})
			return
		}

		sendCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := c.send(sendCtx, c.conn, reply); err != nil {
			c.logger.Error("Failed to send MESSAGE_RESPONSE",
				logger.Field{Key: "topic", Value: result.Topic},
				logger.Field{Key: "task_id", Value: result.TaskID},
				logger.Field{Key: "error", Value: err.Error()})
		}
	}()

	return nil
}

func (c *Consumer) sendDenyMessage(ctx context.Context, taskID, queue string, err error) {
	// Send deny message asynchronously to avoid blocking
	go func() {
		headers := HeadersWithConsumerID(ctx, c.id)
		reply, err := codec.NewMessage(consts.MESSAGE_DENY, utils.ToByte(fmt.Sprintf(`{"id":"%s", "error":"%s"}`, taskID, err.Error())), queue, headers)
		if err != nil {
			c.logger.Error("Failed to create MESSAGE_DENY",
				logger.Field{Key: "queue", Value: queue},
				logger.Field{Key: "task_id", Value: taskID},
				logger.Field{Key: "error", Value: err.Error()})
			return
		}

		sendCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if sendErr := c.send(sendCtx, c.conn, reply); sendErr != nil {
			c.logger.Error("Failed to send MESSAGE_DENY",
				logger.Field{Key: "queue", Value: queue},
				logger.Field{Key: "task_id", Value: taskID},
				logger.Field{Key: "original_error", Value: err.Error()},
				logger.Field{Key: "send_error", Value: sendErr.Error()})
		}
	}()
}

// isHealthy checks if the connection is still healthy
func (c *Consumer) isHealthy() bool {
	c.connMutex.RLock()
	defer c.connMutex.RUnlock()

	if c.conn == nil || atomic.LoadInt32(&c.isConnected) == 0 {
		return false
	}

	// Simple health check by setting read deadline
	c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	defer c.conn.SetReadDeadline(time.Time{})

	one := make([]byte, 1)
	n, err := c.conn.Read(one)

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return true // Timeout is expected for health check
		}
		return false
	}

	// If we read data, put it back (this shouldn't happen in health check)
	if n > 0 {
		// This is a simplified health check; in production, you might want to buffer this
		return true
	}

	return true
}

// startHealthChecker starts periodic health checks
func (c *Consumer) startHealthChecker() {
	c.healthTicker = time.NewTicker(30 * time.Second)
	go func() {
		defer c.healthTicker.Stop()
		for {
			select {
			case <-c.healthTicker.C:
				if !c.isHealthy() {
					c.logger.Warn("Connection health check failed, triggering reconnection",
						logger.Field{Key: "consumer_id", Value: c.id})
					select {
					case c.reconnectCh <- struct{}{}:
					default:
						// Channel is full, reconnection already pending
					}
				}
			case <-c.shutdown:
				return
			}
		}
	}()
}

func (c *Consumer) attemptConnect() error {
	if atomic.LoadInt32(&c.isShutdown) == 1 {
		return fmt.Errorf("consumer is shutdown")
	}

	var err error
	delay := c.opts.initialDelay

	for i := 0; i < c.opts.maxRetries; i++ {
		if atomic.LoadInt32(&c.isShutdown) == 1 {
			return fmt.Errorf("consumer is shutdown")
		}

		conn, err := GetConnection(c.opts.brokerAddr, c.opts.tlsConfig)
		if err == nil {
			c.connMutex.Lock()
			c.conn = conn
			atomic.StoreInt32(&c.isConnected, 1)
			c.connMutex.Unlock()

			c.logger.Info("Successfully connected to broker",
				logger.Field{Key: "consumer_id", Value: c.id},
				logger.Field{Key: "broker_addr", Value: c.opts.brokerAddr})
			return nil
		}

		sleepDuration := utils.CalculateJitter(delay, c.opts.jitterPercent)
		c.logger.Warn("Failed to connect to broker, retrying",
			logger.Field{Key: "consumer_id", Value: c.id},
			logger.Field{Key: "broker_addr", Value: c.opts.brokerAddr},
			logger.Field{Key: "attempt", Value: fmt.Sprintf("%d/%d", i+1, c.opts.maxRetries)},
			logger.Field{Key: "error", Value: err.Error()},
			logger.Field{Key: "retry_in", Value: sleepDuration.String()})

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
	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Initial connection
	if err := c.attemptConnect(); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}

	// Initialize pool
	c.pool = NewPool(
		c.opts.numOfWorkers,
		WithTaskQueueSize(c.opts.queueSize),
		WithMaxMemoryLoad(c.opts.maxMemoryLoad),
		WithHandler(c.ProcessTask),
		WithPoolCallback(c.OnResponse),
		WithTaskStorage(c.opts.storage),
	)

	// Subscribe to queue
	if err := c.subscribe(ctx, c.queue); err != nil {
		return fmt.Errorf("failed to subscribe to queue %s: %w", c.queue, err)
	}

	// Start worker pool
	c.pool.Start(c.opts.numOfWorkers)

	// Start health checker
	c.startHealthChecker()

	// Start HTTP API if enabled
	if c.opts.enableHTTPApi {
		go func() {
			if _, err := c.StartHTTPAPI(); err != nil {
				c.logger.Error("Failed to start HTTP API",
					logger.Field{Key: "consumer_id", Value: c.id},
					logger.Field{Key: "error", Value: err.Error()})
			}
		}()
	}

	c.logger.Info("Consumer started successfully",
		logger.Field{Key: "consumer_id", Value: c.id},
		logger.Field{Key: "queue", Value: c.queue})

	// Main processing loop with enhanced error handling
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Context cancelled, stopping consumer",
				logger.Field{Key: "consumer_id", Value: c.id})
			return c.Close()

		case <-c.shutdown:
			c.logger.Info("Shutdown signal received",
				logger.Field{Key: "consumer_id", Value: c.id})
			return nil

		case <-c.reconnectCh:
			c.logger.Info("Reconnection triggered",
				logger.Field{Key: "consumer_id", Value: c.id})
			if err := c.handleReconnection(ctx); err != nil {
				c.logger.Error("Reconnection failed, will retry based on backoff policy",
					logger.Field{Key: "consumer_id", Value: c.id},
					logger.Field{Key: "error", Value: err.Error()})
				// The handleReconnection method now implements its own backoff,
				// so we don't need to do anything here except continue the loop
			}

		default:
			// Apply rate limiting if configured
			if c.opts.ConsumerRateLimiter != nil {
				c.opts.ConsumerRateLimiter.Wait()
			}

			// Process messages with timeout
			if err := c.processWithTimeout(ctx); err != nil {
				if atomic.LoadInt32(&c.isShutdown) == 1 {
					return nil
				}

				c.logger.Error("Error processing message",
					logger.Field{Key: "consumer_id", Value: c.id},
					logger.Field{Key: "error", Value: err.Error()})

				// Trigger reconnection for connection errors
				if isConnectionError(err) {
					select {
					case c.reconnectCh <- struct{}{}:
					default:
					}
				}

				// Brief pause before retrying
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (c *Consumer) processWithTimeout(ctx context.Context) error {
	// Consumer should wait indefinitely for messages from broker - no I/O timeout
	// Only individual task processing should have timeouts, not the consumer connection
	c.connMutex.RLock()
	conn := c.conn
	c.connMutex.RUnlock()

	if conn == nil {
		return fmt.Errorf("no connection available")
	}

	// Read message without timeout - consumer should be long-running background service
	err := c.readMessage(ctx, conn)

	// If message was processed successfully, reset reconnection attempts
	if err == nil {
		if atomic.LoadInt32(&c.reconnectAttempts) > 0 {
			atomic.StoreInt32(&c.reconnectAttempts, 0)
			c.logger.Debug("Reset reconnection attempts after successful message processing",
				logger.Field{Key: "consumer_id", Value: c.id})
		}
	}

	return err
}

func (c *Consumer) handleReconnection(ctx context.Context) error {
	c.reconnectMutex.Lock()
	defer c.reconnectMutex.Unlock()

	// Increment reconnection attempts
	attempts := atomic.AddInt32(&c.reconnectAttempts, 1)

	// Calculate backoff based on consecutive attempts
	backoffDelay := utils.CalculateJitter(
		c.opts.initialDelay*time.Duration(1<<minInt(int(attempts-1), 6)), // Cap exponential growth at 2^6
		c.opts.jitterPercent,
	)

	// Cap maximum backoff
	if backoffDelay > c.opts.maxBackoff {
		backoffDelay = c.opts.maxBackoff
	}

	// If we've been reconnecting too frequently, implement circuit breaker logic
	timeSinceLastAttempt := time.Since(c.lastReconnectAttempt)
	if attempts > 1 && timeSinceLastAttempt < backoffDelay {
		remainingWait := backoffDelay - timeSinceLastAttempt
		c.logger.Warn("Throttling reconnection attempt",
			logger.Field{Key: "consumer_id", Value: c.id},
			logger.Field{Key: "consecutive_attempts", Value: int(attempts)},
			logger.Field{Key: "wait_duration", Value: remainingWait.String()})

		// Wait with context cancellation support
		select {
		case <-time.After(remainingWait):
		case <-ctx.Done():
			return ctx.Err()
		case <-c.shutdown:
			return fmt.Errorf("consumer is shutdown")
		}
	}

	c.lastReconnectAttempt = time.Now()

	// If we've exceeded reasonable attempts, implement longer backoff
	if attempts > int32(c.opts.maxRetries*2) {
		longBackoff := 5 * time.Minute // Long circuit breaker period
		c.logger.Warn("Too many consecutive reconnection attempts, entering long backoff",
			logger.Field{Key: "consumer_id", Value: c.id},
			logger.Field{Key: "consecutive_attempts", Value: int(attempts)},
			logger.Field{Key: "backoff_duration", Value: longBackoff.String()})

		select {
		case <-time.After(longBackoff):
		case <-ctx.Done():
			return ctx.Err()
		case <-c.shutdown:
			return fmt.Errorf("consumer is shutdown")
		}
	}

	// Mark as disconnected
	atomic.StoreInt32(&c.isConnected, 0)

	// Close existing connection
	c.connMutex.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.connMutex.Unlock()

	c.logger.Info("Attempting reconnection",
		logger.Field{Key: "consumer_id", Value: c.id},
		logger.Field{Key: "attempt", Value: int(attempts)},
		logger.Field{Key: "backoff_delay", Value: backoffDelay.String()})

	// Attempt reconnection
	if err := c.attemptConnect(); err != nil {
		c.logger.Error("Reconnection failed",
			logger.Field{Key: "consumer_id", Value: c.id},
			logger.Field{Key: "attempt", Value: int(attempts)},
			logger.Field{Key: "error", Value: err.Error()})
		return fmt.Errorf("failed to reconnect (attempt %d): %w", attempts, err)
	}

	// Reconnection successful, try to resubscribe
	if err := c.subscribe(ctx, c.queue); err != nil {
		c.logger.Error("Failed to resubscribe after reconnection",
			logger.Field{Key: "consumer_id", Value: c.id},
			logger.Field{Key: "error", Value: err.Error()})
		return fmt.Errorf("failed to resubscribe after reconnection: %w", err)
	}

	// Reset reconnection attempts on successful reconnection
	atomic.StoreInt32(&c.reconnectAttempts, 0)

	c.logger.Info("Successfully reconnected and resubscribed",
		logger.Field{Key: "consumer_id", Value: c.id})
	return nil
}

// Helper function to get minimum of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "closed network") ||
		strings.Contains(errStr, "broken pipe")
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

func (c *Consumer) Update(ctx context.Context, payload []byte) error {
	var newConfig DynamicConfig
	if err := json.Unmarshal(payload, &newConfig); err != nil {
		log.Printf("Invalid payload for CONSUMER_UPDATE: %v", err)
		return err
	}
	if c.pool != nil {
		if err := c.pool.UpdateConfig(&newConfig); err != nil {
			log.Printf("Failed to update pool config: %v", err)
			return err
		}
	}
	return c.sendOpsMessage(ctx, consts.CONSUMER_UPDATED)
}

func (c *Consumer) Resume(ctx context.Context) error {
	return c.operate(ctx, consts.CONSUMER_RESUMED, c.pool.Resume)
}

func (c *Consumer) Stop(ctx context.Context) error {
	return c.operate(ctx, consts.CONSUMER_STOPPED, c.pool.Stop)
}

func (c *Consumer) operate(ctx context.Context, cmd consts.CMD, poolOperation func()) error {
	poolOperation()
	if err := c.sendOpsMessage(ctx, cmd); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) sendOpsMessage(ctx context.Context, cmd consts.CMD) error {
	headers := HeadersWithConsumerID(ctx, c.id)
	msg, err := codec.NewMessage(cmd, nil, c.queue, headers)
	if err != nil {
		return fmt.Errorf("error creating operation message: %v", err)
	}
	return c.send(ctx, c.conn, msg)
}

func (c *Consumer) Conn() net.Conn {
	return c.conn
}

// StartHTTPAPI starts an HTTP server on a random available port and registers API endpoints.
// It returns the port number the server is listening on.
func (c *Consumer) StartHTTPAPI() (int, error) {
	// Listen on a random port.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, fmt.Errorf("failed to start listener: %w", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	// Create a new HTTP mux and register endpoints.
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", c.handleStats)
	mux.HandleFunc("/update", c.handleUpdate)
	mux.HandleFunc("/pause", c.handlePause)
	mux.HandleFunc("/resume", c.handleResume)
	mux.HandleFunc("/stop", c.handleStop)

	// Start the server in a new goroutine.
	go func() {
		// Log errors if the HTTP server stops.
		if err := http.Serve(ln, mux); err != nil {
			log.Printf("HTTP server error on port %d: %v", port, err)
		}
	}()

	log.Printf("HTTP API for consumer %s started on port %d", c.id, port)
	return port, nil
}

// handleStats responds with JSON containing consumer and pool metrics.
func (c *Consumer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Gather consumer and pool stats using formatted metrics.
	stats := map[string]interface{}{
		"consumer_id":  c.id,
		"queue":        c.queue,
		"pool_metrics": c.pool.FormattedMetrics(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode stats: %v", err), http.StatusInternalServerError)
	}
}

// handleUpdate accepts a POST request with a JSON payload to update the consumer's pool configuration.
// It reuses the consumer's Update method which updates the pool configuration.
func (c *Consumer) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read the request body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Call the Update method on the consumer (which in turn updates the pool configuration).
	if err := c.Update(r.Context(), body); err != nil {
		http.Error(w, fmt.Sprintf("failed to update configuration: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{"status": "configuration updated"}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

// handlePause pauses the consumer's pool.
func (c *Consumer) handlePause(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := c.Pause(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("failed to pause consumer: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{"status": "consumer paused"}
	json.NewEncoder(w).Encode(resp)
}

// handleResume resumes the consumer's pool.
func (c *Consumer) handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := c.Resume(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("failed to resume consumer: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{"status": "consumer resumed"}
	json.NewEncoder(w).Encode(resp)
}

// handleStop stops the consumer's pool.
func (c *Consumer) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Stop the consumer.
	if err := c.Stop(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("failed to stop consumer: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	resp := map[string]string{"status": "consumer stopped"}
	json.NewEncoder(w).Encode(resp)
}
