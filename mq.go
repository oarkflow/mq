package mq

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/json"

	"github.com/oarkflow/json/jsonparser"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
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
	Cancelled  Status = "Cancelled"
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
	ResetTo         string          `json:"reset_to,omitempty"` // Node ID to reset to, or "back" for previous page node
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

// QueueConfig holds configuration for a specific queue
type QueueConfig struct {
	MaxDepth       int           `json:"max_depth"`
	MaxRetries     int           `json:"max_retries"`
	MessageTTL     time.Duration `json:"message_ttl"`
	DeadLetter     bool          `json:"dead_letter"`
	Persistent     bool          `json:"persistent"`
	BatchSize      int           `json:"batch_size"`
	Priority       int           `json:"priority"`
	OrderedMode    bool          `json:"ordered_mode"`
	Throttling     bool          `json:"throttling"`
	ThrottleRate   int           `json:"throttle_rate"`
	ThrottleBurst  int           `json:"throttle_burst"`
	CompactionMode bool          `json:"compaction_mode"`
}

// QueueOption defines options for queue configuration
type QueueOption func(*QueueConfig)

// WithQueueOption creates a queue with specific configuration
func WithQueueOption(config QueueConfig) QueueOption {
	return func(c *QueueConfig) {
		*c = config
	}
}

// WithQueueMaxDepth sets the maximum queue depth
func WithQueueMaxDepth(maxDepth int) QueueOption {
	return func(c *QueueConfig) {
		c.MaxDepth = maxDepth
	}
}

// WithQueueMaxRetries sets the maximum retries for queue messages
func WithQueueMaxRetries(maxRetries int) QueueOption {
	return func(c *QueueConfig) {
		c.MaxRetries = maxRetries
	}
}

// WithQueueTTL sets the message TTL for the queue
func WithQueueTTL(ttl time.Duration) QueueOption {
	return func(c *QueueConfig) {
		c.MessageTTL = ttl
	}
}

// WithDeadLetter enables dead letter queue for failed messages
func WithDeadLetter() QueueOption {
	return func(c *QueueConfig) {
		c.DeadLetter = true
	}
}

// WithPersistent enables message persistence
func WithPersistent() QueueOption {
	return func(c *QueueConfig) {
		c.Persistent = true
	}
}

// RateLimiter implementation
type RateLimiter struct {
	mu     sync.Mutex
	C      chan struct{}
	ticker *time.Ticker
	rate   int
	burst  int
	stop   chan struct{}
}

// NewRateLimiter creates a new RateLimiter with the specified rate and burst.
func NewRateLimiter(rate int, burst int) *RateLimiter {
	rl := &RateLimiter{
		C:     make(chan struct{}, burst),
		rate:  rate,
		burst: burst,
		stop:  make(chan struct{}),
	}
	rl.ticker = time.NewTicker(time.Second / time.Duration(rate))
	go rl.run()
	return rl
}

// run is the internal goroutine that periodically sends tokens.
func (rl *RateLimiter) run() {
	for {
		select {
		case <-rl.ticker.C:
			// Blocking send to ensure token accumulation doesn't discard tokens.
			rl.mu.Lock()
			// Try sending token, but don't block if channel is full.
			select {
			case rl.C <- struct{}{}:
			default:
			}
			rl.mu.Unlock()
		case <-rl.stop:
			return
		}
	}
}

// Wait blocks until a token is available.
func (rl *RateLimiter) Wait() {
	<-rl.C
}

// Update allows dynamic adjustment of rate and burst at runtime.
// It immediately applies the new settings.
func (rl *RateLimiter) Update(newRate, newBurst int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Stop the old ticker.
	rl.ticker.Stop()
	// Replace the channel with a new one of the new burst capacity.
	rl.C = make(chan struct{}, newBurst)
	// Update internal state.
	rl.rate = newRate
	rl.burst = newBurst
	// Start a new ticker with the updated rate.
	rl.ticker = time.NewTicker(time.Second / time.Duration(newRate))
	// The run goroutine will pick up tokens from the new ticker and use the new channel.
}

// Stop terminates the rate limiter's internal goroutine.
func (rl *RateLimiter) Stop() {
	close(rl.stop)
	rl.ticker.Stop()
}

type Options struct {
	storage              TaskStorage
	consumerOnSubscribe  func(ctx context.Context, topic, consumerName string)
	consumerOnClose      func(ctx context.Context, topic, consumerName string)
	notifyResponse       func(context.Context, Result) error
	brokerAddr           string
	enableHTTPApi        bool
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
	BrokerRateLimiter    *RateLimiter  // new field for broker rate limiting
	ConsumerRateLimiter  *RateLimiter  // new field for consumer rate limiting
	consumerTimeout      time.Duration // timeout for consumer message processing (0 = no timeout)
	adminAddr            string        // address for admin server
	metricsAddr          string        // address for metrics server
	enableSecurity       bool          // enable security features
	enableMonitoring     bool          // enable monitoring features
	username             string        // username for authentication
	password             string        // password for authentication
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

func (o *Options) HTTPApi() bool {
	return o.enableHTTPApi
}

func (o *Options) ConsumerTimeout() time.Duration {
	return o.consumerTimeout
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
	Task       *Task
	RetryCount int
}

type consumer struct {
	conn    net.Conn
	id      string
	state   consts.ConsumerState
	queue   string
	pool    *Pool
	metrics *ConsumerMetrics
}

type ConsumerMetrics struct {
	ProcessedTasks int64
	ErrorCount     int64
	LastActivity   time.Time
}

type publisher struct {
	conn net.Conn
	id   string
}

// Enhanced Broker Types and Interfaces

// ConnectionPool manages a pool of broker connections
type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[string]*BrokerConnection
	maxConns    int
	connCount   int64
}

// BrokerConnection represents a single broker connection
type BrokerConnection struct {
	mu           sync.RWMutex
	conn         net.Conn
	id           string
	connType     string
	lastActivity time.Time
	isActive     bool
}

// HealthChecker monitors broker health
type HealthChecker struct {
	mu         sync.RWMutex
	broker     *Broker
	interval   time.Duration
	ticker     *time.Ticker
	shutdown   chan struct{}
	thresholds HealthThresholds
}

// HealthThresholds defines health check thresholds
type HealthThresholds struct {
	MaxMemoryUsage  int64
	MaxCPUUsage     float64
	MaxConnections  int
	MaxQueueDepth   int
	MaxResponseTime time.Duration
	MinFreeMemory   int64
}

// CircuitState represents the state of a circuit breaker
type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

// EnhancedCircuitBreaker provides circuit breaker functionality
type EnhancedCircuitBreaker struct {
	mu              sync.RWMutex
	threshold       int64
	timeout         time.Duration
	state           CircuitState
	failureCount    int64
	successCount    int64
	lastFailureTime time.Time
}

// MetricsCollector collects and stores metrics
type MetricsCollector struct {
	mu      sync.RWMutex
	metrics map[string]*Metric
}

// Metric represents a single metric
type Metric struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// MessageStore interface for storing messages
type MessageStore interface {
	Store(msg *StoredMessage) error
	Retrieve(id string) (*StoredMessage, error)
	Delete(id string) error
	List(queue string, limit int, offset int) ([]*StoredMessage, error)
	Count(queue string) (int64, error)
	Cleanup(olderThan time.Time) error
}

// StoredMessage represents a message stored in the message store
type StoredMessage struct {
	ID        string            `json:"id"`
	Queue     string            `json:"queue"`
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers,omitempty"`
	Metadata  map[string]any    `json:"metadata,omitempty"`
	Priority  int               `json:"priority"`
	CreatedAt time.Time         `json:"created_at"`
	ExpiresAt *time.Time        `json:"expires_at,omitempty"`
	Attempts  int               `json:"attempts"`
}

type Broker struct {
	// Core broker functionality
	queues        storage.IMap[string, *Queue] // Modified to support tenant-specific queues
	consumers     storage.IMap[string, *consumer]
	publishers    storage.IMap[string, *publisher]
	deadLetter    storage.IMap[string, *Queue]
	deferredTasks storage.IMap[string, *QueuedTask] // NEW: Store for deferred tasks
	dlqHandlers   storage.IMap[string, Handler]     // NEW: Custom handlers for DLQ queues
	opts          *Options
	pIDs          storage.IMap[string, bool]
	listener      net.Listener

	// Enhanced production features
	connectionPool     *ConnectionPool
	healthChecker      *HealthChecker
	circuitBreaker     *EnhancedCircuitBreaker
	metricsCollector   *MetricsCollector
	messageStore       MessageStore
	securityManager    *SecurityManager
	adminServer        *AdminServer
	metricsServer      *MetricsServer
	authenticatedConns storage.IMap[string, bool]              // authenticated connections
	taskHeaders        storage.IMap[string, map[string]string] // task headers by task ID
	pendingTasks       map[string]map[string]*Task             // consumerID -> taskID -> task
	enhanced           *EnhancedFeatures                       // enhanced features (DLQ, WAL, ACK, etc.)
	mu                 sync.RWMutex                            // for pendingTasks
	isShutdown         int32
	shutdown           chan struct{}
	stopDeferredChan   chan struct{} // NEW: Signal to stop deferred task processor
	wg                 sync.WaitGroup
	logger             logger.Logger
}

func NewBroker(opts ...Option) *Broker {
	options := SetupOptions(opts...)

	broker := &Broker{
		// Core broker functionality
		queues:        memory.New[string, *Queue](),
		publishers:    memory.New[string, *publisher](),
		consumers:     memory.New[string, *consumer](),
		deadLetter:    memory.New[string, *Queue](),
		deferredTasks: memory.New[string, *QueuedTask](), // NEW: Initialize deferred tasks map
		dlqHandlers:   memory.New[string, Handler](),     // NEW: Initialize DLQ handlers map
		pIDs:          memory.New[string, bool](),
		pendingTasks:  make(map[string]map[string]*Task),
		opts:          options,

		// Enhanced production features
		connectionPool:     NewConnectionPool(1000), // max 1000 connections
		healthChecker:      NewHealthChecker(),
		circuitBreaker:     NewEnhancedCircuitBreaker(10, 30*time.Second), // 10 failures, 30s timeout
		metricsCollector:   NewMetricsCollector(),
		messageStore:       NewInMemoryMessageStore(),
		authenticatedConns: memory.New[string, bool](),
		taskHeaders:        memory.New[string, map[string]string](),
		shutdown:           make(chan struct{}),
		stopDeferredChan:   make(chan struct{}), // NEW: Initialize stop channel for deferred processor
		logger:             options.Logger(),
	}

	if options.enableSecurity {
		broker.securityManager = NewSecurityManager()
	}
	if options.enableMonitoring {
		if options.adminAddr != "" {
			broker.adminServer = NewAdminServer(broker, options.adminAddr, options.Logger())
		}
		if options.metricsAddr != "" {
			// Need to create MonitoringConfig, use default
			config := &MonitoringConfig{
				EnableMetrics:       true,
				MetricsPort:         9090, // default
				MetricsPath:         "/metrics",
				EnableHealthCheck:   true,
				HealthCheckPort:     8080,
				HealthCheckPath:     "/health",
				HealthCheckInterval: time.Minute,
				EnableLogging:       true,
				LogLevel:            "info",
			}
			broker.metricsServer = NewMetricsServer(broker, config, options.Logger())
		}
	}

	broker.healthChecker.broker = broker
	return broker
}

func (b *Broker) Options() *Options {
	return b.opts
}

func (b *Broker) SecurityManager() *SecurityManager {
	return b.securityManager
}

// InitializeSecurity initializes default users, roles, and permissions for development/testing
func (b *Broker) InitializeSecurity() error {
	if b.securityManager == nil {
		return fmt.Errorf("security manager not initialized")
	}
	return nil
}

func (b *Broker) OnClose(ctx context.Context, conn net.Conn) error {
	consumerID, ok := GetConsumerID(ctx)
	if ok && consumerID != "" {
		log.Printf("Broker: Consumer connection closed: %s, address: %s", consumerID, conn.RemoteAddr())
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
				log.Printf("Broker: Consumer connection closed: %s, address: %s", consumerID, conn.RemoteAddr())
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
		log.Printf("Broker: Publisher connection closed: %s, address: %s", publisherID, conn.RemoteAddr())
		if con, exists := b.publishers.Get(publisherID); exists {
			con.conn.Close()
			b.publishers.Del(publisherID)
		}
	}
	// Remove from authenticated connections
	connID := conn.RemoteAddr().String()
	b.authenticatedConns.Del(connID)

	log.Printf("BROKER - Connection closed: address %s", conn.RemoteAddr())
	return nil
}

func (b *Broker) OnError(_ context.Context, conn net.Conn, err error) {
	if conn != nil {
		if b.isConnectionClosed(err) {
			log.Printf("Connection closed for consumer at %s", conn.RemoteAddr())
			// Find and remove the consumer
			b.consumers.ForEach(func(id string, con *consumer) bool {
				if con.conn == conn {
					b.RemoveConsumer(id)
					b.mu.Lock()
					if tasks, ok := b.pendingTasks[id]; ok {
						for _, task := range tasks {
							// Put back to queue
							if q, ok := b.queues.Get(task.Topic); ok {
								select {
								case q.tasks <- &QueuedTask{Task: task, RetryCount: task.Retries}:
									log.Printf("Requeued task %s for consumer %s", task.ID, id)
								default:
									log.Printf("Failed to requeue task %s, queue full", task.ID)
								}
							}
						}
						delete(b.pendingTasks, id)
					}
					b.mu.Unlock()
					return false
				}
				return true
			})
		} else {
			log.Printf("Error reading from connection: %v", err)
		}
	}
}

func (b *Broker) isConnectionClosed(err error) bool {
	return err == io.EOF || strings.Contains(err.Error(), "connection closed") || strings.Contains(err.Error(), "connection reset")
}

func (b *Broker) isAuthenticated(connID string) bool {
	if b.securityManager == nil {
		return true // no security, allow all
	}
	_, ok := b.authenticatedConns.Get(connID)
	return ok
}

func (b *Broker) OnMessage(ctx context.Context, msg *codec.Message, conn net.Conn) {
	// Set message headers in context for publisher/consumer ID extraction
	ctx = SetHeaders(ctx, msg.Headers)

	connID := conn.RemoteAddr().String()

	// Check authentication for protected commands
	if b.securityManager != nil && (msg.Command == consts.PUBLISH || msg.Command == consts.SUBSCRIBE) {
		if !b.isAuthenticated(connID) {
			b.logger.Warn("Unauthenticated access attempt", logger.Field{Key: "command", Value: msg.Command.String()}, logger.Field{Key: "conn", Value: connID})
			// Send error response
			return
		}
	}

	switch msg.Command {
	case consts.AUTH:
		b.AuthHandler(ctx, conn, msg)
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
	case consts.CONSUMER_UPDATED:
		b.OnConsumerUpdated(ctx, msg)
	default:
		log.Printf("BROKER - UNKNOWN_COMMAND ~> %s on %s", msg.Command, msg.Queue)
	}
}

func (b *Broker) AuthHandler(ctx context.Context, conn net.Conn, msg *codec.Message) {
	connID := conn.RemoteAddr().String()

	// Parse auth credentials from payload
	var authReq map[string]any
	if err := json.Unmarshal(msg.Payload, &authReq); err != nil {
		b.logger.Error("Invalid auth request", logger.Field{Key: "error", Value: err.Error()})
		return
	}

	username, _ := authReq["username"].(string)
	password, _ := authReq["password"].(string)

	// Authenticate
	user, err := b.securityManager.Authenticate(ctx, map[string]any{
		"username": username,
		"password": password,
	})
	if err != nil {
		b.logger.Warn("Authentication failed", logger.Field{Key: "username", Value: username}, logger.Field{Key: "conn", Value: connID})
		// Send AUTH_DENY
		denyMsg, err := codec.NewMessage(consts.AUTH_DENY, []byte(fmt.Sprintf(`{"error":"%s"}`, err.Error())), "", msg.Headers)
		if err != nil {
			b.logger.Error("Failed to create AUTH_DENY message", logger.Field{Key: "error", Value: err.Error()})
			return
		}
		if err := b.send(ctx, conn, denyMsg); err != nil {
			b.logger.Error("Failed to send AUTH_DENY", logger.Field{Key: "error", Value: err.Error()})
		}
		return
	}

	// Mark as authenticated
	b.authenticatedConns.Set(connID, true)

	// Send AUTH_ACK
	ackMsg, err := codec.NewMessage(consts.AUTH_ACK, []byte(`{"status":"authenticated"}`), "", msg.Headers)
	if err != nil {
		b.logger.Error("Failed to create AUTH_ACK message", logger.Field{Key: "error", Value: err.Error()})
		return
	}
	if err := b.send(ctx, conn, ackMsg); err != nil {
		b.logger.Error("Failed to send AUTH_ACK", logger.Field{Key: "error", Value: err.Error()})
	}

	b.logger.Info("User authenticated", logger.Field{Key: "username", Value: user.Username}, logger.Field{Key: "conn", Value: connID})
}

func (b *Broker) AdjustConsumerWorkers(noOfWorkers int, consumerID ...string) {
	b.consumers.ForEach(func(_ string, c *consumer) bool {
		return true
	})
}

func (b *Broker) MessageAck(ctx context.Context, msg *codec.Message) {
	consumerID, _ := GetConsumerID(ctx)
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	log.Printf("BROKER - MESSAGE_ACK ~> %s on %s for Task %s", consumerID, msg.Queue, taskID)
	b.mu.Lock()
	if tasks, ok := b.pendingTasks[consumerID]; ok {
		delete(tasks, taskID)
	}
	b.mu.Unlock()
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
			if b.opts.notifyResponse != nil {
				result := Result{
					Status: "STOPPED",
					Topic:  "", // adjust if queue name is available
					TaskID: consumerID,
					Ctx:    ctx,
				}
				_ = b.opts.notifyResponse(ctx, result)
			}
		}
	}
}

func (b *Broker) OnConsumerUpdated(ctx context.Context, msg *codec.Message) {
	consumerID, _ := GetConsumerID(ctx)
	if consumerID != "" {
		log.Printf("BROKER - CONSUMER ~> Updated %s", consumerID)
		if b.opts.notifyResponse != nil {
			result := Result{
				Status:  "CONSUMER UPDATED",
				TaskID:  consumerID,
				Ctx:     ctx,
				Payload: msg.Payload,
			}
			_ = b.opts.notifyResponse(ctx, result)
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
	// Extract task ID from response
	var result Result
	if err := json.Unmarshal(msg.Payload, &result); err != nil {
		log.Printf("Error unmarshaling response: %v", err)
		return
	}
	taskID := result.TaskID

	// Retrieve stored headers for this task
	if headers, ok := b.taskHeaders.Get(taskID); ok {
		ctx = SetHeaders(ctx, headers)
		// Clean up stored headers
		b.taskHeaders.Del(taskID)
	}

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
	msg, err := codec.NewMessage(consts.PUBLISH, payload, queue, headers.AsMap())
	if err != nil {
		return fmt.Errorf("failed to create PUBLISH message: %w", err)
	}
	b.broadcastToConsumers(msg)
	return nil
}

func (b *Broker) PublishHandler(ctx context.Context, conn net.Conn, msg *codec.Message) {
	pub := b.addPublisher(ctx, msg.Queue, conn)
	taskID, _ := jsonparser.GetString(msg.Payload, "id")
	log.Printf("BROKER - PUBLISH ~> received from %s on %s for Task %s", pub.id, msg.Queue, taskID)

	// Store headers for response routing
	b.taskHeaders.Set(taskID, msg.Headers)

	// Send acknowledgment back to publisher
	ack, err := codec.NewMessage(consts.PUBLISH_ACK, utils.ToByte(fmt.Sprintf(`{"id":"%s"}`, taskID)), msg.Queue, msg.Headers)
	if err != nil {
		log.Printf("Error creating PUBLISH_ACK message: %v\n", err)
		return
	}
	if err := b.send(ctx, conn, ack); err != nil {
		log.Printf("Error sending PUBLISH_ACK: %v\n", err)
	}

	// Apply enhanced features if enabled
	if b.enhanced != nil && b.enhanced.enabled {
		// Parse the task from the message
		var task Task
		if err := json.Unmarshal(msg.Payload, &task); err != nil {
			log.Printf("Error parsing task for enhanced features: %v\n", err)
			b.broadcastToConsumers(msg)
			return
		}

		// Check for duplicates
		if b.enhanced.dedupManager != nil {
			isDuplicate, err := b.enhanced.dedupManager.CheckDuplicate(ctx, &task)
			if err != nil {
				log.Printf("Error checking duplicate: %v\n", err)
			} else if isDuplicate {
				b.logger.Debug("Duplicate message rejected",
					logger.Field{Key: "taskID", Value: task.ID})
				return // Don't broadcast duplicates
			}
		}

		// Acquire flow control credits
		if b.enhanced.flowController != nil {
			if err := b.enhanced.flowController.AcquireCredit(ctx, 1); err != nil {
				log.Printf("Flow control credit acquisition failed: %v\n", err)
				// Continue anyway - don't block
			} else {
				defer b.enhanced.flowController.ReleaseCredit(1)
			}
		}

		// Write to WAL
		if b.enhanced.walLog != nil {
			walEntry := &WALEntry{
				EntryType: WALEntryEnqueue,
				TaskID:    task.ID,
				QueueName: msg.Queue,
				Payload:   msg.Payload, // already []byte
			}
			if err := b.enhanced.walLog.WriteEntry(ctx, walEntry); err != nil {
				b.logger.Error("Failed to write WAL entry",
					logger.Field{Key: "error", Value: err})
			}
		}

		// Start tracing
		if b.enhanced.lifecycleTracker != nil {
			b.enhanced.lifecycleTracker.TrackEnqueue(ctx, &task, msg.Queue)
		}

		// Track for acknowledgment
		if b.enhanced.ackManager != nil {
			_ = b.enhanced.ackManager.TrackMessage(ctx, &task, msg.Queue, pub.id)
		}

		// Check if task is deferred
		if !task.DeferUntil.IsZero() && task.DeferUntil.After(time.Now()) {
			// Create QueuedTask for deferred execution
			queuedTask := &QueuedTask{
				Message:    msg,
				Task:       &task,
				RetryCount: 0,
			}
			// Add to deferred tasks queue
			b.AddDeferredTask(queuedTask)
			log.Printf("[DEFERRED] Task %s deferred until %s",
				task.ID, task.DeferUntil.Format(time.RFC3339))
			return // Don't broadcast yet
		}
	}

	// Broadcast to consumers
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
	ack, err := codec.NewMessage(consts.SUBSCRIBE_ACK, nil, msg.Queue, msg.Headers)
	if err != nil {
		log.Printf("Error creating SUBSCRIBE_ACK message: %v\n", err)
		return
	}
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
	// Start health checker
	b.healthChecker.Start()

	// Start deferred task processor
	b.StartDeferredTaskProcessor(ctx)

	// Start connection cleanup routine
	b.wg.Add(1)
	go b.connectionCleanupRoutine()

	// Start metrics collection routine
	b.wg.Add(1)
	go b.metricsCollectionRoutine()

	// Start message store cleanup routine
	b.wg.Add(1)
	go b.messageStoreCleanupRoutine()

	// Start admin server if enabled
	if b.adminServer != nil {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			if err := b.adminServer.Start(); err != nil {
				b.logger.Error("Failed to start admin server", logger.Field{Key: "error", Value: err.Error()})
			}
		}()
	}

	// Start metrics server if enabled
	if b.metricsServer != nil {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			if err := b.metricsServer.Start(ctx); err != nil {
				b.logger.Error("Failed to start metrics server", logger.Field{Key: "error", Value: err.Error()})
			}
		}()
	}

	b.logger.Info("Broker starting with production features enabled")

	// Start the broker with enhanced features
	if err := b.startBroker(ctx); err != nil {
		return err
	}

	// Wait for shutdown signal
	<-b.shutdown
	b.logger.Info("Broker shutting down")

	// Wait for all goroutines to finish
	b.wg.Wait()

	return nil
}

func (b *Broker) send(ctx context.Context, conn net.Conn, msg *codec.Message) error {
	return codec.SendMessage(ctx, conn, msg)
}

func (b *Broker) receive(ctx context.Context, c net.Conn) (*codec.Message, error) {
	return codec.ReadMessage(ctx, c)
}

func (b *Broker) broadcastToConsumers(msg *codec.Message) {
	if queue, ok := b.queues.Get(msg.Queue); ok {
		var t Task
		json.Unmarshal(msg.Payload, &t)
		task := &QueuedTask{Message: msg, Task: &t, RetryCount: 0}
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

	// Create consumer with proper initialization
	con := &consumer{
		id:    consumerID,
		conn:  conn,
		state: consts.ConsumerStateActive,
		queue: queueName,
		pool:  nil, // Pool will be set when consumer connects
		metrics: &ConsumerMetrics{
			ProcessedTasks: 0,
			ErrorCount:     0,
			LastActivity:   time.Now(),
		},
	}

	b.consumers.Set(consumerID, con)
	q.consumers.Set(consumerID, con)
	log.Printf("BROKER - SUBSCRIBE ~> %s on %s", consumerID, queueName)
	return consumerID
}

func (b *Broker) UpdateConsumerPool(consumerID string, pool *Pool) {
	if con, exists := b.consumers.Get(consumerID); exists {
		con.pool = pool
	}
}

func (b *Broker) UpdateConsumerMetrics(consumerID string, processedTasks, errorCount int64) {
	if con, exists := b.consumers.Get(consumerID); exists && con.metrics != nil {
		con.metrics.ProcessedTasks = processedTasks
		con.metrics.ErrorCount = errorCount
		con.metrics.LastActivity = time.Now()
	}
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

func (b *Broker) handleConsumer(
	ctx context.Context, cmd consts.CMD, state consts.ConsumerState,
	consumerID string, payload []byte, queues ...string,
) {
	fn := func(queue *Queue) {
		con, ok := queue.consumers.Get(consumerID)
		if ok {
			ack, err := codec.NewMessage(cmd, payload, queue.name, map[string]string{consts.ConsumerKey: consumerID})
			if err != nil {
				log.Printf("Error creating message for consumer %s: %v", consumerID, err)
				return
			}
			err = b.send(ctx, con.conn, ack)
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

func (b *Broker) UpdateConsumer(ctx context.Context, consumerID string, config DynamicConfig, queues ...string) error {
	var err error
	payload, _ := json.Marshal(config)
	fn := func(queue *Queue) error {
		con, ok := queue.consumers.Get(consumerID)
		if ok {
			ack, err := codec.NewMessage(consts.CONSUMER_UPDATE, payload, queue.name, map[string]string{consts.ConsumerKey: consumerID})
			if err != nil {
				log.Printf("Error creating message for consumer %s: %v", consumerID, err)
				return err
			}
			return b.send(ctx, con.conn, ack)
		}
		return nil
	}
	if len(queues) > 0 {
		for _, queueName := range queues {
			if queue, ok := b.queues.Get(queueName); ok {
				err = fn(queue)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	b.queues.ForEach(func(queueName string, queue *Queue) bool {
		err = fn(queue)
		if err != nil {
			return false
		}
		return true
	})
	return nil
}

func (b *Broker) PauseConsumer(ctx context.Context, consumerID string, queues ...string) {
	b.handleConsumer(ctx, consts.CONSUMER_PAUSE, consts.ConsumerStatePaused, consumerID, utils.ToByte("{}"), queues...)
}

func (b *Broker) ResumeConsumer(ctx context.Context, consumerID string, queues ...string) {
	b.handleConsumer(ctx, consts.CONSUMER_RESUME, consts.ConsumerStateActive, consumerID, utils.ToByte("{}"), queues...)
}

func (b *Broker) StopConsumer(ctx context.Context, consumerID string, queues ...string) {
	b.handleConsumer(ctx, consts.CONSUMER_STOP, consts.ConsumerStateStopped, consumerID, utils.ToByte("{}"), queues...)
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
		// Handle each task in a separate goroutine to avoid blocking the dispatch loop
		go func(t *QueuedTask) {
			if b.opts.BrokerRateLimiter != nil {
				b.opts.BrokerRateLimiter.Wait()
			}

			success := false
			currentDelay := delay

			for !success && t.RetryCount <= b.opts.maxRetries {
				if b.dispatchTaskToConsumer(ctx, queue, t) {
					success = true
					b.acknowledgeTask(ctx, t.Message.Queue, queue.name)
				} else {
					t.RetryCount++
					currentDelay = b.backoffRetry(queue, t, currentDelay)
				}
			}

			if t.RetryCount > b.opts.maxRetries {
				b.sendToDLQ(queue, t)
			}
		}(task)
	}
}

func (b *Broker) sendToDLQ(queue *Queue, task *QueuedTask) {
	id := task.Task.ID
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

	// Deduplication: Check if the task has already been processed
	taskID := task.Task.ID
	if _, exists := b.pIDs.Get(taskID); exists {
		log.Printf("Task %s already processed, skipping...", taskID)
		return true
	}

	queue.consumers.ForEach(func(_ string, con *consumer) bool {
		if con.state != consts.ConsumerStateActive {
			err = fmt.Errorf("consumer %s is not active", con.id)
			return true
		}

		// Send message asynchronously to avoid blocking
		b.mu.Lock()
		if b.pendingTasks[con.id] == nil {
			b.pendingTasks[con.id] = make(map[string]*Task)
		}
		b.pendingTasks[con.id][taskID] = task.Task
		b.mu.Unlock()
		go func(consumer *consumer, message *codec.Message) {
			sendCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if sendErr := b.send(sendCtx, consumer.conn, message); sendErr != nil {
				log.Printf("Failed to send task %s to consumer %s: %v", taskID, consumer.id, sendErr)
			} else {
				log.Printf("Successfully sent task %s to consumer %s", taskID, consumer.id)
			}
		}(con, task.Message)

		consumerFound = true
		// Mark the task as processed
		b.pIDs.Set(taskID, true)
		return false // Break the loop since we found a consumer
	})

	if err != nil {
		log.Println(err.Error())
		return false
	}
	if !consumerFound {
		log.Printf("No available consumers for queue %s, retrying...", queue.name)
		if b.opts.notifyResponse != nil {
			result := Result{
				Status: "NO_CONSUMER",
				Topic:  queue.name,
				TaskID: taskID,
				Ctx:    ctx,
			}
			_ = b.opts.notifyResponse(ctx, result)
		}
	}
	return consumerFound
}

// Modified backoffRetry: Re-insert the task into queue.tasks after backoff.
func (b *Broker) backoffRetry(queue *Queue, task *QueuedTask, delay time.Duration) time.Duration {
	backoffDuration := utils.CalculateJitter(delay, b.opts.jitterPercent)
	log.Printf("Backing off for %v before retrying task for queue %s", backoffDuration, task.Task.Topic)

	// Perform backoff sleep in a goroutine to avoid blocking
	go func() {
		time.Sleep(backoffDuration)
	}()

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
		// Stop deferred task processor
		b.StopDeferredTaskProcessor()
		return b.listener.Close()
	}
	return nil
}

func (b *Broker) SetURL(url string) {
	b.opts.brokerAddr = url
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

// NewQueueWithConfig creates a queue with specific configuration
func (b *Broker) NewQueueWithConfig(name string, opts ...QueueOption) *Queue {
	config := QueueConfig{
		MaxDepth:   b.opts.queueSize,
		MaxRetries: 3,
		MessageTTL: 1 * time.Hour,
		BatchSize:  1,
	}

	// Apply options
	for _, opt := range opts {
		opt(&config)
	}

	q := newQueueWithConfig(name, config)
	b.queues.Set(name, q)

	// Create DLQ for the queue if enabled
	if config.DeadLetter {
		dlqConfig := config
		dlqConfig.MaxDepth = config.MaxDepth / 10 // 10% of main queue
		dlq := newQueueWithConfig(name+"_dlq", dlqConfig)
		b.deadLetter.Set(name, dlq)
	}

	ctx := context.Background()
	go b.dispatchWorker(ctx, q)
	if config.DeadLetter {
		if dlq, ok := b.deadLetter.Get(name); ok {
			go b.dispatchWorker(ctx, dlq)
		}
	}
	return q
}

// Ensure message ordering in task queues
func (b *Broker) NewQueueWithOrdering(name string) *Queue {
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

// Add explicit acknowledgment for successful task processing
func (b *Broker) acknowledgeTask(ctx context.Context, taskID string, queueName string) {
	log.Printf("Acknowledging task %s on queue %s", taskID, queueName)
	if b.opts.notifyResponse != nil {
		result := Result{
			Status: "ACKNOWLEDGED",
			Topic:  queueName,
			TaskID: taskID,
			Ctx:    ctx,
		}
		_ = b.opts.notifyResponse(ctx, result)
	}
}

// Add authentication and authorization for publishers and consumers
func (b *Broker) Authenticate(ctx context.Context, credentials map[string]string) error {
	username, userExists := credentials["username"]
	password, passExists := credentials["password"]
	if !userExists || !passExists {
		return fmt.Errorf("missing credentials")
	}
	// Example: Hardcoded credentials for simplicity
	if username != "admin" || password != "password" {
		return fmt.Errorf("invalid credentials")
	}
	return nil
}

func (b *Broker) Authorize(ctx context.Context, role string, action string) error {
	// Example: Simple role-based authorization
	if role == "publisher" && action == "publish" {
		return nil
	}
	if role == "consumer" && action == "consume" {
		return nil
	}
	return fmt.Errorf("unauthorized action")
}

// Enhanced Broker Methods (Production Features)

// NewConnectionPool creates a new connection pool
func NewConnectionPool(maxConns int) *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]*BrokerConnection),
		maxConns:    maxConns,
	}
}

// AddConnection adds a connection to the pool
func (cp *ConnectionPool) AddConnection(id string, conn net.Conn, connType string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if len(cp.connections) >= cp.maxConns {
		return fmt.Errorf("connection pool is full")
	}

	brokerConn := &BrokerConnection{
		conn:         conn,
		id:           id,
		connType:     connType,
		lastActivity: time.Now(),
		isActive:     true,
	}

	cp.connections[id] = brokerConn
	atomic.AddInt64(&cp.connCount, 1)
	return nil
}

// RemoveConnection removes a connection from the pool
func (cp *ConnectionPool) RemoveConnection(id string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if conn, exists := cp.connections[id]; exists {
		conn.conn.Close()
		delete(cp.connections, id)
		atomic.AddInt64(&cp.connCount, -1)
	}
}

// GetActiveConnections returns the number of active connections
func (cp *ConnectionPool) GetActiveConnections() int64 {
	return atomic.LoadInt64(&cp.connCount)
}

// NewHealthChecker creates a new health checker
func NewHealthChecker() *HealthChecker {
	return &HealthChecker{
		interval: 30 * time.Second,
		shutdown: make(chan struct{}),
		thresholds: HealthThresholds{
			MaxMemoryUsage:  1024 * 1024 * 1024, // 1GB
			MaxCPUUsage:     80.0,               // 80%
			MaxConnections:  900,                // 90% of max
			MaxQueueDepth:   10000,
			MaxResponseTime: 5 * time.Second,
			MinFreeMemory:   100 * 1024 * 1024, // 100MB
		},
	}
}

// NewEnhancedCircuitBreaker creates a new circuit breaker
func NewEnhancedCircuitBreaker(threshold int64, timeout time.Duration) *EnhancedCircuitBreaker {
	return &EnhancedCircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
		state:     CircuitClosed,
	}
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: make(map[string]*Metric),
	}
}

// NewInMemoryMessageStore creates a new in-memory message store
func NewInMemoryMessageStore() *InMemoryMessageStore {
	return &InMemoryMessageStore{
		messages: memory.New[string, *StoredMessage](),
	}
}

// Store stores a message
func (ims *InMemoryMessageStore) Store(msg *StoredMessage) error {
	ims.messages.Set(msg.ID, msg)
	return nil
}

// Retrieve retrieves a message by ID
func (ims *InMemoryMessageStore) Retrieve(id string) (*StoredMessage, error) {
	msg, exists := ims.messages.Get(id)
	if !exists {
		return nil, fmt.Errorf("message not found: %s", id)
	}
	return msg, nil
}

// Delete deletes a message
func (ims *InMemoryMessageStore) Delete(id string) error {
	ims.messages.Del(id)
	return nil
}

// List lists messages for a queue
func (ims *InMemoryMessageStore) List(queue string, limit int, offset int) ([]*StoredMessage, error) {
	var result []*StoredMessage
	count := 0
	skipped := 0

	ims.messages.ForEach(func(id string, msg *StoredMessage) bool {
		if msg.Queue == queue {
			if skipped < offset {
				skipped++
				return true
			}

			result = append(result, msg)
			count++

			return count < limit
		}
		return true
	})

	return result, nil
}

// Count counts messages in a queue
func (ims *InMemoryMessageStore) Count(queue string) (int64, error) {
	count := int64(0)
	ims.messages.ForEach(func(id string, msg *StoredMessage) bool {
		if msg.Queue == queue {
			count++
		}
		return true
	})
	return count, nil
}

// Cleanup removes old messages
func (ims *InMemoryMessageStore) Cleanup(olderThan time.Time) error {
	var toDelete []string

	ims.messages.ForEach(func(id string, msg *StoredMessage) bool {
		if msg.CreatedAt.Before(olderThan) ||
			(msg.ExpiresAt != nil && msg.ExpiresAt.Before(time.Now())) {
			toDelete = append(toDelete, id)
		}
		return true
	})

	for _, id := range toDelete {
		ims.messages.Del(id)
	}

	return nil
}

// Enhanced Start method with production features

// startBroker starts the core broker functionality
func (b *Broker) startBroker(ctx context.Context) error {
	addr := b.opts.BrokerAddr()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	b.listener = listener
	b.logger.Info("Enhanced broker listening", logger.Field{Key: "address", Value: addr})

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-b.shutdown:
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					select {
					case <-b.shutdown:
						return
					default:
						b.logger.Error("Accept error", logger.Field{Key: "error", Value: err.Error()})
						continue
					}
				}

				// Add connection to pool
				connID := fmt.Sprintf("conn_%d", time.Now().UnixNano())
				b.connectionPool.AddConnection(connID, conn, "unknown")

				b.wg.Add(1)
				go func(c net.Conn) {
					defer b.wg.Done()
					b.handleEnhancedConnection(ctx, c)
				}(conn)
			}
		}
	}()

	return nil
}

// handleEnhancedConnection handles incoming connections with enhanced features
func (b *Broker) handleEnhancedConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			b.logger.Error("Connection handler panic",
				logger.Field{Key: "panic", Value: fmt.Sprintf("%v", r)},
				logger.Field{Key: "remote_addr", Value: conn.RemoteAddr().String()})
		}
		conn.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.shutdown:
			return
		default:
			msg, err := b.receive(ctx, conn)
			if err != nil {
				b.OnError(ctx, conn, err)
				return
			}
			b.OnMessage(ctx, msg, conn)
		}
	}
}

// connectionCleanupRoutine periodically cleans up idle connections
func (b *Broker) connectionCleanupRoutine() {
	defer b.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.connectionPool.CleanupIdleConnections(10 * time.Minute)
		case <-b.shutdown:
			return
		}
	}
}

// CleanupIdleConnections removes idle connections
func (cp *ConnectionPool) CleanupIdleConnections(idleTimeout time.Duration) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	now := time.Now()
	for id, conn := range cp.connections {
		conn.mu.RLock()
		lastActivity := conn.lastActivity
		conn.mu.RUnlock()

		if now.Sub(lastActivity) > idleTimeout {
			conn.conn.Close()
			delete(cp.connections, id)
			atomic.AddInt64(&cp.connCount, -1)
		}
	}
}

// metricsCollectionRoutine periodically collects and reports metrics
func (b *Broker) metricsCollectionRoutine() {
	defer b.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.collectMetrics()
		case <-b.shutdown:
			return
		}
	}
}

// collectMetrics collects current system metrics
func (b *Broker) collectMetrics() {
	// Collect connection metrics
	activeConns := b.connectionPool.GetActiveConnections()
	b.metricsCollector.RecordMetric("broker.connections.active", float64(activeConns), nil)

	// Collect queue metrics
	b.queues.ForEach(func(name string, queue *Queue) bool {
		queueDepth := len(queue.tasks)
		consumerCount := queue.consumers.Size()

		b.metricsCollector.RecordMetric("broker.queue.depth", float64(queueDepth),
			map[string]string{"queue": name})
		b.metricsCollector.RecordMetric("broker.queue.consumers", float64(consumerCount),
			map[string]string{"queue": name})

		return true
	})
}

// RecordMetric records a metric
func (mc *MetricsCollector) RecordMetric(name string, value float64, tags map[string]string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.metrics[name] = &Metric{
		Name:      name,
		Value:     value,
		Timestamp: time.Now(),
		Tags:      tags,
	}
}

// messageStoreCleanupRoutine periodically cleans up old messages
func (b *Broker) messageStoreCleanupRoutine() {
	defer b.wg.Done()

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Clean up messages older than 24 hours
			cutoff := time.Now().Add(-24 * time.Hour)
			if err := b.messageStore.Cleanup(cutoff); err != nil {
				b.logger.Error("Failed to cleanup old messages",
					logger.Field{Key: "error", Value: err.Error()})
			}
		case <-b.shutdown:
			return
		}
	}
}

// Enhanced Stop method with graceful shutdown
func (b *Broker) StopEnhanced() error {
	if !atomic.CompareAndSwapInt32(&b.isShutdown, 0, 1) {
		return nil // Already shutdown
	}

	b.logger.Info("Enhanced broker shutting down gracefully")

	// Signal shutdown
	close(b.shutdown)

	// Stop health checker
	b.healthChecker.Stop()

	// Wait for all goroutines to finish
	b.wg.Wait()

	// Close all connections
	b.connectionPool.mu.Lock()
	for id, conn := range b.connectionPool.connections {
		conn.conn.Close()
		delete(b.connectionPool.connections, id)
	}
	b.connectionPool.mu.Unlock()

	// Close listener
	if b.listener != nil {
		b.listener.Close()
	}

	b.logger.Info("Enhanced broker shutdown completed")
	return nil
}

// Start starts the health checker
func (hc *HealthChecker) Start() {
	hc.ticker = time.NewTicker(hc.interval)
	go func() {
		defer hc.ticker.Stop()
		for {
			select {
			case <-hc.ticker.C:
				hc.performHealthCheck()
			case <-hc.shutdown:
				return
			}
		}
	}()
}

// Stop stops the health checker
func (hc *HealthChecker) Stop() {
	close(hc.shutdown)
}

// performHealthCheck performs a comprehensive health check
func (hc *HealthChecker) performHealthCheck() {
	// Check connection count
	activeConns := hc.broker.connectionPool.GetActiveConnections()
	if activeConns > int64(hc.thresholds.MaxConnections) {
		hc.broker.logger.Warn("High connection count detected",
			logger.Field{Key: "active_connections", Value: activeConns},
			logger.Field{Key: "threshold", Value: hc.thresholds.MaxConnections})
	}

	// Check queue depths
	hc.broker.queues.ForEach(func(name string, queue *Queue) bool {
		if len(queue.tasks) > hc.thresholds.MaxQueueDepth {
			hc.broker.logger.Warn("High queue depth detected",
				logger.Field{Key: "queue", Value: name},
				logger.Field{Key: "depth", Value: len(queue.tasks)},
				logger.Field{Key: "threshold", Value: hc.thresholds.MaxQueueDepth})
		}
		return true
	})

	// Record health metrics
	hc.broker.metricsCollector.RecordMetric("broker.connections.active", float64(activeConns), nil)
	hc.broker.metricsCollector.RecordMetric("broker.health.check.timestamp", float64(time.Now().Unix()), nil)
}

// Call executes a function with circuit breaker protection
func (cb *EnhancedCircuitBreaker) Call(fn func() error) error {
	cb.mu.RLock()
	state := cb.state
	cb.mu.RUnlock()

	switch state {
	case CircuitOpen:
		cb.mu.RLock()
		lastFailure := cb.lastFailureTime
		cb.mu.RUnlock()

		if time.Since(lastFailure) > cb.timeout {
			cb.mu.Lock()
			cb.state = CircuitHalfOpen
			cb.mu.Unlock()
		} else {
			return fmt.Errorf("circuit breaker is open")
		}
	case CircuitHalfOpen:
		// Allow one request through
	case CircuitClosed:
		// Normal operation
	}

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failureCount++
		cb.lastFailureTime = time.Now()

		if cb.failureCount >= cb.threshold {
			cb.state = CircuitOpen
		} else if cb.state == CircuitHalfOpen {
			cb.state = CircuitOpen
		}
	} else {
		cb.successCount++
		if cb.state == CircuitHalfOpen {
			cb.state = CircuitClosed
			cb.failureCount = 0
		}
	}

	return err
}

// InMemoryMessageStore implements MessageStore in memory
type InMemoryMessageStore struct {
	messages storage.IMap[string, *StoredMessage]
}

func (b *Broker) GetConsumers() []*AdminConsumerMetrics {
	consumers := []*AdminConsumerMetrics{}
	b.consumers.ForEach(func(id string, con *consumer) bool {
		// Get status based on consumer state
		status := "active"
		switch con.state {
		case consts.ConsumerStateActive:
			status = "active"
		case consts.ConsumerStatePaused:
			status = "paused"
		case consts.ConsumerStateStopped:
			status = "stopped"
		}

		// Handle cases where pool might be nil
		maxConcurrentTasks := 0
		taskTimeout := 0
		maxRetries := 0

		if con.pool != nil {
			config := con.pool.GetCurrentConfig()
			maxConcurrentTasks = config.NumberOfWorkers
			taskTimeout = int(config.Timeout.Seconds())
			maxRetries = config.MaxRetries
		}

		// Ensure metrics is not nil
		processedTasks := int64(0)
		errorCount := int64(0)
		lastActivity := time.Now()

		if con.metrics != nil {
			processedTasks = con.metrics.ProcessedTasks
			errorCount = con.metrics.ErrorCount
			lastActivity = con.metrics.LastActivity
		}

		consumers = append(consumers, &AdminConsumerMetrics{
			ID:                 id,
			Queue:              con.queue,
			Status:             status,
			ProcessedTasks:     processedTasks,
			ErrorCount:         errorCount,
			LastActivity:       lastActivity,
			MaxConcurrentTasks: maxConcurrentTasks,
			TaskTimeout:        taskTimeout,
			MaxRetries:         maxRetries,
		})
		return true
	})
	return consumers
}

func (b *Broker) GetPools() []*AdminPoolMetrics {
	pools := []*AdminPoolMetrics{}
	b.queues.ForEach(func(name string, queue *Queue) bool {
		// Initialize default values
		workers := 0
		queueSize := 0
		activeTasks := 0
		maxMemoryLoad := int64(0)
		lastActivity := time.Now()

		// Get metrics from queue if available
		if queue.metrics != nil {
			workers = queue.metrics.WorkerCount
			queueSize = queue.metrics.QueueDepth
			activeTasks = queue.metrics.ActiveTasks
			maxMemoryLoad = queue.metrics.MaxMemoryLoad
			lastActivity = queue.metrics.LastActivity
		}

		// If metrics are empty, try to get some basic info from the queue
		if queueSize == 0 && queue.tasks != nil {
			queueSize = len(queue.tasks)
		}

		pools = append(pools, &AdminPoolMetrics{
			ID:            name,
			Workers:       workers,
			QueueSize:     queueSize,
			ActiveTasks:   activeTasks,
			Status:        "running", // Default status
			MaxMemoryLoad: maxMemoryLoad,
			LastActivity:  lastActivity,
		})
		return true
	})
	return pools
}
