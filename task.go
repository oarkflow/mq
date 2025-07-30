package mq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

type Queue struct {
	consumers   storage.IMap[string, *consumer]
	tasks       chan *QueuedTask // channel to hold tasks
	name        string
	config      *QueueConfig     // Queue configuration
	deadLetter  chan *QueuedTask // Dead letter queue for failed messages
	rateLimiter *RateLimiter     // Rate limiter for the queue
	metrics     *QueueMetrics    // Queue-specific metrics
	mu          sync.RWMutex     // Mutex for thread safety
}

// QueueMetrics holds metrics for a specific queue
type QueueMetrics struct {
	MessagesReceived  int64         `json:"messages_received"`
	MessagesProcessed int64         `json:"messages_processed"`
	MessagesFailed    int64         `json:"messages_failed"`
	CurrentDepth      int64         `json:"current_depth"`
	AverageLatency    time.Duration `json:"average_latency"`
	LastActivity      time.Time     `json:"last_activity"`
	WorkerCount       int           `json:"worker_count"`
	QueueDepth        int           `json:"queue_depth"`
	ActiveTasks       int           `json:"active_tasks"`
	MaxMemoryLoad     int64         `json:"max_memory_load"`
}

func newQueue(name string, queueSize int) *Queue {
	return &Queue{
		name:      name,
		consumers: memory.New[string, *consumer](),
		tasks:     make(chan *QueuedTask, queueSize), // buffer size for tasks
		config: &QueueConfig{
			MaxDepth:   queueSize,
			MaxRetries: 3,
			MessageTTL: 1 * time.Hour,
			BatchSize:  1,
		},
		deadLetter: make(chan *QueuedTask, queueSize/10), // 10% of main queue size
		metrics:    &QueueMetrics{},
	}
}

// newQueueWithConfig creates a queue with specific configuration
func newQueueWithConfig(name string, config QueueConfig) *Queue {
	queueSize := config.MaxDepth
	if queueSize <= 0 {
		queueSize = 100 // default size
	}

	queue := &Queue{
		name:       name,
		consumers:  memory.New[string, *consumer](),
		tasks:      make(chan *QueuedTask, queueSize),
		config:     &config,
		deadLetter: make(chan *QueuedTask, queueSize/10),
		metrics:    &QueueMetrics{},
	}

	// Set up rate limiter if throttling is enabled
	if config.Throttling && config.ThrottleRate > 0 {
		queue.rateLimiter = NewRateLimiter(config.ThrottleRate, config.ThrottleBurst)
	}

	return queue
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
	Error       error           `json:"-"`               // Don't serialize errors directly
	ErrorMsg    string          `json:"error,omitempty"` // Serialize error message if present
	ID          string          `json:"id"`
	Topic       string          `json:"topic"`
	Status      Status          `json:"status"` // Use Status type instead of string
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority,omitempty"`
	Retries     int             `json:"retries,omitempty"`
	MaxRetries  int             `json:"max_retries,omitempty"`
	dag         any
	// Enhanced deduplication and tracing
	DedupKey string            `json:"dedup_key,omitempty"`
	TraceID  string            `json:"trace_id,omitempty"`
	SpanID   string            `json:"span_id,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
}

func (t *Task) GetFlow() any {
	return t.dag
}

// SetError sets the error and updates the error message
func (t *Task) SetError(err error) {
	t.Error = err
	if err != nil {
		t.ErrorMsg = err.Error()
		t.Status = Failed
	}
}

// GetError returns the error if present
func (t *Task) GetError() error {
	return t.Error
}

// AddTag adds a tag to the task
func (t *Task) AddTag(key, value string) {
	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}
	t.Tags[key] = value
}

// AddHeader adds a header to the task
func (t *Task) AddHeader(key, value string) {
	if t.Headers == nil {
		t.Headers = make(map[string]string)
	}
	t.Headers[key] = value
}

// IsExpired checks if the task has expired
func (t *Task) IsExpired() bool {
	if t.Expiry.IsZero() {
		return false
	}
	return time.Now().After(t.Expiry)
}

// CanRetry checks if the task can be retried
func (t *Task) CanRetry() bool {
	return t.Retries < t.MaxRetries
}

// IncrementRetry increments the retry count
func (t *Task) IncrementRetry() {
	t.Retries++
}

func NewTask(id string, payload json.RawMessage, nodeKey string, opts ...TaskOption) *Task {
	if id == "" {
		id = NewID()
	}
	task := &Task{
		ID:        id,
		Payload:   payload,
		Topic:     nodeKey,
		CreatedAt: time.Now(),
		Status:    Pending,
		TraceID:   NewID(), // Generate unique trace ID
		SpanID:    NewID(), // Generate unique span ID
	}
	for _, opt := range opts {
		opt(task)
	}
	return task
}

// TaskOption for setting priority
func WithPriority(priority int) TaskOption {
	return func(t *Task) {
		t.Priority = priority
	}
}

// TaskOption for setting max retries
func WithTaskMaxRetries(maxRetries int) TaskOption {
	return func(t *Task) {
		t.MaxRetries = maxRetries
	}
}

// TaskOption for setting expiry time
func WithExpiry(expiry time.Time) TaskOption {
	return func(t *Task) {
		t.Expiry = expiry
	}
}

// TaskOption for setting TTL (time to live)
func WithTTL(ttl time.Duration) TaskOption {
	return func(t *Task) {
		t.Expiry = time.Now().Add(ttl)
	}
}

// TaskOption for adding tags
func WithTags(tags map[string]string) TaskOption {
	return func(t *Task) {
		if t.Tags == nil {
			t.Tags = make(map[string]string)
		}
		for k, v := range tags {
			t.Tags[k] = v
		}
	}
}

// TaskOption for adding headers
func WithTaskHeaders(headers map[string]string) TaskOption {
	return func(t *Task) {
		if t.Headers == nil {
			t.Headers = make(map[string]string)
		}
		for k, v := range headers {
			t.Headers[k] = v
		}
	}
}

// TaskOption for setting trace ID
func WithTraceID(traceID string) TaskOption {
	return func(t *Task) {
		t.TraceID = traceID
	}
}

// new TaskOption for deduplication:
func WithDedupKey(key string) TaskOption {
	return func(t *Task) {
		t.DedupKey = key
	}
}

// Add advanced dead-letter queue management
func (b *Broker) ReprocessDLQ(queueName string) error {
	dlqName := queueName + "_dlq"
	dlq, ok := b.deadLetter.Get(dlqName)
	if !ok {
		return fmt.Errorf("dead-letter queue %s does not exist", dlqName)
	}
	for {
		select {
		case task := <-dlq.tasks:
			b.NewQueue(queueName).tasks <- task
		default:
			return nil
		}
	}
}
