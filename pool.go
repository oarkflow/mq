package mq

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/log"

	"github.com/oarkflow/mq/utils"
)

// Callback is called when a task processing is completed.
type Callback func(ctx context.Context, result Result) error

// CompletionCallback is called when the pool completes a graceful shutdown.
type CompletionCallback func()

// Metrics holds cumulative pool metrics.
type Metrics struct {
	TotalTasks           int64 // total number of tasks processed
	CompletedTasks       int64 // number of successfully processed tasks
	ErrorCount           int64 // number of tasks that resulted in error
	TotalMemoryUsed      int64 // current memory used (in bytes) by tasks in flight
	TotalScheduled       int64 // number of tasks scheduled
	ExecutionTime        int64 // cumulative execution time in milliseconds
	CumulativeMemoryUsed int64 // cumulative memory used (sum of all task sizes) in bytes
}

// Plugin is used to inject custom behavior before or after task processing.
type Plugin interface {
	Initialize(config interface{}) error
	BeforeTask(task *QueueTask)
	AfterTask(task *QueueTask, result Result)
}

// DefaultPlugin is a no-op implementation of Plugin.
type DefaultPlugin struct{}

func (dp *DefaultPlugin) Initialize(config interface{}) error { return nil }
func (dp *DefaultPlugin) BeforeTask(task *QueueTask) {
	Logger.Info().Str("taskID", task.payload.ID).Msg("BeforeTask plugin invoked")
}
func (dp *DefaultPlugin) AfterTask(task *QueueTask, result Result) {
	Logger.Info().Str("taskID", task.payload.ID).Msg("AfterTask plugin invoked")
}

// DeadLetterQueue stores tasks that have permanently failed with enhanced management.
type DeadLetterQueue struct {
	tasks     []*QueueTask
	mu        sync.RWMutex
	maxSize   int
	createdAt time.Time
}

func NewDeadLetterQueue() *DeadLetterQueue {
	return &DeadLetterQueue{
		tasks:     make([]*QueueTask, 0),
		maxSize:   10000, // Configurable maximum size
		createdAt: time.Now(),
	}
}

func (dlq *DeadLetterQueue) Tasks() []*QueueTask {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()
	// Return a copy to prevent external modification
	tasksCopy := make([]*QueueTask, len(dlq.tasks))
	copy(tasksCopy, dlq.tasks)
	return tasksCopy
}

func (dlq *DeadLetterQueue) Add(task *QueueTask) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	// Check size limits
	if len(dlq.tasks) >= dlq.maxSize {
		// Remove oldest task to make room
		Logger.Warn().Str("taskID", dlq.tasks[0].payload.ID).Msg("DLQ full, removing oldest task")
		dlq.tasks = dlq.tasks[1:]
	}

	// Add failure metadata
	task.payload.ProcessedAt = time.Now()
	task.payload.Status = Failed

	dlq.tasks = append(dlq.tasks, task)
	Logger.Warn().Str("taskID", task.payload.ID).
		Int("retryCount", task.retryCount).
		Int("dlqSize", len(dlq.tasks)).
		Msg("Task added to Dead Letter Queue")
}

// GetTasksByErrorType returns tasks that failed with similar errors
func (dlq *DeadLetterQueue) GetTasksByErrorType(errorPattern string) []*QueueTask {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	var matchingTasks []*QueueTask
	for _, task := range dlq.tasks {
		if task.payload.Error != nil && strings.Contains(task.payload.Error.Error(), errorPattern) {
			matchingTasks = append(matchingTasks, task)
		}
	}
	return matchingTasks
}

// Clear removes all tasks from the DLQ
func (dlq *DeadLetterQueue) Clear() int {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	count := len(dlq.tasks)
	dlq.tasks = dlq.tasks[:0]
	Logger.Info().Msgf("Cleared %d tasks from Dead Letter Queue", count)
	return count
}

// RemoveOlderThan removes tasks older than the specified duration
func (dlq *DeadLetterQueue) RemoveOlderThan(duration time.Duration) int {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	cutoff := time.Now().Add(-duration)
	originalCount := len(dlq.tasks)

	filteredTasks := make([]*QueueTask, 0, len(dlq.tasks))
	for _, task := range dlq.tasks {
		if task.payload.ProcessedAt.After(cutoff) {
			filteredTasks = append(filteredTasks, task)
		}
	}

	dlq.tasks = filteredTasks
	removed := originalCount - len(dlq.tasks)

	if removed > 0 {
		Logger.Info().Msgf("Removed %d old tasks from Dead Letter Queue", removed)
	}

	return removed
}

// Size returns the current number of tasks in the DLQ
func (dlq *DeadLetterQueue) Size() int {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()
	return len(dlq.tasks)
}

// GetStats returns statistics about the DLQ
func (dlq *DeadLetterQueue) GetStats() map[string]interface{} {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	errorCounts := make(map[string]int)
	var oldestTask, newestTask time.Time

	for i, task := range dlq.tasks {
		// Count error types
		if task.payload.Error != nil {
			errorType := fmt.Sprintf("%T", task.payload.Error)
			errorCounts[errorType]++
		}

		// Track oldest and newest
		if i == 0 {
			oldestTask = task.payload.ProcessedAt
			newestTask = task.payload.ProcessedAt
		} else {
			if task.payload.ProcessedAt.Before(oldestTask) {
				oldestTask = task.payload.ProcessedAt
			}
			if task.payload.ProcessedAt.After(newestTask) {
				newestTask = task.payload.ProcessedAt
			}
		}
	}

	return map[string]interface{}{
		"total_tasks":  len(dlq.tasks),
		"max_size":     dlq.maxSize,
		"error_counts": errorCounts,
		"oldest_task":  oldestTask,
		"newest_task":  newestTask,
		"created_at":   dlq.createdAt,
	}
}

// InMemoryMetricsRegistry stores metrics in memory.
type InMemoryMetricsRegistry struct {
	metrics map[string]int64
	mu      sync.RWMutex
}

func NewInMemoryMetricsRegistry() *InMemoryMetricsRegistry {
	return &InMemoryMetricsRegistry{
		metrics: make(map[string]int64),
	}
}

func (m *InMemoryMetricsRegistry) Register(metricName string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := value.(int64); ok {
		m.metrics[metricName] = v
		Logger.Info().Str("metric", metricName).Msgf("Registered metric: %d", v)
	}
}

func (m *InMemoryMetricsRegistry) Increment(metricName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics[metricName]++
}

func (m *InMemoryMetricsRegistry) Get(metricName string) interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.metrics[metricName]
}

// WarningThresholds defines thresholds for warnings.
type WarningThresholds struct {
	HighMemory    int64         // in bytes
	LongExecution time.Duration // threshold duration
}

// DynamicConfig holds runtime configuration values.
type DynamicConfig struct {
	Timeout          time.Duration
	BatchSize        int
	MaxMemoryLoad    int64
	IdleTimeout      time.Duration
	BackoffDuration  time.Duration
	MaxRetries       int
	ReloadInterval   time.Duration
	WarningThreshold WarningThresholds
	NumberOfWorkers  int // new field for worker count
}

var Config = &DynamicConfig{
	Timeout:         10 * time.Second,
	BatchSize:       1,
	MaxMemoryLoad:   100 * 1024 * 1024,
	IdleTimeout:     5 * time.Minute,
	BackoffDuration: 2 * time.Second,
	MaxRetries:      3,
	ReloadInterval:  30 * time.Second,
	WarningThreshold: WarningThresholds{
		HighMemory:    1 * 1024 * 1024, // 1 MB
		LongExecution: 2 * time.Second,
	},
	NumberOfWorkers: 5, // default worker count
}

// Pool represents the worker pool processing tasks.
type Pool struct {
	taskStorage                TaskStorage
	stop                       chan struct{}
	taskNotify                 chan struct{}
	workerAdjust               chan int
	handler                    Handler
	completionCallback         CompletionCallback
	taskAvailableCond          *sync.Cond
	callback                   Callback
	dlq                        *DeadLetterQueue
	taskQueue                  PriorityQueue
	overflowBuffer             []*QueueTask
	metrics                    Metrics
	wg                         sync.WaitGroup
	taskCompletionNotifier     sync.WaitGroup
	timeout                    time.Duration
	batchSize                  int
	maxMemoryLoad              int64
	idleTimeout                time.Duration
	backoffDuration            time.Duration
	maxRetries                 int
	overflowBufferLock         sync.RWMutex
	taskQueueLock              sync.Mutex
	numOfWorkers               int32
	paused                     bool
	logger                     log.Logger
	gracefulShutdown           bool
	thresholds                 ThresholdConfig
	diagnosticsEnabled         bool
	metricsRegistry            MetricsRegistry
	circuitBreaker             CircuitBreakerConfig
	circuitBreakerOpen         bool
	circuitBreakerFailureCount int32
	gracefulShutdownTimeout    time.Duration
	plugins                    []Plugin
}

// NewPool creates and starts a new pool with the given number of workers.
func NewPool(numOfWorkers int, opts ...PoolOption) *Pool {
	pool := &Pool{
		stop:                    make(chan struct{}),
		taskNotify:              make(chan struct{}, numOfWorkers),
		batchSize:               Config.BatchSize,
		timeout:                 Config.Timeout,
		idleTimeout:             Config.IdleTimeout,
		backoffDuration:         Config.BackoffDuration,
		maxRetries:              Config.MaxRetries,
		logger:                  Logger,
		numOfWorkers:            int32(numOfWorkers),
		dlq:                     NewDeadLetterQueue(),
		metricsRegistry:         NewInMemoryMetricsRegistry(),
		diagnosticsEnabled:      true,
		gracefulShutdownTimeout: 10 * time.Second,
	}
	pool.taskAvailableCond = sync.NewCond(&sync.Mutex{})
	for _, opt := range opts {
		opt(pool)
	}
	if pool.taskQueue == nil {
		pool.taskQueue = make(PriorityQueue, 0, 10)
	}
	pool.Init()
	return pool
}

func (wp *Pool) Init() {
	heap.Init(&wp.taskQueue)
	wp.Start(int(wp.numOfWorkers))
	go startConfigReloader(wp)
	go wp.dynamicWorkerScaler()
}

func validateDynamicConfig(c *DynamicConfig) error {
	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if c.BatchSize <= 0 {
		return errors.New("BatchSize must be > 0")
	}
	if c.MaxMemoryLoad <= 0 {
		return errors.New("MaxMemoryLoad must be > 0")
	}
	return nil
}

func startConfigReloader(pool *Pool) {
	ticker := time.NewTicker(Config.ReloadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := validateDynamicConfig(Config); err != nil {
				Logger.Error().Err(err).Msg("Invalid dynamic config, skipping reload")
				continue
			}
			if pool.timeout != Config.Timeout {
				pool.timeout = Config.Timeout
			}
			if pool.batchSize != Config.BatchSize {
				pool.batchSize = Config.BatchSize
			}
			if pool.maxMemoryLoad != Config.MaxMemoryLoad {
				pool.maxMemoryLoad = Config.MaxMemoryLoad
			}
			if pool.idleTimeout != Config.IdleTimeout {
				pool.idleTimeout = Config.IdleTimeout
			}
			if pool.backoffDuration != Config.BackoffDuration {
				pool.backoffDuration = Config.BackoffDuration
			}
			if pool.maxRetries != Config.MaxRetries {
				pool.maxRetries = Config.MaxRetries
			}
			if pool.thresholds.HighMemory != Config.WarningThreshold.HighMemory {
				pool.thresholds.HighMemory = Config.WarningThreshold.HighMemory
			}
			if pool.thresholds.LongExecution != Config.WarningThreshold.LongExecution {
				pool.thresholds.LongExecution = Config.WarningThreshold.LongExecution
			}
		case <-pool.stop:
			return
		}
	}
}

func (wp *Pool) Start(numWorkers int) {
	storedTasks, err := wp.taskStorage.GetAllTasks()
	if err == nil {
		wp.taskQueueLock.Lock()
		for _, task := range storedTasks {
			heap.Push(&wp.taskQueue, task)
		}
		wp.taskQueueLock.Unlock()
	}
	for i := 0; i < numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
	atomic.StoreInt32(&wp.numOfWorkers, int32(numWorkers))
	go wp.monitorWorkerAdjustments()
	go wp.startOverflowDrainer()
	go wp.monitorIdleWorkers()
}

func (wp *Pool) DLQ() *DeadLetterQueue {
	return wp.dlq
}

func (wp *Pool) worker() {
	defer wp.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			wp.logger.Error().Msgf("Worker panic recovered: %v", r)
			// Restart the worker if not shutting down
			if !wp.gracefulShutdown {
				wp.wg.Add(1)
				go wp.worker()
			}
		}
	}()

	for {
		// Check for shutdown first
		select {
		case <-wp.stop:
			return
		default:
		}

		// Wait for tasks with proper synchronization
		wp.taskAvailableCond.L.Lock()
		for len(wp.taskQueue) == 0 && !wp.paused && !wp.gracefulShutdown {
			wp.taskAvailableCond.Wait()
		}
		wp.taskAvailableCond.L.Unlock()

		// Check shutdown again after waiting
		select {
		case <-wp.stop:
			return
		default:
			if !wp.paused && !wp.gracefulShutdown {
				wp.processNextBatch()
			}
		}
	}
}

func (wp *Pool) processNextBatch() {
	if wp.gracefulShutdown {
		return
	}

	wp.taskQueueLock.Lock()
	tasks := make([]*QueueTask, 0, wp.batchSize)
	for len(wp.taskQueue) > 0 && !wp.paused && len(tasks) < wp.batchSize {
		task := heap.Pop(&wp.taskQueue).(*QueueTask)
		tasks = append(tasks, task)
	}
	wp.taskQueueLock.Unlock()

	// If no tasks in memory, try fetching from storage
	if len(tasks) == 0 && !wp.paused && wp.taskStorage != nil {
		for len(tasks) < wp.batchSize {
			task, err := wp.taskStorage.FetchNextTask()
			if err != nil {
				break
			}
			tasks = append(tasks, task)
		}
	}

	// Process tasks with controlled concurrency
	if len(tasks) > 0 {
		for _, task := range tasks {
			if task != nil && !wp.gracefulShutdown {
				wp.taskCompletionNotifier.Add(1)
				wp.handleTask(task)
			}
		}
	}
}

func (wp *Pool) handleTask(task *QueueTask) {
	if task == nil || task.payload == nil {
		wp.logger.Warn().Msg("Received nil task or payload")
		// Only call Done if Add was called (which is now only for actual tasks)
		wp.taskCompletionNotifier.Done()
		return
	}

	// Create timeout context with proper cancellation
	ctx, cancel := context.WithTimeout(task.ctx, wp.timeout)
	defer cancel()

	// Check for task expiration
	if task.payload.IsExpired() {
		wp.logger.Warn().Str("taskID", task.payload.ID).Msg("Task expired, moving to DLQ")
		wp.dlq.Add(task)
		atomic.AddInt64(&wp.metrics.ErrorCount, 1)
		return
	}

	// Measure memory usage for the task
	taskSize := int64(utils.SizeOf(task.payload))

	// Check memory limits before processing
	if wp.maxMemoryLoad > 0 && atomic.LoadInt64(&wp.metrics.TotalMemoryUsed)+taskSize > wp.maxMemoryLoad {
		wp.logger.Warn().Str("taskID", task.payload.ID).Msg("Memory limit reached, storing in overflow")
		wp.storeInOverflow(task)
		return
	}

	// Update metrics atomically
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, taskSize)
	atomic.AddInt64(&wp.metrics.CumulativeMemoryUsed, taskSize)
	atomic.AddInt64(&wp.metrics.TotalTasks, 1)

	// Recovery mechanism for handler panics
	var result Result
	var handlerErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				handlerErr = fmt.Errorf("handler panic: %v", r)
				wp.logger.Error().Str("taskID", task.payload.ID).Msgf("Handler panic recovered: %v", r)
			}
		}()

		startTime := time.Now()

		// Execute plugins before task processing
		for _, plugin := range wp.plugins {
			plugin.BeforeTask(task)
		}

		// Execute the actual task handler
		if wp.handler != nil {
			result = wp.handler(ctx, task.payload)
		} else {
			handlerErr = fmt.Errorf("no handler configured")
		}

		// Calculate execution time
		execMs := time.Since(startTime).Milliseconds()
		atomic.AddInt64(&wp.metrics.ExecutionTime, execMs)

		// Execute plugins after task processing
		for _, plugin := range wp.plugins {
			plugin.AfterTask(task, result)
		}

		// Check execution time threshold
		if wp.thresholds.LongExecution > 0 && execMs > wp.thresholds.LongExecution.Milliseconds() {
			wp.logger.Warn().Str("taskID", task.payload.ID).Msgf("Exceeded execution time threshold: %d ms", execMs)
		}
	}()

	// Handle any panic errors
	if handlerErr != nil {
		result.Error = handlerErr
	}

	// Check memory usage threshold
	if wp.thresholds.HighMemory > 0 && taskSize > wp.thresholds.HighMemory {
		wp.logger.Warn().Str("taskID", task.payload.ID).Msgf("Memory usage %d exceeded threshold", taskSize)
	}

	// Process result and handle errors
	if result.Error != nil {
		atomic.AddInt64(&wp.metrics.ErrorCount, 1)
		wp.logger.Error().Str("taskID", task.payload.ID).Msgf("Error processing task: %v", result.Error)
		wp.handleTaskFailure(task, result)
	} else {
		atomic.AddInt64(&wp.metrics.CompletedTasks, 1)
		wp.handleTaskSuccess(task, result, ctx)
	}

	// Execute callback if provided
	if wp.callback != nil {
		if err := wp.callback(ctx, result); err != nil {
			atomic.AddInt64(&wp.metrics.ErrorCount, 1)
			wp.logger.Error().Str("taskID", task.payload.ID).Msgf("Callback error: %v", err)
		}
	}

	// Cleanup task from storage
	if wp.taskStorage != nil {
		if err := wp.taskStorage.DeleteTask(task.payload.ID); err != nil {
			wp.logger.Warn().Str("taskID", task.payload.ID).Msgf("Failed to delete task from storage: %v", err)
		}
	}

	// Update metrics
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, -taskSize)
	wp.metricsRegistry.Register("task_execution_time", time.Since(time.Now()).Milliseconds())

	// Signal task completion
	wp.taskCompletionNotifier.Done()
}

// handleTaskFailure processes task failures with retry logic and circuit breaker
func (wp *Pool) handleTaskFailure(task *QueueTask, result Result) {
	wp.backoffAndStore(task)

	// Circuit breaker logic
	if wp.circuitBreaker.Enabled {
		newCount := atomic.AddInt32(&wp.circuitBreakerFailureCount, 1)
		if newCount >= int32(wp.circuitBreaker.FailureThreshold) {
			wp.circuitBreakerOpen = true
			wp.logger.Warn().Msg("Circuit breaker opened due to errors")

			// Reset circuit breaker after timeout
			go func() {
				time.Sleep(wp.circuitBreaker.ResetTimeout)
				atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
				wp.circuitBreakerOpen = false
				wp.logger.Info().Msg("Circuit breaker reset to closed state")
			}()
		}
	}
}

// handleTaskSuccess processes successful task completion
func (wp *Pool) handleTaskSuccess(task *QueueTask, result Result, ctx context.Context) {
	// Reset circuit breaker failure count on success
	if wp.circuitBreaker.Enabled {
		atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
	}

	// Log diagnostic information if enabled
	if wp.diagnosticsEnabled {
		execTime := time.Since(task.payload.CreatedAt).Milliseconds()
		wp.logger.Info().Str("taskID", task.payload.ID).Msgf("Task completed successfully in %d ms", execTime)
	}
}

func (wp *Pool) backoffAndStore(task *QueueTask) {
	if task.retryCount < wp.maxRetries {
		task.retryCount++

		// Exponential backoff with jitter and max cap
		baseBackoff := wp.backoffDuration
		exponentialBackoff := baseBackoff * time.Duration(1<<uint(task.retryCount-1))

		// Cap the maximum backoff time to prevent excessive delays
		maxBackoff := time.Minute * 5
		if exponentialBackoff > maxBackoff {
			exponentialBackoff = maxBackoff
		}

		// Add jitter to prevent thundering herd
		jitter := time.Duration(rand.Int63n(int64(exponentialBackoff) / 2))
		sleepDuration := exponentialBackoff + jitter

		wp.logger.Info().Str("taskID", task.payload.ID).Msgf("Retry %d/%d: will retry after %s",
			task.retryCount, wp.maxRetries, sleepDuration)

		// Schedule retry asynchronously to avoid blocking worker
		go func() {
			time.Sleep(sleepDuration)
			if !wp.gracefulShutdown {
				wp.storeInOverflow(task)
			}
		}()
	} else {
		wp.logger.Error().Str("taskID", task.payload.ID).Msgf("Task failed after %d retries, moving to DLQ", wp.maxRetries)
		wp.dlq.Add(task)
	}
}

func (wp *Pool) monitorIdleWorkers() {
	for {
		select {
		case <-wp.stop:
			return
		default:
			time.Sleep(wp.idleTimeout)
			wp.adjustIdleWorkers()
		}
	}
}

func (wp *Pool) adjustIdleWorkers() {
	currentWorkers := atomic.LoadInt32(&wp.numOfWorkers)
	if currentWorkers > 1 {
		atomic.StoreInt32(&wp.numOfWorkers, currentWorkers-1)
		wp.wg.Add(1)
		go wp.worker()
	}
}

func (wp *Pool) monitorWorkerAdjustments() {
	for {
		select {
		case adjustment := <-wp.workerAdjust:
			currentWorkers := atomic.LoadInt32(&wp.numOfWorkers)
			newWorkerCount := int(currentWorkers) + adjustment
			if newWorkerCount > 0 {
				wp.adjustWorkers(newWorkerCount)
			}
		case <-wp.stop:
			return
		}
	}
}

func (wp *Pool) adjustWorkers(newWorkerCount int) {
	currentWorkers := int(atomic.LoadInt32(&wp.numOfWorkers))

	if newWorkerCount <= 0 {
		wp.logger.Warn().Msg("Invalid worker count, ignoring adjustment")
		return
	}

	if newWorkerCount > currentWorkers {
		// Add workers
		diff := newWorkerCount - currentWorkers
		wp.logger.Info().Msgf("Scaling up: adding %d workers", diff)

		for i := 0; i < diff; i++ {
			wp.wg.Add(1)
			go wp.worker()
		}
	} else if newWorkerCount < currentWorkers {
		// Reduce workers gracefully
		diff := currentWorkers - newWorkerCount
		wp.logger.Info().Msgf("Scaling down: removing %d workers", diff)

		// Signal workers to stop
		for i := 0; i < diff; i++ {
			select {
			case wp.stop <- struct{}{}:
			default:
				// Channel might be full or closed
			}
		}
	}

	atomic.StoreInt32(&wp.numOfWorkers, int32(newWorkerCount))
	wp.logger.Info().Msgf("Worker count adjusted to %d", newWorkerCount)
}

func (wp *Pool) EnqueueTask(ctx context.Context, payload *Task, priority int) error {
	if wp.gracefulShutdown {
		return fmt.Errorf("pool is shutting down, cannot accept new tasks")
	}

	if payload == nil {
		return fmt.Errorf("payload cannot be nil")
	}

	// Circuit breaker check
	if wp.circuitBreaker.Enabled && wp.circuitBreakerOpen {
		return fmt.Errorf("circuit breaker open, task rejected")
	}

	// Generate ID if not provided
	if payload.ID == "" {
		payload.ID = NewID()
	}

	// Validate task expiration
	if payload.IsExpired() {
		return fmt.Errorf("task has already expired")
	}

	// Create queue task
	task := &QueueTask{
		ctx:        ctx,
		payload:    payload,
		priority:   priority,
		retryCount: 0,
	}

	// Save to persistent storage first
	if wp.taskStorage != nil {
		if err := wp.taskStorage.SaveTask(task); err != nil {
			return fmt.Errorf("failed to save task to storage: %w", err)
		}
	}

	// Check memory limits
	taskSize := int64(utils.SizeOf(payload))
	currentMemory := atomic.LoadInt64(&wp.metrics.TotalMemoryUsed)

	if wp.maxMemoryLoad > 0 && currentMemory+taskSize > wp.maxMemoryLoad {
		wp.logger.Warn().Str("taskID", payload.ID).Msg("Memory limit reached, storing in overflow buffer")
		wp.storeInOverflow(task)
		return fmt.Errorf("max memory load reached, task stored in overflow buffer")
	}

	// Add to priority queue
	wp.taskQueueLock.Lock()
	heap.Push(&wp.taskQueue, task)
	queueLen := len(wp.taskQueue)
	wp.taskQueueLock.Unlock()

	// Signal waiting workers
	wp.taskAvailableCond.L.Lock()
	wp.taskAvailableCond.Signal()
	wp.taskAvailableCond.L.Unlock()

	// Update metrics
	atomic.AddInt64(&wp.metrics.TotalScheduled, 1)

	wp.logger.Debug().Str("taskID", payload.ID).Msgf("Task enqueued with priority %d, queue depth: %d", priority, queueLen)

	return nil
}

// PoolHealthStatus represents the health state of the pool
type PoolHealthStatus struct {
	IsHealthy           bool          `json:"is_healthy"`
	WorkerCount         int32         `json:"worker_count"`
	QueueDepth          int           `json:"queue_depth"`
	OverflowDepth       int           `json:"overflow_depth"`
	DLQDepth            int           `json:"dlq_depth"`
	CircuitBreakerOpen  bool          `json:"circuit_breaker_open"`
	MemoryUsage         string        `json:"memory_usage"`
	MemoryUsagePercent  float64       `json:"memory_usage_percent"`
	LastTaskProcessedAt *time.Time    `json:"last_task_processed_at,omitempty"`
	Uptime              time.Duration `json:"uptime"`
	ErrorRate           float64       `json:"error_rate"`
	ThroughputPerSecond float64       `json:"throughput_per_second"`
	Issues              []string      `json:"issues,omitempty"`
}

// GetHealthStatus returns the current health status of the pool
func (wp *Pool) GetHealthStatus() PoolHealthStatus {
	wp.taskQueueLock.Lock()
	queueDepth := len(wp.taskQueue)
	wp.taskQueueLock.Unlock()

	wp.overflowBufferLock.RLock()
	overflowDepth := len(wp.overflowBuffer)
	wp.overflowBufferLock.RUnlock()

	dlqDepth := len(wp.dlq.Tasks())

	totalTasks := atomic.LoadInt64(&wp.metrics.TotalTasks)
	errorCount := atomic.LoadInt64(&wp.metrics.ErrorCount)
	currentMemory := atomic.LoadInt64(&wp.metrics.TotalMemoryUsed)

	var errorRate float64
	if totalTasks > 0 {
		errorRate = float64(errorCount) / float64(totalTasks) * 100
	}

	var memoryUsagePercent float64
	if wp.maxMemoryLoad > 0 {
		memoryUsagePercent = float64(currentMemory) / float64(wp.maxMemoryLoad) * 100
	}

	// Calculate throughput (tasks per second over last minute)
	throughput := float64(atomic.LoadInt64(&wp.metrics.CompletedTasks)) / time.Since(time.Now().Add(-time.Minute)).Seconds()

	var issues []string
	isHealthy := true

	// Health checks
	if wp.circuitBreakerOpen {
		issues = append(issues, "Circuit breaker is open")
		isHealthy = false
	}

	if errorRate > 10 { // More than 10% error rate
		issues = append(issues, fmt.Sprintf("High error rate: %.2f%%", errorRate))
		isHealthy = false
	}

	if memoryUsagePercent > 90 {
		issues = append(issues, fmt.Sprintf("High memory usage: %.2f%%", memoryUsagePercent))
		isHealthy = false
	}

	if queueDepth > 1000 {
		issues = append(issues, fmt.Sprintf("High queue depth: %d", queueDepth))
		isHealthy = false
	}

	if overflowDepth > 100 {
		issues = append(issues, fmt.Sprintf("High overflow buffer depth: %d", overflowDepth))
		isHealthy = false
	}

	if atomic.LoadInt32(&wp.numOfWorkers) == 0 {
		issues = append(issues, "No active workers")
		isHealthy = false
	}

	return PoolHealthStatus{
		IsHealthy:           isHealthy,
		WorkerCount:         atomic.LoadInt32(&wp.numOfWorkers),
		QueueDepth:          queueDepth,
		OverflowDepth:       overflowDepth,
		DLQDepth:            dlqDepth,
		CircuitBreakerOpen:  wp.circuitBreakerOpen,
		MemoryUsage:         utils.FormatBytes(currentMemory),
		MemoryUsagePercent:  memoryUsagePercent,
		ErrorRate:           errorRate,
		ThroughputPerSecond: throughput,
		Issues:              issues,
	}
}

// RecoverFromFailure attempts to recover from various failure scenarios
func (wp *Pool) RecoverFromFailure() error {
	wp.logger.Info().Msg("Attempting to recover from failure")

	// Reset circuit breaker if it's open
	if wp.circuitBreakerOpen {
		atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
		wp.circuitBreakerOpen = false
		wp.logger.Info().Msg("Circuit breaker manually reset")
	}

	// Ensure minimum workers are running
	currentWorkers := int(atomic.LoadInt32(&wp.numOfWorkers))
	if currentWorkers == 0 {
		wp.logger.Warn().Msg("No workers running, starting minimum workers")
		wp.AdjustWorkerCount(3)
	}

	// Try to drain overflow buffer
	wp.drainOverflowBuffer()

	return nil
}

func (wp *Pool) Dispatch(event func()) {
	wp.taskAvailableCond.L.Lock()
	event()
	wp.taskAvailableCond.L.Unlock()
}

func (wp *Pool) Pause() {
	wp.paused = true
	wp.Dispatch(wp.taskAvailableCond.Broadcast)
}

func (wp *Pool) SetBatchSize(size int) {
	wp.batchSize = size
}

func (wp *Pool) Resume() {
	wp.paused = false
	wp.Dispatch(wp.taskAvailableCond.Broadcast)
}

func (wp *Pool) storeInOverflow(task *QueueTask) {
	wp.overflowBufferLock.Lock()
	defer wp.overflowBufferLock.Unlock()

	// Check overflow buffer size limits
	const maxOverflowSize = 10000 // Configurable limit
	if len(wp.overflowBuffer) >= maxOverflowSize {
		wp.logger.Error().Str("taskID", task.payload.ID).Msg("Overflow buffer full, moving task to DLQ")
		wp.dlq.Add(task)
		return
	}

	wp.overflowBuffer = append(wp.overflowBuffer, task)
	wp.logger.Debug().Str("taskID", task.payload.ID).Msgf("Task stored in overflow buffer, size: %d", len(wp.overflowBuffer))
}

func (wp *Pool) startOverflowDrainer() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-wp.stop:
			wp.logger.Info().Msg("Overflow drainer shutting down")
			return
		case <-ticker.C:
			wp.drainOverflowBuffer()
		}
	}
}

func (wp *Pool) drainOverflowBuffer() {
	if wp.gracefulShutdown {
		return
	}

	wp.overflowBufferLock.Lock()
	if len(wp.overflowBuffer) == 0 {
		wp.overflowBufferLock.Unlock()
		return
	}

	// Move a batch of tasks from overflow to main queue
	batchSize := min(len(wp.overflowBuffer), wp.batchSize)
	tasksToMove := make([]*QueueTask, batchSize)
	copy(tasksToMove, wp.overflowBuffer[:batchSize])
	wp.overflowBuffer = wp.overflowBuffer[batchSize:]
	overflowSize := len(wp.overflowBuffer)
	wp.overflowBufferLock.Unlock()

	// Check memory before moving tasks
	currentMemory := atomic.LoadInt64(&wp.metrics.TotalMemoryUsed)
	if wp.maxMemoryLoad > 0 && currentMemory > wp.maxMemoryLoad {
		// Put tasks back if memory is still high
		wp.overflowBufferLock.Lock()
		wp.overflowBuffer = append(tasksToMove, wp.overflowBuffer...)
		wp.overflowBufferLock.Unlock()
		return
	}

	// Move tasks to main queue
	moved := 0
	wp.taskQueueLock.Lock()
	for _, task := range tasksToMove {
		// Double-check task hasn't expired
		if !task.payload.IsExpired() {
			heap.Push(&wp.taskQueue, task)
			moved++
		} else {
			wp.dlq.Add(task)
		}
	}
	wp.taskQueueLock.Unlock()

	if moved > 0 {
		// Signal workers that tasks are available
		wp.taskAvailableCond.L.Lock()
		wp.taskAvailableCond.Broadcast()
		wp.taskAvailableCond.L.Unlock()

		wp.logger.Debug().Msgf("Moved %d tasks from overflow to main queue, %d remaining in overflow", moved, overflowSize)
	}
}

// Helper function for min (Go 1.21+ has this built-in)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (wp *Pool) Stop() {
	wp.logger.Info().Msg("Initiating graceful shutdown")
	wp.gracefulShutdown = true

	// Pause new task processing
	wp.Pause()

	// Signal all goroutines to stop
	close(wp.stop)

	// Create channels for coordinated shutdown
	workersFinished := make(chan struct{})
	tasksFinished := make(chan struct{})

	// Wait for workers to finish
	go func() {
		wp.wg.Wait()
		close(workersFinished)
	}()

	// Wait for pending tasks to complete
	go func() {
		wp.taskCompletionNotifier.Wait()
		close(tasksFinished)
	}()

	// Wait with timeout
	shutdownTimer := time.NewTimer(wp.gracefulShutdownTimeout)
	defer shutdownTimer.Stop()

	workersComplete := false
	tasksComplete := false

	for !workersComplete || !tasksComplete {
		select {
		case <-workersFinished:
			if !workersComplete {
				wp.logger.Info().Msg("All workers have finished")
				workersComplete = true
			}
		case <-tasksFinished:
			if !tasksComplete {
				wp.logger.Info().Msg("All pending tasks have completed")
				tasksComplete = true
			}
		case <-shutdownTimer.C:
			wp.logger.Warn().Msgf("Graceful shutdown timeout (%v) reached, forcing shutdown", wp.gracefulShutdownTimeout)
			goto forceShutdown
		}
	}

forceShutdown:
	// Final cleanup
	wp.cleanup()

	if wp.completionCallback != nil {
		wp.completionCallback()
	}

	wp.logger.Info().Msg("Pool shutdown completed")
}

// cleanup performs final resource cleanup
func (wp *Pool) cleanup() {
	// Close overflow drainer
	// Note: We rely on the stop channel being closed to stop the drainer

	// Log final metrics
	metrics := wp.FormattedMetrics()
	wp.logger.Info().Msgf("Final metrics: Tasks=%d, Completed=%d, Errors=%d, Memory=%s",
		metrics.TotalTasks, metrics.CompletedTasks, metrics.ErrorCount, metrics.CurrentMemoryUsed)

	// Cleanup any remaining tasks in overflow buffer
	wp.overflowBufferLock.Lock()
	if len(wp.overflowBuffer) > 0 {
		wp.logger.Warn().Msgf("Cleaning up %d tasks from overflow buffer", len(wp.overflowBuffer))
		for _, task := range wp.overflowBuffer {
			if wp.taskStorage != nil {
				wp.taskStorage.DeleteTask(task.payload.ID)
			}
		}
		wp.overflowBuffer = nil
	}
	wp.overflowBufferLock.Unlock()
}

func (wp *Pool) AdjustWorkerCount(newWorkerCount int) {
	adjustment := newWorkerCount - int(atomic.LoadInt32(&wp.numOfWorkers))
	if adjustment != 0 {
		wp.workerAdjust <- adjustment
	}
}

func (wp *Pool) AddScheduledMetrics(total int) {
	wp.metrics.TotalScheduled = int64(total)
}

func (wp *Pool) Metrics() Metrics {
	return wp.metrics
}

// FormattedMetrics is a helper struct to present human-readable metrics.
type FormattedMetrics struct {
	TotalTasks           int64  `json:"total_tasks"`
	CompletedTasks       int64  `json:"completed_tasks"`
	ErrorCount           int64  `json:"error_count"`
	CurrentMemoryUsed    string `json:"current_memory_used"`
	CumulativeMemoryUsed string `json:"cumulative_memory_used"`
	TotalScheduled       int64  `json:"total_scheduled"`
	CumulativeExecution  string `json:"cumulative_execution"`
	AverageExecution     string `json:"average_execution"`
}

// FormattedMetrics returns a formatted version of the pool metrics.
func (wp *Pool) FormattedMetrics() FormattedMetrics {
	var avgExec time.Duration
	if wp.metrics.CompletedTasks > 0 {
		avgExec = time.Duration(wp.metrics.ExecutionTime/wp.metrics.CompletedTasks) * time.Millisecond
	}
	return FormattedMetrics{
		TotalTasks:           wp.metrics.TotalTasks,
		CompletedTasks:       wp.metrics.CompletedTasks,
		ErrorCount:           wp.metrics.ErrorCount,
		CurrentMemoryUsed:    utils.FormatBytes(wp.metrics.TotalMemoryUsed),
		CumulativeMemoryUsed: utils.FormatBytes(wp.metrics.CumulativeMemoryUsed),
		TotalScheduled:       wp.metrics.TotalScheduled,
		CumulativeExecution:  (time.Duration(wp.metrics.ExecutionTime) * time.Millisecond).String(),
		AverageExecution:     avgExec.String(),
	}
}

func (wp *Pool) dynamicWorkerScaler() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wp.adjustWorkersBasedOnLoad()
		case <-wp.stop:
			wp.logger.Info().Msg("Dynamic worker scaler shutting down")
			return
		}
	}
}

func (wp *Pool) adjustWorkersBasedOnLoad() {
	if wp.gracefulShutdown {
		return
	}

	wp.taskQueueLock.Lock()
	queueLen := len(wp.taskQueue)
	wp.taskQueueLock.Unlock()

	wp.overflowBufferLock.RLock()
	overflowLen := len(wp.overflowBuffer)
	wp.overflowBufferLock.RUnlock()

	currentWorkers := int(atomic.LoadInt32(&wp.numOfWorkers))
	totalPendingTasks := queueLen + overflowLen

	// Calculate optimal worker count based on load
	var targetWorkers int

	switch {
	case totalPendingTasks == 0:
		// No pending tasks, maintain minimum workers
		targetWorkers = max(1, currentWorkers/2)
	case totalPendingTasks < 5:
		// Low load
		targetWorkers = max(1, min(currentWorkers, 3))
	case totalPendingTasks < 20:
		// Medium load
		targetWorkers = min(currentWorkers+1, 10)
	case totalPendingTasks < 100:
		// High load
		targetWorkers = min(totalPendingTasks/5+1, 20)
	default:
		// Very high load
		targetWorkers = min(30, totalPendingTasks/10+1)
	}

	// Apply constraints
	const minWorkers, maxWorkers = 1, 50
	targetWorkers = max(minWorkers, min(maxWorkers, targetWorkers))

	if targetWorkers != currentWorkers {
		wp.logger.Info().Msgf("Auto-scaling workers from %d to %d (queue: %d, overflow: %d)",
			currentWorkers, targetWorkers, queueLen, overflowLen)
		wp.AdjustWorkerCount(targetWorkers)
	}
}

// Helper function for max
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// UpdateConfig updates pool configuration via a POOL_UPDATE command with validation.
func (wp *Pool) UpdateConfig(newConfig *DynamicConfig) error {
	if err := validateDynamicConfig(newConfig); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	wp.logger.Info().Msg("Updating pool configuration")

	// Update configuration atomically where possible
	oldTimeout := wp.timeout
	oldBatchSize := wp.batchSize
	oldWorkerCount := int(atomic.LoadInt32(&wp.numOfWorkers))

	wp.timeout = newConfig.Timeout
	wp.batchSize = newConfig.BatchSize
	wp.maxMemoryLoad = newConfig.MaxMemoryLoad
	wp.idleTimeout = newConfig.IdleTimeout
	wp.backoffDuration = newConfig.BackoffDuration
	wp.maxRetries = newConfig.MaxRetries
	wp.thresholds = ThresholdConfig{
		HighMemory:    newConfig.WarningThreshold.HighMemory,
		LongExecution: newConfig.WarningThreshold.LongExecution,
	}

	// Adjust worker count if specified and different
	newWorkerCount := newConfig.NumberOfWorkers
	if newWorkerCount > 0 && newWorkerCount != oldWorkerCount {
		wp.adjustWorkers(newWorkerCount)
	}

	wp.logger.Info().
		Dur("old_timeout", oldTimeout).Dur("new_timeout", newConfig.Timeout).
		Int("old_batch_size", oldBatchSize).Int("new_batch_size", newConfig.BatchSize).
		Int("old_workers", oldWorkerCount).Int("new_workers", newWorkerCount).
		Msg("Pool configuration updated successfully")

	return nil
}

// GetCurrentConfig returns the current pool configuration
func (wp *Pool) GetCurrentConfig() DynamicConfig {
	return DynamicConfig{
		Timeout:         wp.timeout,
		BatchSize:       wp.batchSize,
		MaxMemoryLoad:   wp.maxMemoryLoad,
		IdleTimeout:     wp.idleTimeout,
		BackoffDuration: wp.backoffDuration,
		MaxRetries:      wp.maxRetries,
		ReloadInterval:  Config.ReloadInterval, // Global config
		WarningThreshold: WarningThresholds{
			HighMemory:    wp.thresholds.HighMemory,
			LongExecution: wp.thresholds.LongExecution,
		},
		NumberOfWorkers: int(atomic.LoadInt32(&wp.numOfWorkers)),
	}
}

// PauseProcessing pauses task processing
func (wp *Pool) PauseProcessing() {
	wp.logger.Info().Msg("Pausing task processing")
	wp.Pause()
}

// ResumeProcessing resumes task processing
func (wp *Pool) ResumeProcessing() {
	wp.logger.Info().Msg("Resuming task processing")
	wp.Resume()
}

// GetQueueDepth returns the current depth of the main task queue
func (wp *Pool) GetQueueDepth() int {
	wp.taskQueueLock.Lock()
	defer wp.taskQueueLock.Unlock()
	return len(wp.taskQueue)
}

// GetOverflowDepth returns the current depth of the overflow buffer
func (wp *Pool) GetOverflowDepth() int {
	wp.overflowBufferLock.RLock()
	defer wp.overflowBufferLock.RUnlock()
	return len(wp.overflowBuffer)
}

// FlushQueues moves all tasks from overflow buffer to main queue (if memory allows)
func (wp *Pool) FlushQueues() error {
	wp.logger.Info().Msg("Flushing overflow buffer to main queue")

	// Force drain overflow buffer
	for i := 0; i < 10; i++ { // Try up to 10 times
		wp.drainOverflowBuffer()

		wp.overflowBufferLock.RLock()
		overflowSize := len(wp.overflowBuffer)
		wp.overflowBufferLock.RUnlock()

		if overflowSize == 0 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	wp.overflowBufferLock.RLock()
	remainingOverflow := len(wp.overflowBuffer)
	wp.overflowBufferLock.RUnlock()

	if remainingOverflow > 0 {
		return fmt.Errorf("could not flush all tasks, %d remain in overflow buffer", remainingOverflow)
	}

	wp.logger.Info().Msg("Successfully flushed overflow buffer")
	return nil
}
