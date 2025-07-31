package mq

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand"
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

// DeadLetterQueue stores tasks that have permanently failed.
type DeadLetterQueue struct {
	tasks []*QueueTask
	mu    sync.Mutex
}

func NewDeadLetterQueue() *DeadLetterQueue {
	return &DeadLetterQueue{
		tasks: make([]*QueueTask, 0),
	}
}

func (dlq *DeadLetterQueue) Tasks() []*QueueTask {
	return dlq.tasks
}

func (dlq *DeadLetterQueue) Add(task *QueueTask) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	dlq.tasks = append(dlq.tasks, task)
	Logger.Warn().Str("taskID", task.payload.ID).Msg("Task added to Dead Letter Queue")
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
	for {
		for len(wp.taskQueue) == 0 && !wp.paused {
			wp.Dispatch(wp.taskAvailableCond.Wait)
		}
		select {
		case <-wp.stop:
			return
		default:
			wp.processNextBatch()
		}
	}
}

func (wp *Pool) processNextBatch() {
	wp.taskQueueLock.Lock()
	defer wp.taskQueueLock.Unlock()
	tasks := make([]*QueueTask, 0, wp.batchSize)
	for len(wp.taskQueue) > 0 && !wp.paused && len(tasks) < wp.batchSize {
		task := heap.Pop(&wp.taskQueue).(*QueueTask)
		tasks = append(tasks, task)
	}
	if len(tasks) == 0 && !wp.paused {
		for len(tasks) < wp.batchSize {
			task, err := wp.taskStorage.FetchNextTask()
			if err != nil {
				break
			}
			tasks = append(tasks, task)
		}
	}
	for _, task := range tasks {
		if task != nil {
			wp.handleTask(task)
		}
	}
}

func (wp *Pool) handleTask(task *QueueTask) {
	ctx, cancel := context.WithTimeout(task.ctx, wp.timeout)
	defer cancel()

	// Measure memory usage for the task.
	taskSize := int64(utils.SizeOf(task.payload))
	// Increase current memory usage.
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, taskSize)
	// Increase cumulative memory usage.
	atomic.AddInt64(&wp.metrics.CumulativeMemoryUsed, taskSize)
	atomic.AddInt64(&wp.metrics.TotalTasks, 1)
	startTime := time.Now()
	result := wp.handler(ctx, task.payload)
	execMs := time.Since(startTime).Milliseconds()
	atomic.AddInt64(&wp.metrics.ExecutionTime, execMs)

	if wp.thresholds.LongExecution > 0 && execMs > wp.thresholds.LongExecution.Milliseconds() {
		wp.logger.Warn().Str("taskID", task.payload.ID).Msgf("Exceeded execution time threshold: %d ms", execMs)
	}
	if wp.thresholds.HighMemory > 0 && taskSize > wp.thresholds.HighMemory {
		wp.logger.Warn().Str("taskID", task.payload.ID).Msgf("Memory usage %d exceeded threshold", taskSize)
	}

	if result.Error != nil {
		atomic.AddInt64(&wp.metrics.ErrorCount, 1)
		wp.logger.Error().Str("taskID", task.payload.ID).Msgf("Error processing task: %v", result.Error)
		wp.backoffAndStore(task)
		if wp.circuitBreaker.Enabled {
			newCount := atomic.AddInt32(&wp.circuitBreakerFailureCount, 1)
			if newCount >= int32(wp.circuitBreaker.FailureThreshold) {
				wp.circuitBreakerOpen = true
				wp.logger.Warn().Msg("Circuit breaker opened due to errors")
				go func() {
					time.Sleep(wp.circuitBreaker.ResetTimeout)
					atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
					wp.circuitBreakerOpen = false
					wp.logger.Info().Msg("Circuit breaker reset to closed state")
				}()
			}
		}
	} else {
		atomic.AddInt64(&wp.metrics.CompletedTasks, 1)
		// Reset failure count on success if using circuit breaker.
		if wp.circuitBreaker.Enabled {
			atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
		}
	}

	if wp.diagnosticsEnabled {
		wp.logger.Info().Str("taskID", task.payload.ID).Msgf("Task executed in %d ms", execMs)
	}
	if wp.callback != nil {
		if err := wp.callback(ctx, result); err != nil {
			atomic.AddInt64(&wp.metrics.ErrorCount, 1)
			wp.logger.Error().Str("taskID", task.payload.ID).Msgf("Callback error: %v", err)
		}
	}
	_ = wp.taskStorage.DeleteTask(task.payload.ID)
	// Reduce current memory usage.
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, -taskSize)
	wp.metricsRegistry.Register("task_execution_time", execMs)
}

func (wp *Pool) backoffAndStore(task *QueueTask) {
	if task.retryCount < wp.maxRetries {
		task.retryCount++
		wp.storeInOverflow(task)
		// Exponential backoff with jitter:
		backoff := wp.backoffDuration * (1 << (task.retryCount - 1))
		jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
		sleepDuration := backoff + jitter
		wp.logger.Info().Str("taskID", task.payload.ID).Msgf("Retry %d: sleeping for %s", task.retryCount, sleepDuration)
		time.Sleep(sleepDuration)
	} else {
		wp.logger.Error().Str("taskID", task.payload.ID).Msg("Task failed after maximum retries")
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
	if newWorkerCount > currentWorkers {
		for i := 0; i < newWorkerCount-currentWorkers; i++ {
			wp.wg.Add(1)
			go wp.worker()
		}
	} else if newWorkerCount < currentWorkers {
		for i := 0; i < currentWorkers-newWorkerCount; i++ {
			wp.stop <- struct{}{}
		}
	}
	atomic.StoreInt32(&wp.numOfWorkers, int32(newWorkerCount))
}

func (wp *Pool) EnqueueTask(ctx context.Context, payload *Task, priority int) error {
	if wp.circuitBreaker.Enabled && wp.circuitBreakerOpen {
		return fmt.Errorf("circuit breaker open, task rejected")
	}
	if payload.ID == "" {
		payload.ID = NewID()
	}
	task := &QueueTask{ctx: ctx, payload: payload, priority: priority}
	if err := wp.taskStorage.SaveTask(task); err != nil {
		return err
	}
	wp.taskQueueLock.Lock()
	defer wp.taskQueueLock.Unlock()
	taskSize := int64(utils.SizeOf(payload))
	if atomic.LoadInt64(&wp.metrics.TotalMemoryUsed)+taskSize > wp.maxMemoryLoad && wp.maxMemoryLoad > 0 {
		wp.storeInOverflow(task)
		return fmt.Errorf("max memory load reached, task stored in overflow buffer")
	}
	heap.Push(&wp.taskQueue, task)
	wp.Dispatch(wp.taskAvailableCond.Signal)
	wp.taskCompletionNotifier.Add(1)
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
	wp.overflowBuffer = append(wp.overflowBuffer, task)
	wp.overflowBufferLock.Unlock()
}

func (wp *Pool) startOverflowDrainer() {
	for {
		wp.drainOverflowBuffer()
		select {
		case <-wp.stop:
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (wp *Pool) drainOverflowBuffer() {
	wp.overflowBufferLock.Lock()
	overflowTasks := wp.overflowBuffer
	wp.overflowBuffer = nil
	wp.overflowBufferLock.Unlock()
	for _, task := range overflowTasks {
		select {
		case wp.taskNotify <- struct{}{}:
			wp.taskQueueLock.Lock()
			heap.Push(&wp.taskQueue, task)
			wp.taskQueueLock.Unlock()
		default:
			return
		}
	}
}

func (wp *Pool) Stop() {
	wp.gracefulShutdown = true
	wp.Pause()
	close(wp.stop)
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		wp.taskCompletionNotifier.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(wp.gracefulShutdownTimeout):
		wp.logger.Warn().Msg("Graceful shutdown timeout reached")
	}
	if wp.completionCallback != nil {
		wp.completionCallback()
	}
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
			wp.taskQueueLock.Lock()
			queueLen := len(wp.taskQueue)
			wp.taskQueueLock.Unlock()
			newWorkers := queueLen/5 + 1
			wp.AdjustWorkerCount(newWorkers)
		case <-wp.stop:
			return
		}
	}
}

// UpdateConfig updates pool configuration via a POOL_UPDATE command.
func (wp *Pool) UpdateConfig(newConfig *DynamicConfig) error {
	if err := validateDynamicConfig(newConfig); err != nil {
		return err
	}
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
	newWorkerCount := newConfig.NumberOfWorkers
	currentWorkers := int(atomic.LoadInt32(&wp.numOfWorkers))
	if newWorkerCount != currentWorkers && newWorkerCount > 0 {
		wp.adjustWorkers(newWorkerCount)
	}
	wp.logger.Info().Msg("Pool configuration updated via POOL_UPDATE")
	return nil
}
