package mq

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/mq/utils"

	"github.com/oarkflow/log"
)

type Callback func(ctx context.Context, result Result) error
type CompletionCallback func()

type Metrics struct {
	TotalTasks      int64
	CompletedTasks  int64
	ErrorCount      int64
	TotalMemoryUsed int64
	TotalScheduled  int64
	ExecutionTime   int64
}

type Plugin interface {
	Initialize(config interface{}) error
	BeforeTask(task *QueueTask)
	AfterTask(task *QueueTask, result Result)
}

type DefaultPlugin struct{}

func (dp *DefaultPlugin) Initialize(config interface{}) error { return nil }
func (dp *DefaultPlugin) BeforeTask(task *QueueTask) {
	Logger.Info().Str("taskID", task.payload.ID).Msg("BeforeTask plugin invoked")
}
func (dp *DefaultPlugin) AfterTask(task *QueueTask, result Result) {
	Logger.Info().Str("taskID", task.payload.ID).Msg("AfterTask plugin invoked")
}

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

type WarningThresholds struct {
	HighMemory    int64
	LongExecution time.Duration
}

type DynamicConfig struct {
	Timeout          time.Duration
	BatchSize        int
	MaxMemoryLoad    int64
	IdleTimeout      time.Duration
	BackoffDuration  time.Duration
	MaxRetries       int
	ReloadInterval   time.Duration
	WarningThreshold WarningThresholds
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
		HighMemory:    1 * 1024 * 1024,
		LongExecution: 2 * time.Second,
	},
}

type Pool struct {
	taskStorage                TaskStorage
	scheduler                  *Scheduler
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
	port                       int
}

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
		port:                    1234,
		dlq:                     NewDeadLetterQueue(),
		metricsRegistry:         NewInMemoryMetricsRegistry(),
		diagnosticsEnabled:      true,
		gracefulShutdownTimeout: 10 * time.Second,
	}
	pool.scheduler = NewScheduler(pool)
	pool.taskAvailableCond = sync.NewCond(&sync.Mutex{})
	for _, opt := range opts {
		opt(pool)
	}
	if pool.taskQueue == nil {
		pool.taskQueue = make(PriorityQueue, 0, 10)
	}
	heap.Init(&pool.taskQueue)
	pool.scheduler.Start()
	pool.Start(numOfWorkers)
	startConfigReloader(pool)
	go pool.dynamicWorkerScaler()
	go pool.startHealthServer()
	return pool
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
	go func() {
		ticker := time.NewTicker(Config.ReloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := validateDynamicConfig(Config); err != nil {
					Logger.Error().Err(err).Msg("Invalid dynamic config, skipping reload")
					continue
				}
				pool.timeout = Config.Timeout
				pool.batchSize = Config.BatchSize
				pool.maxMemoryLoad = Config.MaxMemoryLoad
				pool.idleTimeout = Config.IdleTimeout
				pool.backoffDuration = Config.BackoffDuration
				pool.maxRetries = Config.MaxRetries
				pool.thresholds = ThresholdConfig{
					HighMemory:    Config.WarningThreshold.HighMemory,
					LongExecution: Config.WarningThreshold.LongExecution,
				}
				Logger.Info().Msg("Dynamic configuration reloaded")
			case <-pool.stop:
				return
			}
		}
	}()
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
	taskSize := int64(utils.SizeOf(task.payload))
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, taskSize)
	atomic.AddInt64(&wp.metrics.TotalTasks, 1)
	startTime := time.Now()
	result := wp.handler(ctx, task.payload)
	executionTime := time.Since(startTime).Milliseconds()
	atomic.AddInt64(&wp.metrics.ExecutionTime, executionTime)
	if wp.thresholds.LongExecution > 0 && executionTime > wp.thresholds.LongExecution.Milliseconds() {
		wp.logger.Warn().Str("taskID", task.payload.ID).Msgf("Exceeded execution time threshold: %d ms", executionTime)
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
		// Reset failure count on success if using circuit breaker
		if wp.circuitBreaker.Enabled {
			atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
		}
	}

	// Diagnostics logging if enabled
	if wp.diagnosticsEnabled {
		wp.logger.Info().Str("taskID", task.payload.ID).Msgf("Task executed in %d ms", executionTime)
	}
	if wp.callback != nil {
		if err := wp.callback(ctx, result); err != nil {
			atomic.AddInt64(&wp.metrics.ErrorCount, 1)
			wp.logger.Error().Str("taskID", task.payload.ID).Msgf("Callback error: %v", err)
		}
	}
	_ = wp.taskStorage.DeleteTask(task.payload.ID)
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, -taskSize)
	wp.metricsRegistry.Register("task_execution_time", executionTime)
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

func (wp *Pool) Metrics() Metrics {
	wp.metrics.TotalScheduled = int64(len(wp.scheduler.tasks))
	return wp.metrics
}

func (wp *Pool) Scheduler() *Scheduler { return wp.scheduler }

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
			wp.logger.Info().Msgf("Auto-scaling: queue length %d, adjusting workers to %d", queueLen, newWorkers)
			wp.AdjustWorkerCount(newWorkers)
		case <-wp.stop:
			return
		}
	}
}

func (wp *Pool) startHealthServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := "OK"
		if wp.gracefulShutdown {
			status = "shutting down"
		}
		_, _ = fmt.Fprintf(w, "status: %s\nworkers: %d\nqueueLength: %d\n",
			status, atomic.LoadInt32(&wp.numOfWorkers), len(wp.taskQueue))
	})
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
	go func() {
		wp.logger.Info().Msg("Starting health server on :8080")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			wp.logger.Error().Err(err).Msg("Health server failed")
		}
	}()

	go func() {
		<-wp.stop
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			wp.logger.Error().Err(err).Msg("Health server shutdown failed")
		} else {
			wp.logger.Info().Msg("Health server shutdown gracefully")
		}
	}()
}
