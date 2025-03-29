package mq

import (
	"container/heap"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/mq/utils"
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

type Pool struct {
	taskStorage            TaskStorage
	scheduler              *Scheduler
	stop                   chan struct{}
	taskNotify             chan struct{}
	workerAdjust           chan int
	handler                Handler
	completionCallback     CompletionCallback
	taskAvailableCond      *sync.Cond
	callback               Callback
	taskQueue              PriorityQueue
	overflowBuffer         []*QueueTask
	metrics                Metrics
	wg                     sync.WaitGroup
	taskCompletionNotifier sync.WaitGroup
	timeout                time.Duration
	batchSize              int
	maxMemoryLoad          int64
	idleTimeout            time.Duration
	backoffDuration        time.Duration
	maxRetries             int
	overflowBufferLock     sync.RWMutex
	taskQueueLock          sync.Mutex
	numOfWorkers           int32
	paused                 bool
	logger                 *log.Logger
	gracefulShutdown       bool

	// New fields for enhancements:
	thresholds                 ThresholdConfig
	diagnosticsEnabled         bool
	metricsRegistry            MetricsRegistry
	circuitBreaker             CircuitBreakerConfig
	circuitBreakerOpen         bool
	circuitBreakerFailureCount int32
	gracefulShutdownTimeout    time.Duration
}

func NewPool(numOfWorkers int, opts ...PoolOption) *Pool {
	pool := &Pool{
		stop:            make(chan struct{}),
		taskNotify:      make(chan struct{}, numOfWorkers),
		batchSize:       1,
		timeout:         10 * time.Second,
		idleTimeout:     5 * time.Minute,
		backoffDuration: 2 * time.Second,
		maxRetries:      3, // Set max retries for failed tasks
		logger:          log.Default(),
	}
	pool.scheduler = NewScheduler(pool)
	pool.taskAvailableCond = sync.NewCond(&sync.Mutex{})
	for _, opt := range opts {
		opt(pool)
	}
	if len(pool.taskQueue) == 0 {
		pool.taskQueue = make(PriorityQueue, 0, 10)
	}
	heap.Init(&pool.taskQueue)
	pool.scheduler.Start()
	pool.Start(numOfWorkers)
	return pool
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
	// @TODO - Why was this done?
	//if len(tasks) > 0 {
	//	wp.taskCompletionNotifier.Done()
	//}
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

	// Warning thresholds check
	if wp.thresholds.LongExecution > 0 && executionTime > int64(wp.thresholds.LongExecution.Milliseconds()) {
		wp.logger.Printf("Warning: Task %s exceeded execution time threshold: %d ms", task.payload.ID, executionTime)
	}
	if wp.thresholds.HighMemory > 0 && taskSize > wp.thresholds.HighMemory {
		wp.logger.Printf("Warning: Task %s memory usage %d exceeded threshold", task.payload.ID, taskSize)
	}

	if result.Error != nil {
		atomic.AddInt64(&wp.metrics.ErrorCount, 1)
		wp.logger.Printf("Error processing task %s: %v", task.payload.ID, result.Error)
		wp.backoffAndStore(task)

		// Circuit breaker check
		if wp.circuitBreaker.Enabled {
			newCount := atomic.AddInt32(&wp.circuitBreakerFailureCount, 1)
			if newCount >= int32(wp.circuitBreaker.FailureThreshold) {
				wp.circuitBreakerOpen = true
				wp.logger.Println("Circuit breaker opened due to errors")
				go func() {
					time.Sleep(wp.circuitBreaker.ResetTimeout)
					atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
					wp.circuitBreakerOpen = false
					wp.logger.Println("Circuit breaker reset")
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
		wp.logger.Printf("Task %s executed in %d ms", task.payload.ID, executionTime)
	}

	if wp.callback != nil {
		if err := wp.callback(ctx, result); err != nil {
			atomic.AddInt64(&wp.metrics.ErrorCount, 1)
			wp.logger.Printf("Error in callback for task %s: %v", task.payload.ID, err)
		}
	}
	_ = wp.taskStorage.DeleteTask(task.payload.ID)
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, -taskSize)
}

func (wp *Pool) backoffAndStore(task *QueueTask) {
	if task.retryCount < wp.maxRetries {
		task.retryCount++
		wp.storeInOverflow(task)
		// Exponential backoff with jitter:
		backoff := wp.backoffDuration * (1 << (task.retryCount - 1))
		jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
		sleepDuration := backoff + jitter
		wp.logger.Printf("Task %s retry %d: sleeping for %s", task.payload.ID, task.retryCount, sleepDuration)
		time.Sleep(sleepDuration)
	} else {
		wp.logger.Printf("Task %s failed after maximum retries", task.payload.ID)
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
	// Check circuit breaker state
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
	wp.overflowBuffer = nil // Clear buffer
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

	// Graceful shutdown with timeout support
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		wp.taskCompletionNotifier.Wait()
		close(done)
	}()
	select {
	case <-done:
		// All workers finished gracefully.
	case <-time.After(wp.gracefulShutdownTimeout):
		wp.logger.Println("Graceful shutdown timeout reached")
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

// [Enhancement Suggestions]
// - Introduce a graceful shutdown sequence that prevents accepting new tasks.
// - Integrate a circuit breaker pattern to temporarily pause task processing if errors spike.
// - Emit events or metrics (with timestamps/retries) for monitoring worker health.
// - Refine the worker autoscaling or rate-limiting to respond to load changes.
// - Add proper context propagation (including cancellation and deadlines) across goroutines.
// - Incorporate centralized error logging and alerting based on failure types.
// - Consider unit tests and stress tests hooks to simulate production load.
