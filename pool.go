package mq

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/mq/utils"
)

type Callback func(ctx context.Context, result Result) error

type Metrics struct {
	TotalTasks      int64
	CompletedTasks  int64
	ErrorCount      int64
	TotalMemoryUsed int64
	TotalScheduled  int64
}

type Pool struct {
	taskStorage        TaskStorage
	taskQueue          PriorityQueue
	taskQueueLock      sync.Mutex
	stop               chan struct{}
	taskNotify         chan struct{}
	workerAdjust       chan int
	wg                 sync.WaitGroup
	maxMemoryLoad      int64
	numOfWorkers       int32
	metrics            Metrics
	paused             bool
	scheduler          *Scheduler
	overflowBufferLock sync.RWMutex
	overflowBuffer     []*QueueTask
	taskAvailableCond  *sync.Cond
	handler            Handler
	callback           Callback
	batchSize          int
}

func NewPool(numOfWorkers int, opts ...PoolOption) *Pool {
	pool := &Pool{
		stop:       make(chan struct{}),
		taskNotify: make(chan struct{}, numOfWorkers),
		batchSize:  1,
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
}

func (wp *Pool) worker() {
	defer wp.wg.Done()
	for {
		wp.taskAvailableCond.L.Lock()
		for len(wp.taskQueue) == 0 && !wp.paused {
			wp.taskAvailableCond.Wait()
		}
		wp.taskAvailableCond.L.Unlock()
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
	taskSize := int64(utils.SizeOf(task.payload))
	wp.metrics.TotalMemoryUsed += taskSize
	wp.metrics.TotalTasks++
	result := wp.handler(task.ctx, task.payload)
	if result.Error != nil {
		wp.metrics.ErrorCount++
	} else {
		wp.metrics.CompletedTasks++
	}
	if wp.callback != nil {
		if err := wp.callback(task.ctx, result); err != nil {
			wp.metrics.ErrorCount++
		}
	}
	_ = wp.taskStorage.DeleteTask(task.payload.ID)
	wp.metrics.TotalMemoryUsed -= taskSize
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
	if wp.metrics.TotalMemoryUsed+taskSize > wp.maxMemoryLoad && wp.maxMemoryLoad > 0 {
		wp.storeInOverflow(task)
		return fmt.Errorf("max memory load reached, task stored in overflow buffer of size %d", taskSize)
	}
	heap.Push(&wp.taskQueue, task)
	wp.taskAvailableCond.L.Lock()
	wp.taskAvailableCond.Signal()
	wp.taskAvailableCond.L.Unlock()
	return nil
}

func (wp *Pool) Pause() { wp.paused = true }

func (wp *Pool) Resume() { wp.paused = false }

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
	defer wp.overflowBufferLock.Unlock()
	for len(wp.overflowBuffer) > 0 {
		select {
		case wp.taskNotify <- struct{}{}:
			wp.taskQueueLock.Lock()
			heap.Push(&wp.taskQueue, wp.overflowBuffer[0])
			wp.overflowBuffer = wp.overflowBuffer[1:]
			wp.taskQueueLock.Unlock()
		default:
			return
		}
	}
}

func (wp *Pool) Stop() {
	close(wp.stop)
	wp.wg.Wait()
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
