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

type QueueTask struct {
	ctx      context.Context
	payload  *Task
	priority int
}

// PriorityQueue implements heap.Interface and holds QueueTasks.
type PriorityQueue []*QueueTask

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority // Higher priority first
}

func (pq PriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*QueueTask)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

type Callback func(ctx context.Context, result Result) error

type SchedulerConfig struct {
	WithCallback Callback
	WithOverlap  bool // true allows overlapping, false waits for previous execution to complete
}

type ScheduledTask struct {
	ctx       context.Context
	handler   Handler
	payload   *Task
	interval  time.Duration
	config    SchedulerConfig
	stop      chan struct{}
	execution chan struct{} // Channel to signal task execution status
}

// Scheduler manages scheduled tasks.
type Scheduler struct {
	tasks []ScheduledTask
	mu    sync.Mutex
}

func (s *Scheduler) Start() {
	for _, task := range s.tasks {
		go s.schedule(task)
	}
}

func (s *Scheduler) schedule(task ScheduledTask) {
	ticker := time.NewTicker(task.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if task.config.WithOverlap || len(task.execution) == 0 { // Check if task can execute
				task.execution <- struct{}{}
				go func() {
					defer func() { <-task.execution }() // Free the channel for the next execution
					result := task.handler(task.ctx, task.payload)
					if task.config.WithCallback != nil {
						task.config.WithCallback(task.ctx, result)
					}
					fmt.Printf("Executed scheduled task: %s\n", task.payload.ID)
				}()
			}
		case <-task.stop:
			return
		}
	}
}

func (s *Scheduler) AddTask(ctx context.Context, handler Handler, payload *Task, interval time.Duration, config SchedulerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stop := make(chan struct{})
	execution := make(chan struct{}, 1) // Buffer for one execution signal
	s.tasks = append(s.tasks, ScheduledTask{ctx: ctx, handler: handler, payload: payload, interval: interval, stop: stop, execution: execution, config: config})
	go s.schedule(s.tasks[len(s.tasks)-1]) // Start scheduling immediately
}

func (s *Scheduler) RemoveTask(payloadID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, task := range s.tasks {
		if task.payload.ID == payloadID {
			close(task.stop) // Stop the task
			// Remove the task from the slice
			s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
			break
		}
	}
}

type Pool struct {
	taskQueue                 PriorityQueue
	taskQueueLock             sync.Mutex
	stop                      chan struct{}
	handler                   Handler
	callback                  Callback
	workerAdjust              chan int
	wg                        sync.WaitGroup
	totalMemoryUsed           int64
	completedTasks            int
	errorCount, maxMemoryLoad int64
	totalTasks                int
	numOfWorkers              int32
	paused                    bool
	Scheduler                 Scheduler
	totalScheduledTasks       int
}

func NewPool(
	numOfWorkers, taskQueueSize int,
	maxMemoryLoad int64,
	handler Handler,
	callback Callback) *Pool {
	pool := &Pool{
		taskQueue:     make(PriorityQueue, 0, taskQueueSize),
		stop:          make(chan struct{}),
		maxMemoryLoad: maxMemoryLoad,
		handler:       handler,
		callback:      callback,
		workerAdjust:  make(chan int),
	}
	heap.Init(&pool.taskQueue) // Initialize the priority queue as a heap
	pool.Scheduler.Start()     // Start the scheduler
	pool.Start(numOfWorkers)
	return pool
}

func (wp *Pool) Start(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
	atomic.StoreInt32(&wp.numOfWorkers, int32(numWorkers))
	go wp.monitorWorkerAdjustments() // Monitor worker changes
}

func (wp *Pool) worker() {
	defer wp.wg.Done()
	for {
		select {
		case <-wp.stop:
			return
		default:
			wp.taskQueueLock.Lock()
			if len(wp.taskQueue) > 0 && !wp.paused {
				// Pop the highest priority task
				task := heap.Pop(&wp.taskQueue).(*QueueTask)
				wp.taskQueueLock.Unlock()

				taskSize := int64(utils.SizeOf(task.payload))
				wp.totalMemoryUsed += taskSize
				wp.totalTasks++

				result := wp.handler(task.ctx, task.payload)

				if result.Error != nil {
					wp.errorCount++
				} else {
					wp.completedTasks++
				}

				if wp.callback != nil {
					if err := wp.callback(task.ctx, result); err != nil {
						wp.errorCount++
					}
				}

				wp.totalMemoryUsed -= taskSize
			} else {
				wp.taskQueueLock.Unlock()
			}
		}
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

func (wp *Pool) AddTask(ctx context.Context, payload *Task, priority int) error {
	task := &QueueTask{ctx: ctx, payload: payload, priority: priority}
	taskSize := int64(utils.SizeOf(payload))

	if wp.totalMemoryUsed+taskSize > wp.maxMemoryLoad && wp.maxMemoryLoad > 0 {
		return fmt.Errorf("max memory load reached, cannot add task of size %d", taskSize)
	}

	wp.taskQueueLock.Lock()
	heap.Push(&wp.taskQueue, task) // Add task to priority queue
	wp.taskQueueLock.Unlock()

	return nil
}

func (wp *Pool) Pause() {
	wp.paused = true
}

func (wp *Pool) Resume() {
	wp.paused = false
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

func (wp *Pool) PrintMetrics() {
	fmt.Printf("Total Tasks: %d, Completed Tasks: %d, Error Count: %d, Total Memory Used: %d bytes, Scheduled Tasks: %d\n",
		wp.totalTasks, wp.completedTasks, wp.errorCount, wp.totalMemoryUsed, len(wp.Scheduler.tasks))
}
