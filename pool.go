package mq

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/oarkflow/mq/utils"
)

type Callback func(ctx context.Context, result Result) error

type Pool struct {
	taskStorage               TaskStorage
	taskQueue                 PriorityQueue
	taskQueueLock             sync.Mutex
	stop                      chan struct{}
	taskNotify                chan struct{}
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
	scheduler                 *Scheduler
	totalScheduledTasks       int
}

func NewPool(
	numOfWorkers, taskQueueSize int,
	maxMemoryLoad int64,
	handler Handler,
	callback Callback,
	storage TaskStorage) *Pool {
	pool := &Pool{
		taskQueue:     make(PriorityQueue, 0, taskQueueSize),
		stop:          make(chan struct{}),
		taskNotify:    make(chan struct{}, 1),
		maxMemoryLoad: maxMemoryLoad,
		handler:       handler,
		callback:      callback,
		taskStorage:   storage,
		workerAdjust:  make(chan int),
	}
	pool.scheduler = NewScheduler(pool)
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
}

func (wp *Pool) worker() {
	defer wp.wg.Done()
	for {
		select {
		case <-wp.taskNotify:
			wp.taskQueueLock.Lock()
			var task *QueueTask
			if len(wp.taskQueue) > 0 && !wp.paused {
				task = heap.Pop(&wp.taskQueue).(*QueueTask)
			}
			wp.taskQueueLock.Unlock()
			if task == nil && !wp.paused {
				var err error
				task, err = wp.taskStorage.FetchNextTask()
				if err != nil {

					continue
				}
			}
			if task != nil {
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
				if err := wp.taskStorage.DeleteTask(task.payload.ID); err != nil {

				}
				wp.totalMemoryUsed -= taskSize
			}
		case <-wp.stop:
			return
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
	if wp.totalMemoryUsed+taskSize > wp.maxMemoryLoad && wp.maxMemoryLoad > 0 {
		return fmt.Errorf("max memory load reached, cannot add task of size %d", taskSize)
	}
	heap.Push(&wp.taskQueue, task)
	select {
	case wp.taskNotify <- struct{}{}:
	default:
	}
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
	fmt.Printf("Total Tasks: %d, Completed Tasks: %d, Error Count: %d, Total Memory Used: %d bytes, Total Scheduled Tasks: %d\n",
		wp.totalTasks, wp.completedTasks, wp.errorCount, wp.totalMemoryUsed, len(wp.scheduler.tasks))
}

func (wp *Pool) Scheduler() *Scheduler {
	return wp.scheduler
}
