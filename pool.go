package mq

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/oarkflow/mq/utils"
)

type QueueTask struct {
	ctx     context.Context
	payload *Task
}

type Callback func(ctx context.Context, result Result) error

type Pool struct {
	totalMemoryUsed           int64
	completedTasks            int
	errorCount, maxMemoryLoad int64
	totalTasks                int
	numOfWorkers              int32 // Change to int32 for atomic operations
	taskQueue                 chan QueueTask
	wg                        sync.WaitGroup
	paused                    bool
	stop                      chan struct{}
	handler                   Handler
	callback                  Callback
	conn                      net.Conn
	workerAdjust              chan int // Channel for adjusting workers dynamically
}

func NewPool(
	numOfWorkers, taskQueueSize int,
	maxMemoryLoad int64,
	handler Handler,
	callback Callback, conn net.Conn) *Pool {
	pool := &Pool{
		numOfWorkers:  int32(numOfWorkers),
		taskQueue:     make(chan QueueTask, taskQueueSize),
		stop:          make(chan struct{}),
		maxMemoryLoad: maxMemoryLoad,
		handler:       handler,
		callback:      callback,
		conn:          conn,
		workerAdjust:  make(chan int),
	}
	pool.Start(int(numOfWorkers))
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
		case task := <-wp.taskQueue:
			if wp.paused {
				continue
			}
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

func (wp *Pool) AddTask(ctx context.Context, payload *Task) error {
	task := QueueTask{ctx: ctx, payload: payload}
	taskSize := int64(utils.SizeOf(payload))
	if wp.totalMemoryUsed+taskSize > wp.maxMemoryLoad && wp.maxMemoryLoad > 0 {
		return fmt.Errorf("max memory load reached, cannot add task of size %d", taskSize)
	}

	select {
	case wp.taskQueue <- task:
		return nil
	default:
		return fmt.Errorf("task queue is full, cannot add task")
	}
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
	fmt.Printf("Total Tasks: %d, Completed Tasks: %d, Error Count: %d, Total Memory Used: %d bytes\n",
		wp.totalTasks, wp.completedTasks, wp.errorCount, wp.totalMemoryUsed)
}
