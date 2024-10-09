package mq

import (
	"context"
	"fmt"
	"sync"

	"github.com/oarkflow/mq/utils"
)

type QueueTask struct {
	ctx     context.Context
	payload *Task
}

type Callback func(result Result, err error)

type Pool struct {
	totalMemoryUsed           int64
	completedTasks            int
	errorCount, maxMemoryLoad int64
	totalTasks, numOfWorkers  int
	taskQueue                 chan QueueTask
	wg                        sync.WaitGroup
	paused                    bool
	stop                      chan struct{}
	handler                   Handler
	callback                  Callback
}

func NewPool(
	numOfWorkers, taskQueueSize int,
	maxMemoryLoad int64,
	handler Handler, callback Callback) *Pool {
	return &Pool{
		numOfWorkers:  numOfWorkers,
		taskQueue:     make(chan QueueTask, taskQueueSize),
		stop:          make(chan struct{}),
		maxMemoryLoad: maxMemoryLoad,
		handler:       handler,
		callback:      callback,
	}
}

func (wp *Pool) Start() {
	for i := 0; i < wp.numOfWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
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
			if wp.handler != nil {
				result := wp.handler(task.ctx, task.payload)
				if result.Error != nil {
					wp.errorCount++
				} else {
					wp.completedTasks++
				}
				if wp.callback != nil {
					wp.callback(result, result.Error)
				}
			}

			wp.totalMemoryUsed -= taskSize
		case <-wp.stop:
			return
		}
	}
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

func (wp *Pool) PrintMetrics() {
	fmt.Printf("Total Tasks: %d, Completed Tasks: %d, Error Count: %d, Total Memory Used: %d bytes\n",
		wp.totalTasks, wp.completedTasks, wp.errorCount, wp.totalMemoryUsed)
}
