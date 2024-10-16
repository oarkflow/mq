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

type PriorityQueue []*QueueTask

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
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
	Callback Callback
	Overlap  bool
}

type ScheduledTask struct {
	ctx              context.Context
	handler          Handler
	payload          *Task
	config           SchedulerConfig
	schedule         *Schedule
	stop             chan struct{}
	executionHistory []ExecutionHistory
}

type Schedule struct {
	Interval   time.Duration
	DayOfWeek  []time.Weekday
	DayOfMonth []int
	TimeOfDay  time.Time
	Recurring  bool
}

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
	if task.schedule.Interval > 0 {
		ticker := time.NewTicker(task.schedule.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.executeTask(task)
			case <-task.stop:
				return
			}
		}
	} else if task.schedule.Recurring {
		for {
			now := time.Now()
			nextRun := task.getNextRunTime(now)
			if nextRun.After(now) {
				time.Sleep(nextRun.Sub(now))
			}
			s.executeTask(task)
		}
	}
}

func (task ScheduledTask) getNextRunTime(now time.Time) time.Time {
	if len(task.schedule.DayOfMonth) > 0 {
		for _, day := range task.schedule.DayOfMonth {
			nextRun := time.Date(now.Year(), now.Month(), day, task.schedule.TimeOfDay.Hour(), task.schedule.TimeOfDay.Minute(), 0, 0, now.Location())
			if nextRun.After(now) {
				return nextRun
			}
		}
		nextMonth := now.AddDate(0, 1, 0)
		return time.Date(nextMonth.Year(), nextMonth.Month(), task.schedule.DayOfMonth[0], task.schedule.TimeOfDay.Hour(), task.schedule.TimeOfDay.Minute(), 0, 0, now.Location())
	}
	if len(task.schedule.DayOfWeek) > 0 {
		for _, weekday := range task.schedule.DayOfWeek {
			nextRun := nextWeekday(now, weekday).Truncate(time.Minute).Add(task.schedule.TimeOfDay.Sub(time.Time{}))
			if nextRun.After(now) {
				return nextRun
			}
		}
	}
	return now
}

func nextWeekday(t time.Time, weekday time.Weekday) time.Time {
	daysUntil := (int(weekday) - int(t.Weekday()) + 7) % 7
	if daysUntil == 0 {
		daysUntil = 7
	}
	return t.AddDate(0, 0, daysUntil)
}

func (s *Scheduler) executeTask(task ScheduledTask) {
	if task.config.Overlap || len(task.schedule.DayOfWeek) == 0 {
		go func() {
			result := task.handler(task.ctx, task.payload)
			task.executionHistory = append(task.executionHistory, ExecutionHistory{Timestamp: time.Now(), Result: result})
			if task.config.Callback != nil {
				_ = task.config.Callback(task.ctx, result)
			}
			fmt.Printf("Executed scheduled task: %s\n", task.payload.ID)
		}()
	}
}

func (s *Scheduler) AddTask(ctx context.Context, handler Handler, payload *Task, config SchedulerConfig, schedule *Schedule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stop := make(chan struct{})
	s.tasks = append(s.tasks, ScheduledTask{ctx: ctx, handler: handler, payload: payload, stop: stop, config: config, schedule: schedule})
	go s.schedule(s.tasks[len(s.tasks)-1])
}

func (s *Scheduler) RemoveTask(payloadID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, task := range s.tasks {
		if task.payload.ID == payloadID {
			close(task.stop)
			s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
			break
		}
	}
}

type Pool struct {
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
		taskNotify:    make(chan struct{}, 1),
		maxMemoryLoad: maxMemoryLoad,
		handler:       handler,
		callback:      callback,
		workerAdjust:  make(chan int),
	}
	heap.Init(&pool.taskQueue)
	pool.Scheduler.Start()
	pool.Start(numOfWorkers)
	return pool
}

func (wp *Pool) Start(numWorkers int) {
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
			if len(wp.taskQueue) > 0 && !wp.paused {
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

func (wp *Pool) AddTask(ctx context.Context, payload *Task, priority int) error {
	wp.taskQueueLock.Lock()
	defer wp.taskQueueLock.Unlock()
	task := &QueueTask{ctx: ctx, payload: payload, priority: priority}
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
		wp.totalTasks, wp.completedTasks, wp.errorCount, wp.totalMemoryUsed, len(wp.Scheduler.tasks))
}

type ExecutionHistory struct {
	Timestamp time.Time
	Result    Result
}

func (s *Scheduler) PrintAllTasks() {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Println("Scheduled Tasks:")
	for _, task := range s.tasks {
		fmt.Printf("Task ID: %s, Next Execution: %s\n", task.payload.ID, task.getNextRunTime(time.Now()))
	}
}

func (s *Scheduler) PrintExecutionHistory(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.tasks {
		if task.payload.ID == taskID {
			fmt.Printf("Execution History for Task ID: %s\n", taskID)
			for _, history := range task.executionHistory {
				fmt.Printf("Timestamp: %s, Result: %v\n", history.Timestamp, history.Result.Error)
			}
			return
		}
	}
	fmt.Printf("No task found with ID: %s\n", taskID)
}
