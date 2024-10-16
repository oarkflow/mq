package mq

import (
	"container/heap"
	"context"
	"fmt"
	"strconv"
	"strings"
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

type ScheduleOptions struct {
	Handler   Handler
	Callback  Callback
	Overlap   bool
	Interval  time.Duration
	Recurring bool
}

type SchedulerOption func(*ScheduleOptions)

// Helper functions to create SchedulerOptions
func WithSchedulerHandler(handler Handler) SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Handler = handler
	}
}

func WithSchedulerCallback(callback Callback) SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Callback = callback
	}
}

func WithOverlap() SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Overlap = true
	}
}

func WithInterval(interval time.Duration) SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Interval = interval
	}
}

func WithRecurring() SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Recurring = true
	}
}

// defaultOptions returns the default scheduling options
func defaultSchedulerOptions() *ScheduleOptions {
	return &ScheduleOptions{
		Interval:  time.Minute,
		Recurring: true,
	}
}

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
	CronSpec   string
}

func (s *Schedule) ToHumanReadable() string {
	var sb strings.Builder
	if s.CronSpec != "" {
		cronDescription, err := parseCronSpec(s.CronSpec)
		if err != nil {
			sb.WriteString(fmt.Sprintf("Invalid CRON spec: %s\n", err.Error()))
		} else {
			sb.WriteString(fmt.Sprintf("CRON-based schedule: %s\n", cronDescription))
		}
	}
	if s.Interval > 0 {
		sb.WriteString(fmt.Sprintf("Recurring every %s\n", s.Interval))
	}
	if len(s.DayOfMonth) > 0 {
		sb.WriteString("Occurs on the following days of the month: ")
		for i, day := range s.DayOfMonth {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%d", day))
		}
		sb.WriteString("\n")
	}
	if len(s.DayOfWeek) > 0 {
		sb.WriteString("Occurs on the following days of the week: ")
		for i, day := range s.DayOfWeek {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(day.String())
		}
		sb.WriteString("\n")
	}
	if !s.TimeOfDay.IsZero() {
		sb.WriteString(fmt.Sprintf("Time of day: %s\n", s.TimeOfDay.Format("15:04")))
	}
	if s.Recurring {
		sb.WriteString("This schedule is recurring.\n")
	} else {
		sb.WriteString("This schedule is one-time.\n")
	}
	if sb.Len() == 0 {
		sb.WriteString("No schedule defined.")
	}
	return sb.String()
}

type CronSchedule struct {
	Minute     string
	Hour       string
	DayOfMonth string
	Month      string
	DayOfWeek  string
}

func (c CronSchedule) String() string {
	return fmt.Sprintf("At %s minute(s) past %s, on %s, during %s, every %s", c.Minute, c.Hour, c.DayOfWeek, c.Month, c.DayOfMonth)
}

func parseCronSpec(cronSpec string) (CronSchedule, error) {
	parts := strings.Fields(cronSpec)
	if len(parts) != 5 {
		return CronSchedule{}, fmt.Errorf("invalid CRON spec: expected 5 fields, got %d", len(parts))
	}
	minute, err := cronFieldToString(parts[0], "minute")
	if err != nil {
		return CronSchedule{}, err
	}
	hour, err := cronFieldToString(parts[1], "hour")
	if err != nil {
		return CronSchedule{}, err
	}
	dayOfMonth, err := cronFieldToString(parts[2], "day of the month")
	if err != nil {
		return CronSchedule{}, err
	}
	month, err := cronFieldToString(parts[3], "month")
	if err != nil {
		return CronSchedule{}, err
	}
	dayOfWeek, err := cronFieldToString(parts[4], "day of the week")
	if err != nil {
		return CronSchedule{}, err
	}
	return CronSchedule{
		Minute:     minute,
		Hour:       hour,
		DayOfMonth: dayOfMonth,
		Month:      month,
		DayOfWeek:  dayOfWeek,
	}, nil
}

func cronFieldToString(field string, fieldName string) (string, error) {
	switch field {
	case "*":
		return fmt.Sprintf("every %s", fieldName), nil
	default:
		values, err := parseCronValue(field)
		if err != nil {
			return "", fmt.Errorf("invalid %s field: %s", fieldName, err.Error())
		}
		return fmt.Sprintf("%s %s", strings.Join(values, ", "), fieldName), nil
	}
}

func parseCronValue(field string) ([]string, error) {
	var values []string
	ranges := strings.Split(field, ",")
	for _, r := range ranges {
		if strings.Contains(r, "-") {
			bounds := strings.Split(r, "-")
			if len(bounds) != 2 {
				return nil, fmt.Errorf("invalid range: %s", r)
			}
			start, err := strconv.Atoi(bounds[0])
			if err != nil {
				return nil, err
			}
			end, err := strconv.Atoi(bounds[1])
			if err != nil {
				return nil, err
			}
			for i := start; i <= end; i++ {
				values = append(values, strconv.Itoa(i))
			}
		} else {
			values = append(values, r)
		}
	}
	return values, nil
}

type Scheduler struct {
	tasks []ScheduledTask
	mu    sync.Mutex
	pool  *Pool
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

func NewScheduler(pool *Pool) *Scheduler {
	return &Scheduler{pool: pool}
}

func (task ScheduledTask) getNextRunTime(now time.Time) time.Time {
	if task.schedule.CronSpec != "" {
		return task.getNextCronRunTime(now)
	}
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

func (task ScheduledTask) getNextCronRunTime(now time.Time) time.Time {
	cronSpecs, err := parseCronSpec(task.schedule.CronSpec)
	if err != nil {
		fmt.Println(fmt.Sprintf("Invalid CRON spec format: %s", err.Error()))
		return now
	}
	nextRun := now
	nextRun = task.applyCronField(nextRun, cronSpecs.Minute, "minute")
	nextRun = task.applyCronField(nextRun, cronSpecs.Hour, "hour")
	nextRun = task.applyCronField(nextRun, cronSpecs.DayOfMonth, "day")
	nextRun = task.applyCronField(nextRun, cronSpecs.Month, "month")
	nextRun = task.applyCronField(nextRun, cronSpecs.DayOfWeek, "weekday")
	return nextRun
}

func (task ScheduledTask) applyCronField(t time.Time, fieldSpec string, unit string) time.Time {
	switch fieldSpec {
	case "*":
		return t
	default:
		value, _ := strconv.Atoi(fieldSpec)
		switch unit {
		case "minute":
			if t.Minute() > value {
				t = t.Add(time.Hour)
			}
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), value, 0, 0, t.Location())
		case "hour":
			if t.Hour() > value {
				t = t.AddDate(0, 0, 1)
			}
			t = time.Date(t.Year(), t.Month(), t.Day(), value, t.Minute(), 0, 0, t.Location())
		case "day":
			if t.Day() > value {
				t = t.AddDate(0, 1, 0)
			}
			t = time.Date(t.Year(), t.Month(), value, t.Hour(), t.Minute(), 0, 0, t.Location())
		case "month":
			if int(t.Month()) > value {
				t = t.AddDate(1, 0, 0)
			}
			t = time.Date(t.Year(), time.Month(value), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
		case "weekday":
			weekday := time.Weekday(value)
			for t.Weekday() != weekday {
				t = t.AddDate(0, 0, 1)
			}
		}
		return t
	}
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

func (s *Scheduler) AddTask(ctx context.Context, payload *Task, opts ...SchedulerOption) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a default options instance
	options := defaultSchedulerOptions()
	for _, opt := range opts {
		opt(options)
	}
	if options.Handler == nil {
		options.Handler = s.pool.handler
	}
	if options.Callback == nil {
		options.Callback = s.pool.callback
	}
	stop := make(chan struct{})

	// Create a new ScheduledTask using the provided options
	s.tasks = append(s.tasks, ScheduledTask{
		ctx:     ctx,
		handler: options.Handler,
		payload: payload,
		stop:    stop,
		config: SchedulerConfig{
			Callback: options.Callback,
			Overlap:  options.Overlap,
		},
		schedule: &Schedule{
			Interval:  options.Interval,
			Recurring: options.Recurring,
		},
	})

	// Start scheduling the task
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
	Scheduler                 *Scheduler
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
	pool.Scheduler = NewScheduler(pool)
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
	if payload.ID == "" {
		payload.ID = NewID()
	}
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
