package main

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/log"
)

func getLogger() log.Logger {
	logger := log.DefaultLogger
	return logger
}

var logger = getLogger()

type MetricsRegistry interface {
	Register(metricName string, value interface{})
}

type InMemoryMetricsRegistry struct {
	metrics map[string]interface{}
	mu      sync.RWMutex
}

func NewInMemoryMetricsRegistry() *InMemoryMetricsRegistry {
	return &InMemoryMetricsRegistry{
		metrics: make(map[string]interface{}),
	}
}

func (m *InMemoryMetricsRegistry) Register(metricName string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics[metricName] = value
	logger.Info().Str("metric", metricName).Msgf("Registered new metric: %v", value)
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

func (dlq *DeadLetterQueue) Add(task *QueueTask) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	dlq.tasks = append(dlq.tasks, task)
	logger.Warn().Str("taskID", task.payload.ID).Msg("Task added to Dead Letter Queue")
}

var dlq = NewDeadLetterQueue()

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

type WarningThresholds struct {
	HighMemory    int64
	LongExecution time.Duration
}

var config = &DynamicConfig{
	Timeout:         10 * time.Second,
	BatchSize:       1,
	MaxMemoryLoad:   1024 * 1024 * 10,
	IdleTimeout:     5 * time.Minute,
	BackoffDuration: 2 * time.Second,
	MaxRetries:      3,
	ReloadInterval:  30 * time.Second,
	WarningThreshold: WarningThresholds{
		HighMemory:    1024 * 1024,
		LongExecution: 2 * time.Second,
	},
}

func startConfigReloader(pool *Pool) {
	go func() {
		ticker := time.NewTicker(config.ReloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:

				pool.timeout = config.Timeout
				pool.batchSize = config.BatchSize
				pool.maxMemoryLoad = config.MaxMemoryLoad
				pool.idleTimeout = config.IdleTimeout
				pool.backoffDuration = config.BackoffDuration
				pool.maxRetries = config.MaxRetries
				pool.thresholds = ThresholdConfig{
					HighMemory:    config.WarningThreshold.HighMemory,
					LongExecution: config.WarningThreshold.LongExecution,
				}
				logger.Info().Msg("Dynamic configuration reloaded")
			case <-pool.stop:
				return
			}
		}
	}()
}

type Plugin interface {
	Initialize(config interface{}) error
	BeforeTask(task *QueueTask)
	AfterTask(task *QueueTask, result Result)
}

type DefaultPlugin struct{}

func (dp *DefaultPlugin) Initialize(config interface{}) error {
	return nil
}

func (dp *DefaultPlugin) BeforeTask(task *QueueTask) {
	logger.Info().Str("taskID", task.payload.ID).Msg("BeforeTask plugin invoked")
}

func (dp *DefaultPlugin) AfterTask(task *QueueTask, result Result) {
	logger.Info().Str("taskID", task.payload.ID).Msg("AfterTask plugin invoked")
}

func acquireDistributedLock(taskID string) bool {

	return true
}

func releaseDistributedLock(taskID string) {

}

func validateTaskInput(task *Task) error {
	if task.Payload == nil {
		return errors.New("task payload cannot be nil")
	}
	return nil
}

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

type ScheduleOptions struct {
	Handler   Handler
	Callback  Callback
	Interval  time.Duration
	Overlap   bool
	Recurring bool
}

type SchedulerOption func(*ScheduleOptions)

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
	TimeOfDay  time.Time
	CronSpec   string
	DayOfWeek  []time.Weekday
	DayOfMonth []int
	Interval   time.Duration
	Recurring  bool
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
	if field == "*" {
		return fmt.Sprintf("every %s", fieldName), nil
	}
	values, err := parseCronValue(field)
	if err != nil {
		return "", fmt.Errorf("invalid %s field: %s", fieldName, err.Error())
	}
	return fmt.Sprintf("%s %s", strings.Join(values, ", "), fieldName), nil
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

type ExecutionHistory struct {
	Timestamp time.Time
	Result    Result
}

type Handler func(ctx context.Context, payload *Task) Result

type Scheduler struct {
	pool  *Pool
	tasks []ScheduledTask
	mu    sync.Mutex
}

func NewScheduler(pool *Pool) *Scheduler {
	return &Scheduler{pool: pool}
}

func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.tasks {
		go s.schedule(task)
	}
}

func (s *Scheduler) schedule(task ScheduledTask) {
	if s.pool.gracefulShutdown {
		return
	}
	if task.schedule.Interval > 0 {
		ticker := time.NewTicker(task.schedule.Interval)
		defer ticker.Stop()
		if task.schedule.Recurring {
			for {
				select {
				case <-ticker.C:
					if s.pool.gracefulShutdown {
						return
					}
					s.executeTask(task)
				case <-task.stop:
					return
				}
			}
		} else {
			select {
			case <-ticker.C:
				if s.pool.gracefulShutdown {
					return
				}
				s.executeTask(task)
			case <-task.stop:
				return
			}
		}
	} else if task.schedule.Recurring {
		for {
			now := time.Now()
			nextRun := task.getNextRunTime(now)
			select {
			case <-time.After(nextRun.Sub(now)):
				if s.pool.gracefulShutdown {
					return
				}
				s.executeTask(task)
			case <-task.stop:
				return
			}
		}
	}
}

func (s *Scheduler) executeTask(task ScheduledTask) {
	if !task.config.Overlap && s.pool.gracefulShutdown == false {

	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error().Str("taskID", task.payload.ID).Msgf("Recovered from panic: %v", r)
			}
		}()
		start := time.Now()
		for _, plug := range s.pool.plugins {
			plug.BeforeTask(&QueueTask{payload: task.payload})
		}

		if !acquireDistributedLock(task.payload.ID) {
			logger.Warn().Str("taskID", task.payload.ID).Msg("Failed to acquire distributed lock")
			return
		}
		defer releaseDistributedLock(task.payload.ID)
		result := task.handler(task.ctx, task.payload)
		execTime := time.Since(start).Milliseconds()
		if s.pool.diagnosticsEnabled {
			logger.Info().Str("taskID", task.payload.ID).Msgf("Executed in %d ms", execTime)
		}
		if result.Error != nil && s.pool.circuitBreaker.Enabled {
			newCount := atomic.AddInt32(&s.pool.circuitBreakerFailureCount, 1)
			if newCount >= int32(s.pool.circuitBreaker.FailureThreshold) {
				s.pool.circuitBreakerOpen = true
				logger.Warn().Msg("Scheduler circuit breaker opened due to errors")
				go func() {
					time.Sleep(s.pool.circuitBreaker.ResetTimeout)
					atomic.StoreInt32(&s.pool.circuitBreakerFailureCount, 0)
					s.pool.circuitBreakerOpen = false
					logger.Info().Msg("Scheduler circuit breaker reset")
				}()
			}
		}
		if task.config.Callback != nil {
			_ = task.config.Callback(task.ctx, result)
		}
		task.executionHistory = append(task.executionHistory, ExecutionHistory{Timestamp: time.Now(), Result: result})
		for _, plug := range s.pool.plugins {
			plug.AfterTask(&QueueTask{payload: task.payload}, result)
		}
		logger.Info().Str("taskID", task.payload.ID).Msg("Scheduled task executed")
	}()
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
		logger.Error().Err(err).Msg("Invalid CRON spec")
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
	if fieldSpec == "*" {
		return t
	}
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

func nextWeekday(t time.Time, weekday time.Weekday) time.Time {
	daysUntil := (int(weekday) - int(t.Weekday()) + 7) % 7
	if daysUntil == 0 {
		daysUntil = 7
	}
	return t.AddDate(0, 0, daysUntil)
}

func (s *Scheduler) AddTask(ctx context.Context, payload *Task, opts ...SchedulerOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateTaskInput(payload); err != nil {
		logger.Error().Err(err).Msg("Invalid task input")
		return
	}
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
	newTask := ScheduledTask{
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
	}
	s.tasks = append(s.tasks, newTask)
	go s.schedule(newTask)
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
	taskStorage                TaskStorage
	scheduler                  *Scheduler
	stop                       chan struct{}
	taskNotify                 chan struct{}
	workerAdjust               chan int
	handler                    Handler
	completionCallback         CompletionCallback
	taskAvailableCond          *sync.Cond
	callback                   Callback
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

func NewPool(numOfWorkers int, opts ...PoolOption) *Pool {
	pool := &Pool{
		stop:                    make(chan struct{}),
		taskNotify:              make(chan struct{}, numOfWorkers),
		batchSize:               config.BatchSize,
		timeout:                 config.Timeout,
		idleTimeout:             config.IdleTimeout,
		backoffDuration:         config.BackoffDuration,
		maxRetries:              config.MaxRetries,
		logger:                  logger,
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
}

func (wp *Pool) handleTask(task *QueueTask) {
	if err := validateTaskInput(task.payload); err != nil {
		logger.Error().Str("taskID", task.payload.ID).Msgf("Validation failed: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(task.ctx, wp.timeout)
	defer cancel()
	taskSize := int64(SizeOf(task.payload))
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, taskSize)
	atomic.AddInt64(&wp.metrics.TotalTasks, 1)
	startTime := time.Now()

	result := wp.handler(ctx, task.payload)
	executionTime := time.Since(startTime).Milliseconds()
	atomic.AddInt64(&wp.metrics.ExecutionTime, executionTime)

	if wp.thresholds.LongExecution > 0 && executionTime > int64(wp.thresholds.LongExecution.Milliseconds()) {
		logger.Warn().Str("taskID", task.payload.ID).Msgf("Exceeded execution time threshold: %d ms", executionTime)
	}
	if wp.thresholds.HighMemory > 0 && taskSize > wp.thresholds.HighMemory {
		logger.Warn().Str("taskID", task.payload.ID).Msgf("Memory usage %d exceeded threshold", taskSize)
	}

	if result.Error != nil {
		atomic.AddInt64(&wp.metrics.ErrorCount, 1)
		logger.Error().Str("taskID", task.payload.ID).Msgf("Error processing task: %v", result.Error)
		wp.backoffAndStore(task)
		if wp.circuitBreaker.Enabled {
			newCount := atomic.AddInt32(&wp.circuitBreakerFailureCount, 1)
			if newCount >= int32(wp.circuitBreaker.FailureThreshold) {
				wp.circuitBreakerOpen = true
				logger.Warn().Msg("Circuit breaker opened due to errors")
				go func() {
					time.Sleep(wp.circuitBreaker.ResetTimeout)
					atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
					wp.circuitBreakerOpen = false
					logger.Info().Msg("Circuit breaker reset")
				}()
			}
		}
	} else {
		atomic.AddInt64(&wp.metrics.CompletedTasks, 1)
		if wp.circuitBreaker.Enabled {
			atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
		}
	}

	if wp.diagnosticsEnabled {
		logger.Info().Str("taskID", task.payload.ID).Msgf("Task executed in %d ms", executionTime)
	}

	if wp.callback != nil {
		if err := wp.callback(ctx, result); err != nil {
			atomic.AddInt64(&wp.metrics.ErrorCount, 1)
			logger.Error().Str("taskID", task.payload.ID).Msgf("Callback error: %v", err)
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
		backoff := wp.backoffDuration * (1 << (task.retryCount - 1))
		jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
		sleepDuration := backoff + jitter
		logger.Info().Str("taskID", task.payload.ID).Msgf("Retry %d: sleeping for %s", task.retryCount, sleepDuration)
		time.Sleep(sleepDuration)
	} else {
		logger.Error().Str("taskID", task.payload.ID).Msg("Task failed after maximum retries")
		dlq.Add(task)
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
	if err := validateTaskInput(payload); err != nil {
		return fmt.Errorf("invalid task input: %w", err)
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
	taskSize := int64(SizeOf(payload))
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
		logger.Warn().Msg("Graceful shutdown timeout reached")
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

type ThresholdConfig struct {
	HighMemory    int64
	LongExecution time.Duration
}

type CircuitBreakerConfig struct {
	Enabled          bool
	FailureThreshold int
	ResetTimeout     time.Duration
}

type PoolOption func(*Pool)

func WithTaskQueueSize(size int) PoolOption {
	return func(p *Pool) {
		p.taskQueue = make(PriorityQueue, 0, size)
	}
}

func WithTaskTimeout(t time.Duration) PoolOption {
	return func(p *Pool) {
		p.timeout = t
	}
}

func WithCompletionCallback(callback func()) PoolOption {
	return func(p *Pool) {
		p.completionCallback = callback
	}
}

func WithMaxMemoryLoad(maxMemoryLoad int64) PoolOption {
	return func(p *Pool) {
		p.maxMemoryLoad = maxMemoryLoad
	}
}

func WithBatchSize(batchSize int) PoolOption {
	return func(p *Pool) {
		p.batchSize = batchSize
	}
}

func WithHandler(handler Handler) PoolOption {
	return func(p *Pool) {
		p.handler = handler
	}
}

func WithPoolCallback(callback Callback) PoolOption {
	return func(p *Pool) {
		p.callback = callback
	}
}

func WithTaskStorage(storage TaskStorage) PoolOption {
	return func(p *Pool) {
		p.taskStorage = storage
	}
}

func WithWarningThresholds(thresholds ThresholdConfig) PoolOption {
	return func(p *Pool) {
		p.thresholds = thresholds
	}
}

func WithDiagnostics(enabled bool) PoolOption {
	return func(p *Pool) {
		p.diagnosticsEnabled = enabled
	}
}

func WithMetricsRegistry(registry MetricsRegistry) PoolOption {
	return func(p *Pool) {
		p.metricsRegistry = registry
	}
}

func WithCircuitBreaker(config CircuitBreakerConfig) PoolOption {
	return func(p *Pool) {
		p.circuitBreaker = config
	}
}

func WithGracefulShutdown(timeout time.Duration) PoolOption {
	return func(p *Pool) {
		p.gracefulShutdownTimeout = timeout
	}
}

func WithPlugin(plugin Plugin) PoolOption {
	return func(p *Pool) {
		p.plugins = append(p.plugins, plugin)
	}
}

type Task struct {
	ID      string
	Payload interface{}
}

type Result struct {
	Error error
}

type QueueTask struct {
	ctx        context.Context
	payload    *Task
	priority   int
	retryCount int
	index      int
}

type PriorityQueue []*QueueTask

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	task := x.(*QueueTask)
	task.index = n
	*pq = append(*pq, task)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	task := old[n-1]
	task.index = -1
	*pq = old[0 : n-1]
	return task
}

func NewID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func SizeOf(payload interface{}) int {
	return 100
}

type TaskStorage interface {
	SaveTask(task *QueueTask) error
	FetchNextTask() (*QueueTask, error)
	DeleteTask(taskID string) error
	GetAllTasks() ([]*QueueTask, error)
}

type InMemoryTaskStorage struct {
	tasks map[string]*QueueTask
	mu    sync.RWMutex
}

func NewInMemoryTaskStorage() *InMemoryTaskStorage {
	return &InMemoryTaskStorage{
		tasks: make(map[string]*QueueTask),
	}
}

func (s *InMemoryTaskStorage) SaveTask(task *QueueTask) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.payload.ID] = task
	return nil
}

func (s *InMemoryTaskStorage) FetchNextTask() (*QueueTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, task := range s.tasks {
		delete(s.tasks, id)
		return task, nil
	}
	return nil, errors.New("no tasks")
}

func (s *InMemoryTaskStorage) DeleteTask(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tasks, taskID)
	return nil
}

func (s *InMemoryTaskStorage) GetAllTasks() ([]*QueueTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tasks := make([]*QueueTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func main() {
	storage := NewInMemoryTaskStorage()
	pool := NewPool(5,
		WithTaskStorage(storage),
		WithHandler(func(ctx context.Context, payload *Task) Result {
			logger.Info().Str("taskID", payload.ID).Msg("Processing task payload")
			time.Sleep(500 * time.Millisecond)
			return Result{}
		}),
		WithPoolCallback(func(ctx context.Context, result Result) error {
			logger.Info().Msg("Task callback invoked")
			return nil
		}),
		WithCircuitBreaker(CircuitBreakerConfig{
			Enabled:          true,
			FailureThreshold: 3,
			ResetTimeout:     5 * time.Second,
		}),
		WithWarningThresholds(ThresholdConfig{
			HighMemory:    config.WarningThreshold.HighMemory,
			LongExecution: config.WarningThreshold.LongExecution,
		}),
		WithDiagnostics(true),
		WithMetricsRegistry(NewInMemoryMetricsRegistry()),
		WithGracefulShutdown(10*time.Second),
		WithPlugin(&DefaultPlugin{}),
	)
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("Payload %d", i)
		task := &Task{Payload: payload}
		if err := pool.EnqueueTask(context.Background(), task, rand.Intn(10)); err != nil {
			logger.Error().Err(err).Msg("Failed to enqueue task")
		}
	}
	time.Sleep(5 * time.Second)
	metrics := pool.Metrics()
	logger.Info().Msgf("Metrics: %+v", metrics)
	pool.Stop()
	logger.Info().Msgf("Dead Letter Queue has %d tasks", len(dlq.tasks))
}
