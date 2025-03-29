package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------- Logger Setup ----------------------

var logger = log.New(os.Stdout, "", log.LstdFlags)

// ---------------------- Distributed Lock ----------------------

// DummyDistributedLock simulates a distributed lock mechanism.
type DummyDistributedLock struct {
	locks map[string]struct{}
	mu    sync.Mutex
}

var distLock = &DummyDistributedLock{locks: make(map[string]struct{})}

func acquireDistributedLock(taskID string) bool {
	distLock.mu.Lock()
	defer distLock.mu.Unlock()
	if _, exists := distLock.locks[taskID]; exists {
		logger.Printf("[WARN] Task %s lock already held", taskID)
		return false
	}
	distLock.locks[taskID] = struct{}{}
	logger.Printf("[INFO] Acquired distributed lock for task %s", taskID)
	return true
}

func releaseDistributedLock(taskID string) {
	distLock.mu.Lock()
	defer distLock.mu.Unlock()
	delete(distLock.locks, taskID)
	logger.Printf("[INFO] Released distributed lock for task %s", taskID)
}

// ---------------------- Dynamic Configuration ----------------------

type WarningThresholds struct {
	HighMemory    int64         `json:"high_memory"`
	LongExecution time.Duration `json:"long_execution"`
}

type DynamicConfig struct {
	Timeout          time.Duration     `json:"timeout"`
	BatchSize        int               `json:"batch_size"`
	MaxMemoryLoad    int64             `json:"max_memory_load"`
	IdleTimeout      time.Duration     `json:"idle_timeout"`
	BackoffDuration  time.Duration     `json:"backoff_duration"`
	MaxRetries       int               `json:"max_retries"`
	ReloadInterval   time.Duration     `json:"reload_interval"`
	WarningThreshold WarningThresholds `json:"warning_threshold"`
}

var config = &DynamicConfig{
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

func validateDynamicConfig(c *DynamicConfig) error {
	if c.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if c.BatchSize <= 0 {
		return errors.New("batch size must be > 0")
	}
	if c.MaxMemoryLoad <= 0 {
		return errors.New("max memory load must be > 0")
	}
	return nil
}

func reloadConfigFromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var newConfig DynamicConfig
	if err := decoder.Decode(&newConfig); err != nil {
		return err
	}
	if err := validateDynamicConfig(&newConfig); err != nil {
		return err
	}
	*config = newConfig
	logger.Printf("[INFO] Reloaded configuration from %s", path)
	return nil
}

func startConfigReloader(pool *Pool, configPath string) {
	go func() {
		ticker := time.NewTicker(config.ReloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := reloadConfigFromFile(configPath); err != nil {
					logger.Printf("[ERROR] Reload config error: %v", err)
					continue
				}
				// update pool parameters with new config
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
				logger.Printf("[INFO] Dynamic configuration reloaded")
			case <-pool.stop:
				return
			}
		}
	}()
}

// ---------------------- Metrics ----------------------

type Metrics struct {
	TotalTasks      int64
	CompletedTasks  int64
	ErrorCount      int64
	TotalMemoryUsed int64
	TotalScheduled  int64
	ExecutionTime   int64
}

// ---------------------- Plugin Interface ----------------------

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
	logger.Printf("[PLUGIN] Before executing task %s", task.payload.ID)
}

func (dp *DefaultPlugin) AfterTask(task *QueueTask, result Result) {
	logger.Printf("[PLUGIN] After executing task %s", task.payload.ID)
}

// ---------------------- Scheduler Types ----------------------

type Callback func(ctx context.Context, result Result) error

type Handler func(ctx context.Context, payload *Task) Result

type CompletionCallback func()

type SchedulerOption func(*ScheduleOptions)

type ScheduleOptions struct {
	Handler   Handler
	Callback  Callback
	Interval  time.Duration
	Overlap   bool
	Recurring bool
}

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

type ExecutionHistory struct {
	Timestamp time.Time
	Result    Result
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
		sb.WriteString("Occurs on days of month: ")
		for i, day := range s.DayOfMonth {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%d", day))
		}
		sb.WriteString("\n")
	}
	if len(s.DayOfWeek) > 0 {
		sb.WriteString("Occurs on days of week: ")
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
	return fmt.Sprintf("At %s minute(s) past %s, on %s, during %s, every %s",
		c.Minute, c.Hour, c.DayOfWeek, c.Month, c.DayOfMonth)
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
	dayOfMonth, err := cronFieldToString(parts[2], "day of month")
	if err != nil {
		return CronSchedule{}, err
	}
	month, err := cronFieldToString(parts[3], "month")
	if err != nil {
		return CronSchedule{}, err
	}
	dayOfWeek, err := cronFieldToString(parts[4], "day of week")
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

func nextWeekday(t time.Time, weekday time.Weekday) time.Time {
	daysUntil := (int(weekday) - int(t.Weekday()) + 7) % 7
	if daysUntil == 0 {
		daysUntil = 7
	}
	return t.AddDate(0, 0, daysUntil)
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
		logger.Printf("[ERROR] Invalid CRON spec: %v", err)
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
	if strings.HasPrefix(fieldSpec, "every") {
		return t
	}
	value, err := strconv.Atoi(strings.TrimSpace(strings.Split(fieldSpec, " ")[0]))
	if err != nil {
		return t
	}
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

// ---------------------- Task Types and Priority Queue ----------------------

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
	// Higher priority first
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
	// Simplified: in production, compute proper memory usage.
	return 100
}

// ---------------------- Task Storage ----------------------

// InMemoryTaskStorage simulates persistent storage. In production, replace with a real DB or persistent store.
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
	return nil, errors.New("no tasks available")
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

// ---------------------- Circuit Breaker ----------------------

type CircuitBreakerConfig struct {
	Enabled          bool
	FailureThreshold int
	ResetTimeout     time.Duration
}

type CircuitBreakerState int

const (
	Closed CircuitBreakerState = iota
	Open
	HalfOpen
)

// ---------------------- Pool ----------------------

type ThresholdConfig struct {
	HighMemory    int64
	LongExecution time.Duration
}

type PoolOption func(*Pool)

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
	gracefulShutdown           bool
	thresholds                 ThresholdConfig
	diagnosticsEnabled         bool
	metricsRegistry            MetricsRegistry
	circuitBreaker             CircuitBreakerConfig
	circuitBreakerOpen         int32 // use 0/1 flag for open state
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
		diagnosticsEnabled:      true,
		gracefulShutdownTimeout: 10 * time.Second,
		metricsRegistry:         NewInMemoryMetricsRegistry(),
		taskStorage:             NewInMemoryTaskStorage(),
	}
	pool.scheduler = NewScheduler(pool)
	pool.taskAvailableCond = sync.NewCond(&sync.Mutex{})
	for _, opt := range opts {
		opt(pool)
	}
	// initialize task queue
	pool.taskQueue = make(PriorityQueue, 0, 10)
	heap.Init(&pool.taskQueue)
	pool.scheduler.Start()
	pool.Start(numOfWorkers)
	// start dynamic config reloader (assumes config file "config.json" exists)
	startConfigReloader(pool, "config.json")
	go pool.dynamicWorkerScaler()
	go pool.startHealthServer()
	return pool
}

func (wp *Pool) Start(numWorkers int) {
	// load any stored tasks into the in-memory queue
	if storedTasks, err := wp.taskStorage.GetAllTasks(); err == nil {
		wp.taskQueueLock.Lock()
		for _, task := range storedTasks {
			heap.Push(&wp.taskQueue, task)
		}
		wp.taskQueueLock.Unlock()
	}
	// start workers
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
		// wait for task availability
		wp.taskAvailableCond.L.Lock()
		for len(wp.taskQueue) == 0 && !wp.paused && !wp.gracefulShutdown {
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
	if err := validateTaskInput(task.payload); err != nil {
		logger.Printf("[ERROR] Task %s validation failed: %v", task.payload.ID, err)
		return
	}
	ctx, cancel := context.WithTimeout(task.ctx, wp.timeout)
	defer cancel()
	taskSize := int64(SizeOf(task.payload))
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, taskSize)
	atomic.AddInt64(&wp.metrics.TotalTasks, 1)
	start := time.Now()
	// plugin hook before task execution
	for _, plug := range wp.plugins {
		plug.BeforeTask(task)
	}
	// acquire distributed lock
	if !acquireDistributedLock(task.payload.ID) {
		logger.Printf("[WARN] Failed to acquire lock for task %s", task.payload.ID)
		return
	}
	defer releaseDistributedLock(task.payload.ID)
	// execute task handler
	result := wp.handler(ctx, task.payload)
	execTime := time.Since(start).Milliseconds()
	atomic.AddInt64(&wp.metrics.ExecutionTime, execTime)
	if wp.thresholds.LongExecution > 0 && execTime > int64(wp.thresholds.LongExecution.Milliseconds()) {
		logger.Printf("[WARN] Task %s exceeded execution time threshold: %d ms", task.payload.ID, execTime)
	}
	if wp.thresholds.HighMemory > 0 && taskSize > wp.thresholds.HighMemory {
		logger.Printf("[WARN] Task %s memory usage %d exceeded threshold", task.payload.ID, taskSize)
	}
	// circuit breaker handling
	if result.Error != nil {
		atomic.AddInt64(&wp.metrics.ErrorCount, 1)
		logger.Printf("[ERROR] Error processing task %s: %v", task.payload.ID, result.Error)
		wp.backoffAndStore(task)
		if wp.circuitBreaker.Enabled {
			newCount := atomic.AddInt32(&wp.circuitBreakerFailureCount, 1)
			if newCount >= int32(wp.circuitBreaker.FailureThreshold) {
				atomic.StoreInt32(&wp.circuitBreakerOpen, 1)
				logger.Printf("[WARN] Circuit breaker opened due to errors")
				go func() {
					time.Sleep(wp.circuitBreaker.ResetTimeout)
					atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
					atomic.StoreInt32(&wp.circuitBreakerOpen, 0)
					logger.Printf("[INFO] Circuit breaker reset to closed state")
				}()
			}
		}
	} else {
		atomic.AddInt64(&wp.metrics.CompletedTasks, 1)
		if wp.circuitBreaker.Enabled {
			atomic.StoreInt32(&wp.circuitBreakerFailureCount, 0)
		}
	}
	// plugin hook after task execution
	for _, plug := range wp.plugins {
		plug.AfterTask(task, result)
	}
	logger.Printf("[INFO] Task %s executed in %d ms", task.payload.ID, execTime)
	if wp.callback != nil {
		if err := wp.callback(ctx, result); err != nil {
			atomic.AddInt64(&wp.metrics.ErrorCount, 1)
			logger.Printf("[ERROR] Callback error for task %s: %v", task.payload.ID, err)
		}
	}
	_ = wp.taskStorage.DeleteTask(task.payload.ID)
	atomic.AddInt64(&wp.metrics.TotalMemoryUsed, -taskSize)
	wp.metricsRegistry.Register("task_execution_time", execTime)
}

func (wp *Pool) backoffAndStore(task *QueueTask) {
	if task.retryCount < wp.maxRetries {
		task.retryCount++
		wp.storeInOverflow(task)
		backoff := wp.backoffDuration * (1 << (task.retryCount - 1))
		jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
		sleepDuration := backoff + jitter
		logger.Printf("[INFO] Task %s retry %d: sleeping for %s", task.payload.ID, task.retryCount, sleepDuration)
		time.Sleep(sleepDuration)
	} else {
		logger.Printf("[ERROR] Task %s failed after maximum retries", task.payload.ID)
		// In production, add the task to a Dead Letter Queue
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
	if wp.circuitBreaker.Enabled && atomic.LoadInt32(&wp.circuitBreakerOpen) == 1 {
		return fmt.Errorf("circuit breaker open, task rejected")
	}
	if err := validateTaskInput(payload); err != nil {
		return fmt.Errorf("invalid task input: %w", err)
	}
	if payload.ID == "" {
		payload.ID = NewID()
	}
	task := &QueueTask{
		ctx:      ctx,
		payload:  payload,
		priority: priority,
	}
	logger.Printf("[INFO] Enqueuing task %s", payload.ID)
	if err := wp.taskStorage.SaveTask(task); err != nil {
		return err
	}
	wp.taskQueueLock.Lock()
	heap.Push(&wp.taskQueue, task)
	wp.taskQueueLock.Unlock()
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
		logger.Printf("[WARN] Graceful shutdown timeout reached")
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
			logger.Printf("[INFO] Auto-scaling: queue length %d, adjusting workers to %d", queueLen, newWorkers)
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
		fmt.Fprintf(w, "status: %s\nworkers: %d\nqueueLength: %d\n", status, atomic.LoadInt32(&wp.numOfWorkers), len(wp.taskQueue))
	})
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		logger.Printf("[INFO] Starting health server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("[ERROR] Health server error: %v", err)
		}
	}()

	go func() {
		<-wp.stop
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Printf("[ERROR] Health server shutdown failed: %v", err)
		} else {
			logger.Printf("[INFO] Health server shutdown gracefully")
		}
	}()
}

// ---------------------- Metrics Registry ----------------------

type MetricsRegistry interface {
	Register(metricName string, value interface{})
	Increment(metricName string)
	Get(metricName string) interface{}
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
		logger.Printf("[METRICS] Registered %s: %d", metricName, v)
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

// ---------------------- Scheduler ----------------------

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
	// Use interval scheduling if defined
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
	go func() {
		start := time.Now()
		// Plugin hook: before task execution
		for _, plug := range s.pool.plugins {
			plug.BeforeTask(getQueueTaskClone(task.payload))
		}
		if !acquireDistributedLock(task.payload.ID) {
			logger.Printf("[WARN] Scheduler failed to acquire lock for task %s", task.payload.ID)
			return
		}
		defer releaseDistributedLock(task.payload.ID)
		result := task.handler(task.ctx, task.payload)
		execTime := time.Since(start).Milliseconds()
		logger.Printf("[INFO] Scheduled task %s executed in %d ms", task.payload.ID, execTime)
		if task.config.Callback != nil {
			_ = task.config.Callback(task.ctx, result)
		}
		task.executionHistory = append(task.executionHistory, ExecutionHistory{Timestamp: time.Now(), Result: result})
		// Plugin hook: after task execution
		for _, plug := range s.pool.plugins {
			plug.AfterTask(getQueueTaskClone(task.payload), result)
		}
	}()
}

func getQueueTaskClone(payload *Task) *QueueTask {
	return &QueueTask{payload: payload}
}

// ---------------------- Task Validation ----------------------

func validateTaskInput(task *Task) error {
	if task.Payload == nil {
		return errors.New("task payload cannot be nil")
	}
	logger.Printf("[INFO] Task %s validated", task.ID)
	return nil
}

// ---------------------- Pool Options ----------------------

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

// ---------------------- Main ----------------------

func main() {
	// Sample handler that simulates work.
	handler := func(ctx context.Context, task *Task) Result {
		logger.Printf("[HANDLER] Processing task %s with payload: %v", task.ID, task.Payload)
		// Simulate processing time
		time.Sleep(500 * time.Millisecond)
		// Simulate random error
		if rand.Intn(10) < 2 {
			return Result{Error: errors.New("simulated processing error")}
		}
		return Result{}
	}

	// Sample callback after task processing.
	callback := func(ctx context.Context, result Result) error {
		if result.Error != nil {
			logger.Printf("[CALLBACK] Task completed with error: %v", result.Error)
		} else {
			logger.Printf("[CALLBACK] Task completed successfully")
		}
		return nil
	}

	// Create pool with default options and a default plugin.
	pool := NewPool(3,
		WithHandler(handler),
		WithPoolCallback(callback),
		WithCircuitBreaker(CircuitBreakerConfig{
			Enabled:          true,
			FailureThreshold: 3,
			ResetTimeout:     10 * time.Second,
		}),
		WithPlugin(&DefaultPlugin{}),
	)

	// Enqueue some sample tasks.
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("data-%d", i)
		task := &Task{
			ID:      "", // will be set by EnqueueTask
			Payload: payload,
		}
		if err := pool.EnqueueTask(context.Background(), task, rand.Intn(10)); err != nil {
			logger.Printf("[ERROR] Failed to enqueue task: %v", err)
		}
	}

	// Let the system run for a while.
	time.Sleep(15 * time.Second)

	// Print some metrics.
	metrics := pool.Metrics()
	logger.Printf("[METRICS] %+v", metrics)

	// Stop the pool gracefully.
	pool.Stop()
	logger.Printf("[INFO] Pool shutdown complete")
}
