package mq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/log"
)

var Logger = log.DefaultLogger

type ScheduleOptions struct {
	Handler   Handler
	Callback  Callback
	Interval  time.Duration
	Overlap   bool
	Recurring bool
}

type SchedulerOption func(*ScheduleOptions)

// WithSchedulerHandler Helper functions to create SchedulerOptions
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

type Scheduler struct {
	pool  *Pool
	tasks []ScheduledTask
	mu    sync.Mutex
}

func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.tasks {
		go s.schedule(task)
	}
}

func (s *Scheduler) schedule(task ScheduledTask) {
	// Check for graceful shutdown
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
					// Check for graceful shutdown before executing
					if s.pool.gracefulShutdown {
						return
					}
					s.executeTask(task)
				case <-task.stop:
					return
				}
			}
		} else {
			// Execute once and return
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

func startSpan(operation string) (context.Context, func()) {
	startTime := time.Now()
	Logger.Info().Str("operation", operation).Msg("Span started")
	ctx := context.WithValue(context.Background(), "traceID", fmt.Sprintf("%d", startTime.UnixNano()))
	return ctx, func() {
		duration := time.Since(startTime)
		Logger.Info().Str("operation", operation).Msgf("Span ended; duration: %v", duration)
	}
}

func acquireDistributedLock(taskID string) bool {
	Logger.Info().Str("taskID", taskID).Msg("Acquiring distributed lock (stub)")
	return true
}

func releaseDistributedLock(taskID string) {
	Logger.Info().Str("taskID", taskID).Msg("Releasing distributed lock (stub)")
}

var taskPool = sync.Pool{
	New: func() interface{} { return new(Task) },
}
var queueTaskPool = sync.Pool{
	New: func() interface{} { return new(QueueTask) },
}

func getQueueTask() *QueueTask {
	return queueTaskPool.Get().(*QueueTask)
}

// Enhance executeTask with circuit breaker and diagnostics logging support.
func (s *Scheduler) executeTask(task ScheduledTask) {
	go func() {
		_, cancelSpan := startSpan("executeTask")
		defer cancelSpan()
		defer func() {
			if r := recover(); r != nil {
				Logger.Error().Str("taskID", task.payload.ID).Msgf("Recovered from panic: %v", r)
			}
		}()
		start := time.Now()
		for _, plug := range s.pool.plugins {
			plug.BeforeTask(getQueueTask())
		}
		if !acquireDistributedLock(task.payload.ID) {
			Logger.Warn().Str("taskID", task.payload.ID).Msg("Failed to acquire distributed lock")
			return
		}
		defer releaseDistributedLock(task.payload.ID)
		result := task.handler(task.ctx, task.payload)
		execTime := time.Since(start).Milliseconds()
		if s.pool.diagnosticsEnabled {
			Logger.Info().Str("taskID", task.payload.ID).Msgf("Executed in %d ms", execTime)
		}
		if result.Error != nil && s.pool.circuitBreaker.Enabled {
			newCount := atomic.AddInt32(&s.pool.circuitBreakerFailureCount, 1)
			if newCount >= int32(s.pool.circuitBreaker.FailureThreshold) {
				s.pool.circuitBreakerOpen = true
				Logger.Warn().Msg("Circuit breaker opened due to errors")
				go func() {
					time.Sleep(s.pool.circuitBreaker.ResetTimeout)
					atomic.StoreInt32(&s.pool.circuitBreakerFailureCount, 0)
					s.pool.circuitBreakerOpen = false
					Logger.Info().Msg("Circuit breaker reset to closed state")
				}()
			}
		}
		if task.config.Callback != nil {
			_ = task.config.Callback(task.ctx, result)
		}
		task.executionHistory = append(task.executionHistory, ExecutionHistory{Timestamp: time.Now(), Result: result})
		for _, plug := range s.pool.plugins {
			plug.AfterTask(getQueueTask(), result)
		}
		Logger.Info().Str("taskID", task.payload.ID).Msg("Scheduled task executed")
	}()
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
		Logger.Error().Err(err).Msg("Invalid CRON spec")
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
func validateTaskInput(task *Task) error {
	if task.Payload == nil {
		return errors.New("task payload cannot be nil")
	}
	Logger.Info().Str("taskID", task.ID).Msg("Task validated")
	return nil
}

func (s *Scheduler) AddTask(ctx context.Context, payload *Task, opts ...SchedulerOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateTaskInput(payload); err != nil {
		Logger.Error().Err(err).Msg("Invalid task input")
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
