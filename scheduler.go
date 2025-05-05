package mq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/log"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

var Logger = log.DefaultLogger

type ScheduleOptions struct {
	Handler      Handler
	Callback     Callback
	Interval     time.Duration
	Overlap      bool
	Recurring    bool
	ScheduleSpec string
}

type SchedulerOption func(*ScheduleOptions)

// WithSchedulerHandler sets the handler.
func WithSchedulerHandler(handler Handler) SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Handler = handler
	}
}

// WithSchedulerCallback sets the callback.
func WithSchedulerCallback(callback Callback) SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Callback = callback
	}
}

// WithOverlap indicates that overlapping executions are allowed.
func WithOverlap() SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Overlap = true
	}
}

// WithInterval sets a fixed interval.
func WithInterval(interval time.Duration) SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Interval = interval
	}
}

// WithRecurring indicates that the task should be rescheduled after execution.
func WithRecurring() SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.Recurring = true
	}
}

// WithScheduleSpec provides a schedule string (e.g., cron expression, @daily, @every 1h30m, etc.)
func WithScheduleSpec(spec string) SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.ScheduleSpec = spec
	}
}

// defaultSchedulerOptions returns the default scheduling options.
func defaultSchedulerOptions() *ScheduleOptions {
	return &ScheduleOptions{
		Interval:  time.Minute,
		Recurring: true,
	}
}

// --------------------------------------------------------
// Schedule and Cron Structures
// --------------------------------------------------------

// Schedule holds a schedule. It may be defined via:
// - A fixed time-interval (Interval)
// - A cron spec (CronSpec) string (which may be 5 or 6 fields)
// - A specific time of day, specific days of week or month.
type Schedule struct {
	TimeOfDay  time.Time      // Optional: time of day for one-off daily recurrence.
	CronSpec   string         // For cron-based scheduling.
	DayOfWeek  []time.Weekday // Optional: days of the week.
	DayOfMonth []int          // Optional: days of the month.
	Interval   time.Duration  // For duration-based scheduling (e.g. @every).
	Recurring  bool           // Indicates if schedule recurs.
}

// ToHumanReadable returns a human‑readable description of the schedule.
func (s *Schedule) ToHumanReadable() string {
	var sb strings.Builder
	if s.CronSpec != "" {
		if desc, err := parseCronSpecDescription(s.CronSpec); err != nil {
			sb.WriteString(fmt.Sprintf("Invalid CRON spec: %s\n", err.Error()))
		} else {
			sb.WriteString(fmt.Sprintf("CRON-based schedule: %s\n", desc))
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

// CronSchedule represents a parsed cron expression. It supports both 5‑field and 6‑field (extended) formats.
type CronSchedule struct {
	Seconds    string // Optional; if empty, assume "0"
	Minute     string
	Hour       string
	DayOfMonth string
	Month      string
	DayOfWeek  string
}

// String returns a summary string for the cron schedule.
func (c CronSchedule) String() string {
	if c.Seconds != "" && c.Seconds != "0" {
		return fmt.Sprintf("At %s seconds, %s minutes past %s, on %s, during %s, every %s",
			c.Seconds, c.Minute, c.Hour, c.DayOfWeek, c.Month, c.DayOfMonth)
	}
	return fmt.Sprintf("At %s minutes past %s, on %s, during %s, every %s",
		c.Minute, c.Hour, c.DayOfWeek, c.Month, c.DayOfMonth)
}

// --------------------------------------------------------
// Parsing: Special Schedule Strings and Cron Specs
// --------------------------------------------------------

// parseScheduleSpec inspects a schedule spec string and returns a Schedule.
// The spec may be a special keyword (starting with '@') or a standard cron expression.
func parseScheduleSpec(spec string) (*Schedule, error) {
	s := &Schedule{Recurring: true} // default recurring
	if strings.HasPrefix(spec, "@") {
		// Handle special cases.
		switch {
		case strings.HasPrefix(spec, "@every"):
			// Format: "@every <duration>", use time.ParseDuration.
			durationStr := strings.TrimSpace(strings.TrimPrefix(spec, "@every"))
			d, err := time.ParseDuration(durationStr)
			if err != nil {
				// If duration parsing fails, try to support days or weeks.
				// For example: "1d", "1w".
				if strings.HasSuffix(durationStr, "d") || strings.HasSuffix(durationStr, "w") {
					numStr := durationStr[:len(durationStr)-1]
					num, err2 := strconv.Atoi(numStr)
					if err2 != nil {
						return nil, fmt.Errorf("unable to parse duration in @every: %s", durationStr)
					}
					if strings.HasSuffix(durationStr, "d") {
						d = time.Duration(num) * 24 * time.Hour
					} else if strings.HasSuffix(durationStr, "w") {
						d = time.Duration(num) * 7 * 24 * time.Hour
					}
				} else {
					return nil, fmt.Errorf("unable to parse @every duration: %v", err)
				}
			}
			s.Interval = d
			return s, nil
		case spec == "@daily":
			s.CronSpec = "0 0 * * *"
			return s, nil
		case spec == "@weekly":
			s.CronSpec = "0 0 * * 0"
			return s, nil
		case spec == "@monthly":
			s.CronSpec = "0 0 1 * *"
			return s, nil
		case spec == "@yearly" || spec == "@annually":
			s.CronSpec = "0 0 1 1 *"
			return s, nil
		case spec == "@reboot":
			// For @reboot, you might want to run the task once at startup.
			s.Recurring = false
			return s, nil
		default:
			return nil, fmt.Errorf("unknown special schedule: %s", spec)
		}
	} else {
		// Assume a standard cron spec
		s.CronSpec = spec
		return s, nil
	}
}

// parseCronSpecDescription parses a cron spec and returns a human‑readable description.
func parseCronSpecDescription(cronSpec string) (string, error) {
	cs, err := parseCronSpec(cronSpec)
	if err != nil {
		return "", err
	}
	return cs.String(), nil
}

// parseCronSpec parses a cron specification string, supporting either 5 fields or 6 (with seconds).
func parseCronSpec(spec string) (CronSchedule, error) {
	parts := strings.Fields(spec)
	if len(parts) == 5 {
		// Assume no seconds provided; use default "0"
		return CronSchedule{
			Seconds:    "0",
			Minute:     parts[0],
			Hour:       parts[1],
			DayOfMonth: parts[2],
			Month:      parts[3],
			DayOfWeek:  parts[4],
		}, nil
	} else if len(parts) == 6 {
		// Extended spec with seconds.
		return CronSchedule{
			Seconds:    parts[0],
			Minute:     parts[1],
			Hour:       parts[2],
			DayOfMonth: parts[3],
			Month:      parts[4],
			DayOfWeek:  parts[5],
		}, nil
	}
	return CronSchedule{}, fmt.Errorf("invalid CRON spec: expected 5 or 6 fields, got %d", len(parts))
}

// --------------------------------------------------------
// Helper: Checking if a time matches a cron field value.
// (Supports "*" and comma separated list of integers.)
//
// For simplicity, we assume all fields in the cron spec represent numeric values.
func matchesCronField(val int, field string) bool {
	if field == "*" {
		return true
	}
	// Support lists separated by commas.
	parts := strings.Split(field, ",")
	for _, p := range parts {
		// Trim any potential spaces.
		p = strings.TrimSpace(p)
		ival, err := strconv.Atoi(p)
		if err == nil && ival == val {
			return true
		}
	}
	return false
}

// checkTimeMatchesCron tests whether a given time t satisfies the cron expression.
func checkTimeMatchesCron(t time.Time, cs CronSchedule) bool {
	// Check seconds, minutes, hour, day, month, weekday.
	sec := t.Second()
	min := t.Minute()
	hour := t.Hour()
	day := t.Day()
	month := int(t.Month())
	weekday := int(t.Weekday()) // Sunday==0, match cron where Sunday==0

	if !matchesCronField(sec, cs.Seconds) {
		return false
	}
	if !matchesCronField(min, cs.Minute) {
		return false
	}
	if !matchesCronField(hour, cs.Hour) {
		return false
	}
	if !matchesCronField(day, cs.DayOfMonth) {
		return false
	}
	if !matchesCronField(month, cs.Month) {
		return false
	}
	if !matchesCronField(weekday, cs.DayOfWeek) {
		return false
	}
	return true
}

// nextCronRunTime computes the next time after now that matches the cron spec.
// For simplicity, it iterates minute by minute (or second by second if extended).
func nextCronRunTime(now time.Time, cs CronSchedule) time.Time {
	// We'll search up to a year ahead.
	searchLimit := now.AddDate(1, 0, 0)
	t := now.Add(time.Second) // start a second later
	for !t.After(now) {
		t = t.Add(time.Second)
	}
	// If seconds field is in use (not just "0"), iterate second-by-second.
	for t.Before(searchLimit) {
		if checkTimeMatchesCron(t, cs) {
			return t
		}
		// Increment by one second if seconds precision is needed;
		// otherwise, for minute-level precision, you can use t = t.Add(time.Minute)
		if cs.Seconds != "0" && cs.Seconds != "*" {
			t = t.Add(time.Second)
		} else {
			t = t.Add(time.Minute)
		}
	}
	// Fallback: return the search limit if no matching time is found.
	return searchLimit
}

// --------------------------------------------------------
// Scheduled Task and Scheduler Structures
// --------------------------------------------------------

// ScheduledTask represents a scheduled job.
type ScheduledTask struct {
	ctx              context.Context
	handler          Handler
	payload          *Task
	config           SchedulerConfig
	schedule         *Schedule
	stop             chan struct{}
	executionHistory []ExecutionHistory
	running          int32
	id               string
}

type SchedulerConfig struct {
	Callback Callback
	Overlap  bool
}

type ExecutionHistory struct {
	Timestamp time.Time
	Result    Result
}

// Scheduler manages scheduling and executing tasks.
type Scheduler struct {
	pool    *Pool
	storage storage.IMap[string, *ScheduledTask] // added storage field
}

// New functional option type for Scheduler.
type SchedulerOpt func(*Scheduler)

// WithStorage sets the storage for ScheduledTasks.
func WithStorage(sm storage.IMap[string, *ScheduledTask]) SchedulerOpt {
	return func(s *Scheduler) {
		s.storage = sm
	}
}

// Update the NewScheduler constructor to use SchedulerOpt.
func NewScheduler(pool *Pool, opts ...SchedulerOpt) *Scheduler {
	s := &Scheduler{
		pool:    pool,
		storage: memory.New[string, *ScheduledTask](),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start begins executing scheduled tasks.
func (s *Scheduler) Start() {
	s.storage.ForEach(func(_ string, task *ScheduledTask) bool {
		go s.schedule(task)
		return true
	})
}

func (s *Scheduler) Close() error {
	s.pool.Stop()
	return nil
}

// schedule dispatches task execution based on its schedule.
func (s *Scheduler) schedule(task *ScheduledTask) {
	// Use the task context for cancellation.
	ctx := task.ctx
	// Main scheduling loop.
	if task.schedule.Interval > 0 {
		// Duration-based scheduling (@every).
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
				case <-ctx.Done():
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
			case <-ctx.Done():
				return
			}
		}
	} else if task.schedule.CronSpec != "" {
		// Cron-based scheduling.
		cs, err := parseCronSpec(task.schedule.CronSpec)
		if err != nil {
			Logger.Error().Err(err).Msg("Invalid CRON spec")
			return
		}
		for {
			now := time.Now()
			nextRun := nextCronRunTime(now, cs)
			select {
			case <-time.After(nextRun.Sub(now)):
				if s.pool.gracefulShutdown {
					return
				}
				s.executeTask(task)
			case <-task.stop:
				return
			case <-ctx.Done():
				return
			}
		}
	} else if !task.schedule.TimeOfDay.IsZero() {
		// A one-off daily time-of-day scheduling.
		for {
			now := time.Now()
			nextRun := time.Date(now.Year(), now.Month(), now.Day(),
				task.schedule.TimeOfDay.Hour(), task.schedule.TimeOfDay.Minute(), 0, 0, now.Location())
			if !nextRun.After(now) {
				nextRun = nextRun.AddDate(0, 0, 1)
			}
			select {
			case <-time.After(nextRun.Sub(now)):
				if s.pool.gracefulShutdown {
					return
				}
				s.executeTask(task)
				if !task.schedule.Recurring {
					return
				}
			case <-task.stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

// executeTask runs the task. It checks the overlap setting and uses context cancellation.
func (s *Scheduler) executeTask(task *ScheduledTask) {
	// If overlapping executions are not allowed, use an atomic flag.
	if !task.config.Overlap {
		if !atomic.CompareAndSwapInt32(&task.running, 0, 1) {
			Logger.Warn().Str("taskID", task.payload.ID).Msg("Skipping execution due to overlap configuration")
			return
		}
	}
	go func() {
		_, cancelSpan := startSpan("executeTask")
		defer cancelSpan()
		defer RecoverPanic(RecoverTitle)
		start := time.Now()
		for _, plug := range s.pool.plugins {
			plug.BeforeTask(getQueueTask())
		}
		if !acquireDistributedLock(task.payload.ID) {
			Logger.Warn().Str("taskID", task.payload.ID).Msg("Failed to acquire distributed lock")
			if !task.config.Overlap {
				atomic.StoreInt32(&task.running, 0)
			}
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
		if !task.config.Overlap {
			atomic.StoreInt32(&task.running, 0)
		}
	}()
}

// AddTask adds a new scheduled task using the supplied context, payload, and options.
func (s *Scheduler) AddTask(ctx context.Context, payload *Task, opts ...SchedulerOption) string {
	var hasDuplicate bool
	if payload.DedupKey != "" {
		s.storage.ForEach(func(_ string, task *ScheduledTask) bool {
			if task.payload.DedupKey == payload.DedupKey {
				hasDuplicate = true
				Logger.Warn().Str("dedup", payload.DedupKey).Msg("Duplicate scheduled task prevented")
			}
			return true
		})
	}
	if hasDuplicate {
		return ""
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

	// Determine the schedule from ScheduleSpec or Interval.
	var sched *Schedule
	var err error
	if options.ScheduleSpec != "" {
		sched, err = parseScheduleSpec(options.ScheduleSpec)
		if err != nil {
			Logger.Error().Err(err).Msg("Failed to parse schedule spec; defaulting to interval-based schedule")
			sched = &Schedule{Interval: options.Interval, Recurring: options.Recurring}
		}
	} else {
		sched = &Schedule{Interval: options.Interval, Recurring: options.Recurring}
	}

	stop := make(chan struct{})
	newTask := &ScheduledTask{
		id:      xid.New().String(),
		ctx:     ctx,
		handler: options.Handler,
		payload: payload,
		stop:    stop,
		config: SchedulerConfig{
			Callback: options.Callback,
			Overlap:  options.Overlap,
		},
		schedule: sched,
	}
	s.storage.Set(newTask.id, newTask)
	go s.schedule(newTask)
	return newTask.id
}

// RemoveTask stops and removes a task by its payload ID.
func (s *Scheduler) RemoveTask(id string) error {
	task, ok := s.storage.Get(id)
	if !ok {
		return fmt.Errorf("No task found with ID: %s\n", id)
	}
	close(task.stop)
	if s.storage != nil {
		s.storage.Del(id)
	}
	return nil
}

// PrintAllTasks prints a summary of all scheduled tasks.
func (s *Scheduler) PrintAllTasks() {
	fmt.Println("Scheduled Tasks:")
	s.storage.ForEach(func(_ string, task *ScheduledTask) bool {
		fmt.Printf("Task ID: %s, Next Execution: %s\n", task.payload.ID, task.getNextRunTime(time.Now()))
		return true
	})
}

// PrintExecutionHistory prints the execution history for a task by its ID.
func (s *Scheduler) PrintExecutionHistory(id string) error {
	task, ok := s.storage.Get(id)
	if !ok {
		return fmt.Errorf("No task found with ID: %s\n", id)
	}
	for _, history := range task.executionHistory {
		fmt.Printf("Timestamp: %s, Result: %v\n", history.Timestamp, history.Result.Error)
	}
	return nil
}

// getNextRunTime computes the next run time for the task.
func (task *ScheduledTask) getNextRunTime(now time.Time) time.Time {
	if task.schedule.Interval > 0 {
		return now.Add(task.schedule.Interval)
	}
	if task.schedule.CronSpec != "" {
		cs, err := parseCronSpec(task.schedule.CronSpec)
		if err != nil {
			Logger.Error().Err(err).Msg("Invalid CRON spec")
			return now
		}
		return nextCronRunTime(now, cs)
	}
	if !task.schedule.TimeOfDay.IsZero() {
		nextRun := time.Date(now.Year(), now.Month(), now.Day(),
			task.schedule.TimeOfDay.Hour(), task.schedule.TimeOfDay.Minute(), 0, 0, now.Location())
		if !nextRun.After(now) {
			nextRun = nextRun.AddDate(0, 0, 1)
		}
		return nextRun
	}
	// For DayOfWeek or DayOfMonth based scheduling, you could add additional logic.
	return now
}

// New type to hold scheduled task information.
type TaskInfo struct {
	TaskID      string    `json:"task_id"`
	NextRunTime time.Time `json:"next_run_time"`
}

// ListScheduledTasks returns details of all scheduled tasks along with their next run time.
func (s *Scheduler) ListScheduledTasks() []TaskInfo {
	now := time.Now()
	infos := make([]TaskInfo, 0, s.storage.Size())
	s.storage.ForEach(func(_ string, task *ScheduledTask) bool {
		infos = append(infos, TaskInfo{
			TaskID:      task.payload.ID,
			NextRunTime: task.getNextRunTime(now),
		})
		return true
	})
	return infos
}

// --------------------------------------------------------
// Additional Helper Functions and Stubs
// --------------------------------------------------------

// startSpan is a stub for tracing span creation.
func startSpan(operation string) (context.Context, func()) {
	startTime := time.Now()
	Logger.Info().Str("operation", operation).Msg("Span started")
	ctx := context.WithValue(context.Background(), "traceID", fmt.Sprintf("%d", startTime.UnixNano()))
	return ctx, func() {
		duration := time.Since(startTime)
		Logger.Info().Str("operation", operation).Msgf("Span ended; duration: %v", duration)
	}
}

// acquireDistributedLock is a stub for distributed locking.
func acquireDistributedLock(taskID string) bool {
	Logger.Info().Str("taskID", taskID).Msg("Acquiring distributed lock (stub)")
	return true
}

// releaseDistributedLock is a stub for releasing a distributed lock.
func releaseDistributedLock(taskID string) {
	Logger.Info().Str("taskID", taskID).Msg("Releasing distributed lock (stub)")
}

var taskPool = sync.Pool{
	New: func() any { return new(Task) },
}

var queueTaskPool = sync.Pool{
	New: func() any { return new(QueueTask) },
}

func getQueueTask() *QueueTask {
	return queueTaskPool.Get().(*QueueTask)
}

// CircuitBreaker holds configuration for error threshold detection.
type CircuitBreaker struct {
	Enabled          bool
	FailureThreshold int
	ResetTimeout     time.Duration
}
