package mq

import (
	"context"
	"expvar"
	"fmt"
	"math"
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
var totalTasks = expvar.NewInt("totalTasks")
var failedTasks = expvar.NewInt("failedTasks")
var execTimeExp = expvar.NewInt("executionTimeMs")

type ScheduleOptions struct {
	Handler      Handler
	Callback     Callback
	Interval     time.Duration
	Overlap      bool
	Recurring    bool
	ScheduleSpec string
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

func WithScheduleSpec(spec string) SchedulerOption {
	return func(opts *ScheduleOptions) {
		opts.ScheduleSpec = spec
	}
}

func defaultSchedulerOptions() *ScheduleOptions {
	return &ScheduleOptions{
		Interval:  time.Minute,
		Recurring: true,
	}
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

type CronSchedule struct {
	Seconds    string
	Minute     string
	Hour       string
	DayOfMonth string
	Month      string
	DayOfWeek  string
}

func (c CronSchedule) String() string {
	if c.Seconds != "" && c.Seconds != "0" {
		return fmt.Sprintf("At %s seconds, %s minutes past %s, on %s, during %s, every %s", c.Seconds, c.Minute, c.Hour, c.DayOfWeek, c.Month, c.DayOfMonth)
	}
	return fmt.Sprintf("At %s minutes past %s, on %s, during %s, every %s", c.Minute, c.Hour, c.DayOfWeek, c.Month, c.DayOfMonth)
}

func parseScheduleSpec(spec string) (*Schedule, error) {
	s := &Schedule{Recurring: true}
	if strings.HasPrefix(spec, "@") {
		switch {
		case strings.HasPrefix(spec, "@every"):
			durationStr := strings.TrimSpace(strings.TrimPrefix(spec, "@every"))
			d, err := time.ParseDuration(durationStr)
			if err != nil {
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
			s.Recurring = false
			return s, nil
		default:
			return nil, fmt.Errorf("unknown special schedule: %s", spec)
		}
	} else {
		s.CronSpec = spec
		return s, nil
	}
}

func parseCronSpecDescription(cronSpec string) (string, error) {
	cs, err := parseCronSpec(cronSpec)
	if err != nil {
		return "", err
	}
	return cs.String(), nil
}

func parseCronSpec(spec string) (CronSchedule, error) {
	parts := strings.Fields(spec)
	if len(parts) == 5 {
		return CronSchedule{
			Seconds:    "0",
			Minute:     parts[0],
			Hour:       parts[1],
			DayOfMonth: parts[2],
			Month:      parts[3],
			DayOfWeek:  parseCronFieldNames(parts[4], "dow"),
		}, nil
	} else if len(parts) == 6 {
		return CronSchedule{
			Seconds:    parts[0],
			Minute:     parts[1],
			Hour:       parts[2],
			DayOfMonth: parts[3],
			Month:      parts[4],
			DayOfWeek:  parseCronFieldNames(parts[5], "dow"),
		}, nil
	}
	return CronSchedule{}, fmt.Errorf("invalid CRON spec: expected 5 or 6 fields, got %d", len(parts))
}

func parseCronFieldNames(field string, fieldType string) string {
	lower := strings.ToLower(field)
	if fieldType == "dow" {
		mapping := map[string]string{"sun": "0", "mon": "1", "tue": "2", "wed": "3", "thu": "4", "fri": "5", "sat": "6"}
		parts := strings.Split(lower, ",")
		for i, p := range parts {
			p = strings.TrimSpace(p)
			if val, ok := mapping[p]; ok {
				parts[i] = val
			}
		}
		return strings.Join(parts, ",")
	}
	return field
}

func matchCronFieldEx(val int, field string) bool {
	if field == "*" {
		return true
	}
	tokens := strings.Split(field, ",")
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if strings.Contains(token, "/") {
			parts := strings.Split(token, "/")
			base := parts[0]
			step, err := strconv.Atoi(parts[1])
			if err != nil || step <= 0 {
				continue
			}
			if base == "*" {
				if val%step == 0 {
					return true
				}
			} else if strings.Contains(base, "-") {
				rangeParts := strings.Split(base, "-")
				if len(rangeParts) == 2 {
					start, err1 := strconv.Atoi(rangeParts[0])
					end, err2 := strconv.Atoi(rangeParts[1])
					if err1 == nil && err2 == nil && val >= start && val <= end && (val-start)%step == 0 {
						return true
					}
				}
			}
		} else if strings.Contains(token, "-") {
			rangeParts := strings.Split(token, "-")
			if len(rangeParts) == 2 {
				start, err1 := strconv.Atoi(rangeParts[0])
				end, err2 := strconv.Atoi(rangeParts[1])
				if err1 == nil && err2 == nil && val >= start && val <= end {
					return true
				}
			}
		} else {
			num, err := strconv.Atoi(token)
			if err == nil && val == num {
				return true
			}
		}
	}
	return false
}

func checkTimeMatchesCron(t time.Time, cs CronSchedule) bool {
	sec := t.Second()
	minute := t.Minute()
	hour := t.Hour()
	day := t.Day()
	month := int(t.Month())
	weekday := int(t.Weekday())
	if !matchCronFieldEx(sec, cs.Seconds) {
		return false
	}
	if !matchCronFieldEx(minute, cs.Minute) {
		return false
	}
	if !matchCronFieldEx(hour, cs.Hour) {
		return false
	}
	if !matchCronFieldEx(day, cs.DayOfMonth) {
		return false
	}
	if !matchCronFieldEx(month, cs.Month) {
		return false
	}
	if !matchCronFieldEx(weekday, cs.DayOfWeek) {
		return false
	}
	return true
}

func nextCronRunTime(now time.Time, cs CronSchedule) time.Time {
	searchLimit := now.AddDate(1, 0, 0)
	t := now.Add(time.Second)
	for !t.After(now) {
		t = t.Add(time.Second)
	}
	for t.Before(searchLimit) {
		if checkTimeMatchesCron(t, cs) {
			return t
		}
		if cs.Seconds != "0" && cs.Seconds != "*" {
			t = t.Add(time.Second)
		} else {
			t = t.Add(time.Minute)
		}
	}
	return searchLimit
}

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
	Callback   Callback
	Overlap    bool
	MaxRetries int
}

type ExecutionHistory struct {
	Timestamp time.Time
	Result    Result
}

type Scheduler struct {
	pool    *Pool
	storage storage.IMap[string, *ScheduledTask]
	wg      sync.WaitGroup
}

type SchedulerOpt func(*Scheduler)

func WithStorage(sm storage.IMap[string, *ScheduledTask]) SchedulerOpt {
	return func(s *Scheduler) {
		s.storage = sm
	}
}

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

func (s *Scheduler) Start() {
	s.storage.ForEach(func(_ string, task *ScheduledTask) bool {
		s.wg.Add(1)
		go s.schedule(task)
		return true
	})
}

func (s *Scheduler) Close() error {
	s.pool.Stop()
	s.wg.Wait()
	return nil
}

func (s *Scheduler) schedule(task *ScheduledTask) {
	defer s.wg.Done()
	ctx := task.ctx
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
		for {
			now := time.Now()
			nextRun := time.Date(now.Year(), now.Month(), now.Day(), task.schedule.TimeOfDay.Hour(), task.schedule.TimeOfDay.Minute(), 0, 0, now.Location())
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

func (s *Scheduler) executeTask(task *ScheduledTask) {
	if !task.config.Overlap {
		if !atomic.CompareAndSwapInt32(&task.running, 0, 1) {
			Logger.Warn().Str("taskID", task.payload.ID).Msg("Skipping execution due to overlap configuration")
			return
		}
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_, cancelSpan := startSpan("executeTask")
		defer cancelSpan()
		defer RecoverPanic(RecoverTitle)
		var attempt int
		var result Result
		var delay time.Duration = 100 * time.Millisecond
		for {
			start := time.Now()
			asyncPluginsBefore(s.pool.plugins)
			if !acquireLockWithRetry(task.payload.ID, 3) {
				Logger.Warn().Str("taskID", task.payload.ID).Msg("Failed to acquire distributed lock after retries")
				if !task.config.Overlap {
					atomic.StoreInt32(&task.running, 0)
				}
				return
			}
			result = task.handler(task.ctx, task.payload)
			releaseDistributedLock(task.payload.ID)
			elapsed := time.Since(start).Milliseconds()
			execTimeExp.Add(elapsed)
			if s.pool.diagnosticsEnabled {
				Logger.Info().Str("taskID", task.payload.ID).Msgf("Executed in %d ms", elapsed)
			}
			if result.Error == nil || attempt >= task.config.MaxRetries {
				break
			}
			attempt++
			time.Sleep(delay)
			delay = time.Duration(math.Min(float64(delay*2), float64(5*time.Second)))
		}
		if result.Error != nil {
			failedTasks.Add(1)
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
		asyncPluginsAfter(s.pool.plugins, result)
		Logger.Info().Str("taskID", task.payload.ID).Msg("Scheduled task executed")
		if !task.config.Overlap {
			atomic.StoreInt32(&task.running, 0)
		}
	}()
}

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
			Callback:   options.Callback,
			Overlap:    options.Overlap,
			MaxRetries: 3,
		},
		schedule: sched,
	}
	s.storage.Set(newTask.id, newTask)
	s.wg.Add(1)
	go s.schedule(newTask)
	totalTasks.Add(1)
	return newTask.id
}

func (s *Scheduler) RemoveTask(id string) error {
	task, ok := s.storage.Get(id)
	if !ok {
		return fmt.Errorf("No task found with ID: %s", id)
	}
	close(task.stop)
	s.storage.Del(id)
	return nil
}

func (s *Scheduler) PrintAllTasks() {
	fmt.Println("Scheduled Tasks:")
	s.storage.ForEach(func(_ string, task *ScheduledTask) bool {
		fmt.Printf("Task ID: %s, Next Execution: %s\n", task.payload.ID, task.getNextRunTime(time.Now()))
		return true
	})
}

func (s *Scheduler) PrintExecutionHistory(id string) error {
	task, ok := s.storage.Get(id)
	if !ok {
		return fmt.Errorf("No task found with ID: %s", id)
	}
	for _, history := range task.executionHistory {
		fmt.Printf("Timestamp: %s, Result: %v\n", history.Timestamp, history.Result.Error)
	}
	return nil
}

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

func (s *Scheduler) UpdateTask(id string, newSched *Schedule) error {
	task, ok := s.storage.Get(id)
	if !ok {
		return fmt.Errorf("No task found with ID: %s", id)
	}
	task.schedule = newSched
	return nil
}

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
		nextRun := time.Date(now.Year(), now.Month(), now.Day(), task.schedule.TimeOfDay.Hour(), task.schedule.TimeOfDay.Minute(), 0, 0, now.Location())
		if !nextRun.After(now) {
			nextRun = nextRun.AddDate(0, 0, 1)
		}
		return nextRun
	}
	return now
}

type TaskInfo struct {
	TaskID      string    `json:"task_id"`
	NextRunTime time.Time `json:"next_run_time"`
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

type DistributedLocker interface {
	Acquire(key string, ttl time.Duration) bool
	Release(key string)
}

type inMemoryLocker struct{}

var defaultDistributedLocker DistributedLocker = &inMemoryLocker{}

func (l *inMemoryLocker) Acquire(key string, ttl time.Duration) bool {
	now := time.Now().UnixNano()
	value, _ := lockerMap.LoadOrStore(key, &LockEntry{expiry: 0})
	entry := value.(*LockEntry)
	entry.mu.Lock()
	if entry.expiry < now {
		entry.expiry = now + int64(ttl)
		entry.mu.Unlock()
		return true
	}
	entry.mu.Unlock()
	return false
}

func (l *inMemoryLocker) Release(key string) {
	now := time.Now().UnixNano()
	if value, ok := lockerMap.Load(key); ok {
		entry := value.(*LockEntry)
		entry.mu.Lock()
		if entry.expiry > now {
			entry.expiry = 0
		}
		entry.mu.Unlock()
	}
}

var lockerMap sync.Map

type LockEntry struct {
	mu     sync.Mutex
	expiry int64
}

func acquireDistributedLock(taskID string) bool {
	return defaultDistributedLocker.Acquire(taskID, 30*time.Second)
}

func releaseDistributedLock(taskID string) {
	defaultDistributedLocker.Release(taskID)
}

func acquireLockWithRetry(taskID string, retries int) bool {
	var i int
	delay := 10 * time.Millisecond
	for i = 0; i < retries; i++ {
		if acquireDistributedLock(taskID) {
			return true
		}
		time.Sleep(delay)
		delay = time.Duration(math.Min(float64(delay*2), float64(200*time.Millisecond)))
	}
	return false
}

func asyncPluginsBefore(plugins []Plugin, timeout ...time.Duration) {
	var wg sync.WaitGroup
	dur := 1 * time.Second
	if len(timeout) > 0 {
		dur = timeout[0]
	}
	for _, plug := range plugins {
		wg.Add(1)
		go func(p Plugin) {
			defer wg.Done()
			p.BeforeTask(getQueueTask())
		}(plug)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(dur):
	}
}

func asyncPluginsAfter(plugins []Plugin, result Result, timeout ...time.Duration) {
	var wg sync.WaitGroup
	dur := 1 * time.Second
	if len(timeout) > 0 {
		dur = timeout[0]
	}
	for _, plug := range plugins {
		wg.Add(1)
		go func(p Plugin) {
			defer wg.Done()
			p.AfterTask(getQueueTask(), result)
		}(plug)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(dur):
	}
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

type CircuitBreaker struct {
	Enabled          bool
	FailureThreshold int
	ResetTimeout     time.Duration
}
