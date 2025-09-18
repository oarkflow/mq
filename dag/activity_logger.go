package dag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// ActivityLevel represents the severity level of an activity
type ActivityLevel string

const (
	ActivityLevelDebug ActivityLevel = "debug"
	ActivityLevelInfo  ActivityLevel = "info"
	ActivityLevelWarn  ActivityLevel = "warn"
	ActivityLevelError ActivityLevel = "error"
	ActivityLevelFatal ActivityLevel = "fatal"
)

// ActivityType represents the type of activity
type ActivityType string

const (
	ActivityTypeTaskStart      ActivityType = "task_start"
	ActivityTypeTaskComplete   ActivityType = "task_complete"
	ActivityTypeTaskFail       ActivityType = "task_fail"
	ActivityTypeTaskCancel     ActivityType = "task_cancel"
	ActivityTypeNodeStart      ActivityType = "node_start"
	ActivityTypeNodeComplete   ActivityType = "node_complete"
	ActivityTypeNodeFail       ActivityType = "node_fail"
	ActivityTypeNodeTimeout    ActivityType = "node_timeout"
	ActivityTypeValidation     ActivityType = "validation"
	ActivityTypeConfiguration  ActivityType = "configuration"
	ActivityTypeAlert          ActivityType = "alert"
	ActivityTypeCleanup        ActivityType = "cleanup"
	ActivityTypeTransaction    ActivityType = "transaction"
	ActivityTypeRetry          ActivityType = "retry"
	ActivityTypeCircuitBreaker ActivityType = "circuit_breaker"
	ActivityTypeWebhook        ActivityType = "webhook"
	ActivityTypeCustom         ActivityType = "custom"
)

// ActivityEntry represents a single activity log entry
type ActivityEntry struct {
	ID          string         `json:"id"`
	Timestamp   time.Time      `json:"timestamp"`
	DAGName     string         `json:"dag_name"`
	Level       ActivityLevel  `json:"level"`
	Type        ActivityType   `json:"type"`
	Message     string         `json:"message"`
	TaskID      string         `json:"task_id,omitempty"`
	NodeID      string         `json:"node_id,omitempty"`
	Duration    time.Duration  `json:"duration,omitempty"`
	Success     *bool          `json:"success,omitempty"`
	Error       string         `json:"error,omitempty"`
	Details     map[string]any `json:"details,omitempty"`
	ContextData map[string]any `json:"context_data,omitempty"`
	UserID      string         `json:"user_id,omitempty"`
	SessionID   string         `json:"session_id,omitempty"`
	TraceID     string         `json:"trace_id,omitempty"`
	SpanID      string         `json:"span_id,omitempty"`
}

// ActivityFilter provides filtering options for activity queries
type ActivityFilter struct {
	StartTime    *time.Time      `json:"start_time,omitempty"`
	EndTime      *time.Time      `json:"end_time,omitempty"`
	Levels       []ActivityLevel `json:"levels,omitempty"`
	Types        []ActivityType  `json:"types,omitempty"`
	TaskIDs      []string        `json:"task_ids,omitempty"`
	NodeIDs      []string        `json:"node_ids,omitempty"`
	UserIDs      []string        `json:"user_ids,omitempty"`
	SuccessOnly  *bool           `json:"success_only,omitempty"`
	FailuresOnly *bool           `json:"failures_only,omitempty"`
	Limit        int             `json:"limit,omitempty"`
	Offset       int             `json:"offset,omitempty"`
	SortBy       string          `json:"sort_by,omitempty"`    // timestamp, level, type
	SortOrder    string          `json:"sort_order,omitempty"` // asc, desc
}

// ActivityStats provides statistics about activities
type ActivityStats struct {
	TotalActivities      int64                   `json:"total_activities"`
	ActivitiesByLevel    map[ActivityLevel]int64 `json:"activities_by_level"`
	ActivitiesByType     map[ActivityType]int64  `json:"activities_by_type"`
	ActivitiesByNode     map[string]int64        `json:"activities_by_node"`
	ActivitiesByTask     map[string]int64        `json:"activities_by_task"`
	SuccessRate          float64                 `json:"success_rate"`
	FailureRate          float64                 `json:"failure_rate"`
	AverageDuration      time.Duration           `json:"average_duration"`
	PeakActivitiesPerMin int64                   `json:"peak_activities_per_minute"`
	TimeRange            ActivityTimeRange       `json:"time_range"`
	RecentErrors         []ActivityEntry         `json:"recent_errors"`
	TopFailingNodes      []NodeFailureStats      `json:"top_failing_nodes"`
	HourlyDistribution   map[string]int64        `json:"hourly_distribution"`
}

// ActivityTimeRange represents a time range for activities
type ActivityTimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// NodeFailureStats represents failure statistics for a node
type NodeFailureStats struct {
	NodeID       string    `json:"node_id"`
	FailureCount int64     `json:"failure_count"`
	FailureRate  float64   `json:"failure_rate"`
	LastFailure  time.Time `json:"last_failure"`
}

// ActivityHook allows custom processing of activity entries
type ActivityHook interface {
	OnActivity(entry ActivityEntry) error
}

// ActivityPersistence defines the interface for persisting activities
type ActivityPersistence interface {
	Store(entries []ActivityEntry) error
	Query(filter ActivityFilter) ([]ActivityEntry, error)
	GetStats(filter ActivityFilter) (ActivityStats, error)
	Close() error
}

// ActivityLoggerConfig configures the activity logger
type ActivityLoggerConfig struct {
	BufferSize        int           `json:"buffer_size"`
	FlushInterval     time.Duration `json:"flush_interval"`
	MaxRetries        int           `json:"max_retries"`
	EnableHooks       bool          `json:"enable_hooks"`
	EnableCompression bool          `json:"enable_compression"`
	MaxEntryAge       time.Duration `json:"max_entry_age"`
	AsyncMode         bool          `json:"async_mode"`
}

// DefaultActivityLoggerConfig returns default configuration
func DefaultActivityLoggerConfig() ActivityLoggerConfig {
	return ActivityLoggerConfig{
		BufferSize:        1000,
		FlushInterval:     5 * time.Second,
		MaxRetries:        3,
		EnableHooks:       true,
		EnableCompression: false,
		MaxEntryAge:       24 * time.Hour,
		AsyncMode:         true,
	}
}

// ActivityLogger provides comprehensive activity logging for DAG operations
type ActivityLogger struct {
	dagName     string
	config      ActivityLoggerConfig
	persistence ActivityPersistence
	logger      logger.Logger
	buffer      []ActivityEntry
	bufferMu    sync.Mutex
	hooks       []ActivityHook
	hooksMu     sync.RWMutex
	stopCh      chan struct{}
	flushCh     chan struct{}
	running     bool
	runningMu   sync.RWMutex
	stats       ActivityStats
	statsMu     sync.RWMutex
}

// NewActivityLogger creates a new activity logger
func NewActivityLogger(dagName string, config ActivityLoggerConfig, persistence ActivityPersistence, logger logger.Logger) *ActivityLogger {
	al := &ActivityLogger{
		dagName:     dagName,
		config:      config,
		persistence: persistence,
		logger:      logger,
		buffer:      make([]ActivityEntry, 0, config.BufferSize),
		hooks:       make([]ActivityHook, 0),
		stopCh:      make(chan struct{}),
		flushCh:     make(chan struct{}, 1),
		stats: ActivityStats{
			ActivitiesByLevel:  make(map[ActivityLevel]int64),
			ActivitiesByType:   make(map[ActivityType]int64),
			ActivitiesByNode:   make(map[string]int64),
			ActivitiesByTask:   make(map[string]int64),
			HourlyDistribution: make(map[string]int64),
		},
	}

	if config.AsyncMode {
		al.start()
	}

	return al
}

// start begins the async processing routines
func (al *ActivityLogger) start() {
	al.runningMu.Lock()
	defer al.runningMu.Unlock()

	if al.running {
		return
	}

	al.running = true
	go al.flushRoutine()
}

// Stop stops the activity logger
func (al *ActivityLogger) Stop() {
	al.runningMu.Lock()
	defer al.runningMu.Unlock()

	if !al.running {
		return
	}

	al.running = false
	close(al.stopCh)

	// Final flush
	al.Flush()
}

// flushRoutine handles periodic flushing of the buffer
func (al *ActivityLogger) flushRoutine() {
	ticker := time.NewTicker(al.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-al.stopCh:
			return
		case <-ticker.C:
			al.Flush()
		case <-al.flushCh:
			al.Flush()
		}
	}
}

// Log logs an activity entry
func (al *ActivityLogger) Log(level ActivityLevel, activityType ActivityType, message string, details map[string]any) {
	al.LogWithContext(context.Background(), level, activityType, message, details)
}

// LogWithContext logs an activity entry with context information
func (al *ActivityLogger) LogWithContext(ctx context.Context, level ActivityLevel, activityType ActivityType, message string, details map[string]any) {
	entry := ActivityEntry{
		ID:          mq.NewID(),
		Timestamp:   time.Now(),
		DAGName:     al.dagName,
		Level:       level,
		Type:        activityType,
		Message:     message,
		Details:     details,
		ContextData: make(map[string]any),
	}

	// Extract context information
	if taskID, ok := ctx.Value("task_id").(string); ok {
		entry.TaskID = taskID
	}
	if nodeID, ok := ctx.Value("node_id").(string); ok {
		entry.NodeID = nodeID
	}
	if userID, ok := ctx.Value("user_id").(string); ok {
		entry.UserID = userID
	}
	if sessionID, ok := ctx.Value("session_id").(string); ok {
		entry.SessionID = sessionID
	}
	if traceID, ok := ctx.Value("trace_id").(string); ok {
		entry.TraceID = traceID
	}
	if spanID, ok := ctx.Value("span_id").(string); ok {
		entry.SpanID = spanID
	}
	if duration, ok := ctx.Value("duration").(time.Duration); ok {
		entry.Duration = duration
	}
	if err, ok := ctx.Value("error").(error); ok {
		entry.Error = err.Error()
		success := false
		entry.Success = &success
	}

	// Extract additional context data
	for key, value := range map[string]any{
		"method":     ctx.Value("method"),
		"user_agent": ctx.Value("user_agent"),
		"ip_address": ctx.Value("ip_address"),
		"request_id": ctx.Value("request_id"),
	} {
		if value != nil {
			entry.ContextData[key] = value
		}
	}

	al.addEntry(entry)
}

// LogTaskStart logs task start activity
func (al *ActivityLogger) LogTaskStart(ctx context.Context, taskID string, nodeID string) {
	al.LogWithContext(ctx, ActivityLevelInfo, ActivityTypeTaskStart,
		fmt.Sprintf("Task %s started on node %s", taskID, nodeID),
		map[string]any{
			"task_id": taskID,
			"node_id": nodeID,
		})
}

// LogTaskComplete logs task completion activity
func (al *ActivityLogger) LogTaskComplete(ctx context.Context, taskID string, nodeID string, duration time.Duration) {
	success := true
	entry := ActivityEntry{
		ID:        mq.NewID(),
		Timestamp: time.Now(),
		DAGName:   al.dagName,
		Level:     ActivityLevelInfo,
		Type:      ActivityTypeTaskComplete,
		Message:   fmt.Sprintf("Task %s completed successfully on node %s", taskID, nodeID),
		TaskID:    taskID,
		NodeID:    nodeID,
		Duration:  duration,
		Success:   &success,
		Details: map[string]any{
			"task_id":  taskID,
			"node_id":  nodeID,
			"duration": duration.String(),
		},
	}
	al.addEntry(entry)
}

// LogTaskFail logs task failure activity
func (al *ActivityLogger) LogTaskFail(ctx context.Context, taskID string, nodeID string, err error, duration time.Duration) {
	success := false
	entry := ActivityEntry{
		ID:        mq.NewID(),
		Timestamp: time.Now(),
		DAGName:   al.dagName,
		Level:     ActivityLevelError,
		Type:      ActivityTypeTaskFail,
		Message:   fmt.Sprintf("Task %s failed on node %s: %s", taskID, nodeID, err.Error()),
		TaskID:    taskID,
		NodeID:    nodeID,
		Duration:  duration,
		Success:   &success,
		Error:     err.Error(),
		Details: map[string]any{
			"task_id":  taskID,
			"node_id":  nodeID,
			"duration": duration.String(),
			"error":    err.Error(),
		},
	}
	al.addEntry(entry)
}

// LogNodeExecution logs node execution details
func (al *ActivityLogger) LogNodeExecution(ctx context.Context, taskID string, nodeID string, result mq.Result, duration time.Duration) {
	if result.Error != nil {
		al.LogTaskFail(ctx, taskID, nodeID, result.Error, duration)
	} else {
		al.LogTaskComplete(ctx, taskID, nodeID, duration)
	}
}

// addEntry adds an entry to the buffer and triggers hooks
func (al *ActivityLogger) addEntry(entry ActivityEntry) {
	// Update statistics
	al.updateStats(entry)

	// Trigger hooks
	if al.config.EnableHooks {
		al.triggerHooks(entry)
	}

	// Add to buffer
	al.bufferMu.Lock()
	al.buffer = append(al.buffer, entry)
	shouldFlush := len(al.buffer) >= al.config.BufferSize
	al.bufferMu.Unlock()

	// Trigger flush if buffer is full
	if shouldFlush {
		select {
		case al.flushCh <- struct{}{}:
		default:
		}
	}

	// Also log to standard logger for immediate feedback
	fields := []logger.Field{
		{Key: "activity_id", Value: entry.ID},
		{Key: "dag_name", Value: entry.DAGName},
		{Key: "type", Value: string(entry.Type)},
		{Key: "task_id", Value: entry.TaskID},
		{Key: "node_id", Value: entry.NodeID},
	}

	if entry.Duration > 0 {
		fields = append(fields, logger.Field{Key: "duration", Value: entry.Duration.String()})
	}

	switch entry.Level {
	case ActivityLevelError, ActivityLevelFatal:
		al.logger.Error(entry.Message, fields...)
	case ActivityLevelWarn:
		al.logger.Warn(entry.Message, fields...)
	case ActivityLevelDebug:
		al.logger.Debug(entry.Message, fields...)
	default:
		al.logger.Info(entry.Message, fields...)
	}
}

// updateStats updates internal statistics
func (al *ActivityLogger) updateStats(entry ActivityEntry) {
	al.statsMu.Lock()
	defer al.statsMu.Unlock()

	al.stats.TotalActivities++
	al.stats.ActivitiesByLevel[entry.Level]++
	al.stats.ActivitiesByType[entry.Type]++

	if entry.NodeID != "" {
		al.stats.ActivitiesByNode[entry.NodeID]++
	}

	if entry.TaskID != "" {
		al.stats.ActivitiesByTask[entry.TaskID]++
	}

	// Update hourly distribution
	hour := entry.Timestamp.Format("2006-01-02T15")
	al.stats.HourlyDistribution[hour]++

	// Track recent errors
	if entry.Level == ActivityLevelError || entry.Level == ActivityLevelFatal {
		al.stats.RecentErrors = append(al.stats.RecentErrors, entry)
		// Keep only last 10 errors
		if len(al.stats.RecentErrors) > 10 {
			al.stats.RecentErrors = al.stats.RecentErrors[len(al.stats.RecentErrors)-10:]
		}
	}
}

// triggerHooks executes all registered hooks
func (al *ActivityLogger) triggerHooks(entry ActivityEntry) {
	al.hooksMu.RLock()
	hooks := make([]ActivityHook, len(al.hooks))
	copy(hooks, al.hooks)
	al.hooksMu.RUnlock()

	for _, hook := range hooks {
		go func(h ActivityHook, e ActivityEntry) {
			if err := h.OnActivity(e); err != nil {
				al.logger.Error("Activity hook error",
					logger.Field{Key: "error", Value: err.Error()},
					logger.Field{Key: "activity_id", Value: e.ID},
				)
			}
		}(hook, entry)
	}
}

// AddHook adds an activity hook
func (al *ActivityLogger) AddHook(hook ActivityHook) {
	al.hooksMu.Lock()
	defer al.hooksMu.Unlock()
	al.hooks = append(al.hooks, hook)
}

// RemoveHook removes an activity hook
func (al *ActivityLogger) RemoveHook(hook ActivityHook) {
	al.hooksMu.Lock()
	defer al.hooksMu.Unlock()

	for i, h := range al.hooks {
		if h == hook {
			al.hooks = append(al.hooks[:i], al.hooks[i+1:]...)
			break
		}
	}
}

// Flush flushes the buffer to persistence
func (al *ActivityLogger) Flush() error {
	al.bufferMu.Lock()
	if len(al.buffer) == 0 {
		al.bufferMu.Unlock()
		return nil
	}

	entries := make([]ActivityEntry, len(al.buffer))
	copy(entries, al.buffer)
	al.buffer = al.buffer[:0] // Clear buffer
	al.bufferMu.Unlock()

	if al.persistence == nil {
		return nil
	}

	// Retry logic
	var err error
	for attempt := 0; attempt < al.config.MaxRetries; attempt++ {
		err = al.persistence.Store(entries)
		if err == nil {
			al.logger.Debug("Activity entries flushed to persistence",
				logger.Field{Key: "count", Value: len(entries)},
			)
			return nil
		}

		al.logger.Warn("Failed to flush activity entries",
			logger.Field{Key: "attempt", Value: attempt + 1},
			logger.Field{Key: "error", Value: err.Error()},
		)

		if attempt < al.config.MaxRetries-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	return fmt.Errorf("failed to flush activities after %d attempts: %w", al.config.MaxRetries, err)
}

// GetActivities retrieves activities based on filter
func (al *ActivityLogger) GetActivities(filter ActivityFilter) ([]ActivityEntry, error) {
	if al.persistence == nil {
		return nil, fmt.Errorf("persistence not configured")
	}
	return al.persistence.Query(filter)
}

// GetStats returns activity statistics
func (al *ActivityLogger) GetStats(filter ActivityFilter) (ActivityStats, error) {
	if al.persistence == nil {
		// Return in-memory stats if no persistence
		al.statsMu.RLock()
		stats := al.stats
		al.statsMu.RUnlock()
		return stats, nil
	}
	return al.persistence.GetStats(filter)
}

// MemoryActivityPersistence provides in-memory activity persistence for testing
type MemoryActivityPersistence struct {
	entries []ActivityEntry
	mu      sync.RWMutex
}

// NewMemoryActivityPersistence creates a new in-memory persistence
func NewMemoryActivityPersistence() *MemoryActivityPersistence {
	return &MemoryActivityPersistence{
		entries: make([]ActivityEntry, 0),
	}
}

// Store stores activity entries in memory
func (mp *MemoryActivityPersistence) Store(entries []ActivityEntry) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.entries = append(mp.entries, entries...)
	return nil
}

// Query queries activity entries with filter
func (mp *MemoryActivityPersistence) Query(filter ActivityFilter) ([]ActivityEntry, error) {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	var result []ActivityEntry
	for _, entry := range mp.entries {
		if mp.matchesFilter(entry, filter) {
			result = append(result, entry)
		}
	}

	// Apply limit and offset
	if filter.Offset > 0 && filter.Offset < len(result) {
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}

	return result, nil
}

// matchesFilter checks if an entry matches the filter
func (mp *MemoryActivityPersistence) matchesFilter(entry ActivityEntry, filter ActivityFilter) bool {
	// Time range check
	if filter.StartTime != nil && entry.Timestamp.Before(*filter.StartTime) {
		return false
	}
	if filter.EndTime != nil && entry.Timestamp.After(*filter.EndTime) {
		return false
	}

	// Level filter
	if len(filter.Levels) > 0 {
		found := false
		for _, level := range filter.Levels {
			if entry.Level == level {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Type filter
	if len(filter.Types) > 0 {
		found := false
		for _, typ := range filter.Types {
			if entry.Type == typ {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Task ID filter
	if len(filter.TaskIDs) > 0 {
		found := false
		for _, taskID := range filter.TaskIDs {
			if entry.TaskID == taskID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Node ID filter
	if len(filter.NodeIDs) > 0 {
		found := false
		for _, nodeID := range filter.NodeIDs {
			if entry.NodeID == nodeID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Success/failure filters
	if filter.SuccessOnly != nil && *filter.SuccessOnly {
		if entry.Success == nil || !*entry.Success {
			return false
		}
	}
	if filter.FailuresOnly != nil && *filter.FailuresOnly {
		if entry.Success == nil || *entry.Success {
			return false
		}
	}

	return true
}

// GetStats returns statistics for the filtered entries
func (mp *MemoryActivityPersistence) GetStats(filter ActivityFilter) (ActivityStats, error) {
	entries, err := mp.Query(filter)
	if err != nil {
		return ActivityStats{}, err
	}

	stats := ActivityStats{
		ActivitiesByLevel:  make(map[ActivityLevel]int64),
		ActivitiesByType:   make(map[ActivityType]int64),
		ActivitiesByNode:   make(map[string]int64),
		ActivitiesByTask:   make(map[string]int64),
		HourlyDistribution: make(map[string]int64),
	}

	var totalDuration time.Duration
	var durationCount int64
	var successCount int64
	var failureCount int64

	for _, entry := range entries {
		stats.TotalActivities++
		stats.ActivitiesByLevel[entry.Level]++
		stats.ActivitiesByType[entry.Type]++

		if entry.NodeID != "" {
			stats.ActivitiesByNode[entry.NodeID]++
		}
		if entry.TaskID != "" {
			stats.ActivitiesByTask[entry.TaskID]++
		}

		hour := entry.Timestamp.Format("2006-01-02T15")
		stats.HourlyDistribution[hour]++

		if entry.Duration > 0 {
			totalDuration += entry.Duration
			durationCount++
		}

		if entry.Success != nil {
			if *entry.Success {
				successCount++
			} else {
				failureCount++
			}
		}

		if entry.Level == ActivityLevelError || entry.Level == ActivityLevelFatal {
			stats.RecentErrors = append(stats.RecentErrors, entry)
		}
	}

	// Calculate rates and averages
	if durationCount > 0 {
		stats.AverageDuration = totalDuration / time.Duration(durationCount)
	}

	total := successCount + failureCount
	if total > 0 {
		stats.SuccessRate = float64(successCount) / float64(total)
		stats.FailureRate = float64(failureCount) / float64(total)
	}

	// Keep only last 10 errors
	if len(stats.RecentErrors) > 10 {
		stats.RecentErrors = stats.RecentErrors[len(stats.RecentErrors)-10:]
	}

	return stats, nil
}

// Close closes the persistence
func (mp *MemoryActivityPersistence) Close() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.entries = nil
	return nil
}
