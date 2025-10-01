package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
)

// TraceEvent represents a single event in a message's lifecycle
type TraceEvent struct {
	Timestamp time.Time         `json:"timestamp"`
	EventType TraceEventType    `json:"event_type"`
	Source    string            `json:"source"`
	Details   map[string]string `json:"details,omitempty"`
	Duration  time.Duration     `json:"duration,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// TraceEventType defines types of trace events
type TraceEventType string

const (
	TraceEventEnqueued   TraceEventType = "ENQUEUED"
	TraceEventDequeued   TraceEventType = "DEQUEUED"
	TraceEventProcessing TraceEventType = "PROCESSING"
	TraceEventCompleted  TraceEventType = "COMPLETED"
	TraceEventFailed     TraceEventType = "FAILED"
	TraceEventRetried    TraceEventType = "RETRIED"
	TraceEventRejected   TraceEventType = "REJECTED"
	TraceEventAcked      TraceEventType = "ACKED"
	TraceEventNacked     TraceEventType = "NACKED"
	TraceEventTimeout    TraceEventType = "TIMEOUT"
	TraceEventDLQ        TraceEventType = "DLQ"
)

// MessageTrace represents the complete trace of a message
type MessageTrace struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	TaskID       string            `json:"task_id"`
	QueueName    string            `json:"queue_name"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time,omitempty"`
	Duration     time.Duration     `json:"duration,omitempty"`
	Events       []*TraceEvent     `json:"events"`
	Tags         map[string]string `json:"tags,omitempty"`
	Status       string            `json:"status"`
	mu           sync.RWMutex
}

// TraceManager manages distributed tracing for messages
type TraceManager struct {
	traces          map[string]*MessageTrace
	mu              sync.RWMutex
	logger          logger.Logger
	storage         TraceStorage
	shutdown        chan struct{}
	retention       time.Duration
	exportInterval  time.Duration
	onTraceComplete func(*MessageTrace)
}

// TraceStorage interface for persistent trace storage
type TraceStorage interface {
	Store(ctx context.Context, trace *MessageTrace) error
	Get(ctx context.Context, traceID string) (*MessageTrace, error)
	Query(ctx context.Context, filter TraceFilter) ([]*MessageTrace, error)
	Delete(ctx context.Context, traceID string) error
	DeleteOlderThan(ctx context.Context, duration time.Duration) (int, error)
	Close() error
}

// TraceFilter for querying traces
type TraceFilter struct {
	QueueName   string
	Status      string
	StartTime   time.Time
	EndTime     time.Time
	MinDuration time.Duration
	MaxDuration time.Duration
	HasError    bool
	Limit       int
	Offset      int
}

// TraceConfig holds tracing configuration
type TraceConfig struct {
	Storage        TraceStorage
	Retention      time.Duration
	ExportInterval time.Duration
	Logger         logger.Logger
}

// NewTraceManager creates a new trace manager
func NewTraceManager(config TraceConfig) *TraceManager {
	if config.Retention == 0 {
		config.Retention = 24 * time.Hour
	}
	if config.ExportInterval == 0 {
		config.ExportInterval = 30 * time.Second
	}

	tm := &TraceManager{
		traces:         make(map[string]*MessageTrace),
		logger:         config.Logger,
		storage:        config.Storage,
		shutdown:       make(chan struct{}),
		retention:      config.Retention,
		exportInterval: config.ExportInterval,
	}

	go tm.exportLoop()
	go tm.cleanupLoop()

	return tm
}

// StartTrace initiates tracing for a message
func (tm *TraceManager) StartTrace(ctx context.Context, task *Task, queueName string) *MessageTrace {
	trace := &MessageTrace{
		TraceID:   task.TraceID,
		SpanID:    task.SpanID,
		TaskID:    task.ID,
		QueueName: queueName,
		StartTime: time.Now(),
		Events:    make([]*TraceEvent, 0),
		Tags:      task.Tags,
		Status:    "in_progress",
	}

	tm.mu.Lock()
	tm.traces[trace.TraceID] = trace
	tm.mu.Unlock()

	tm.RecordEvent(trace.TraceID, TraceEventEnqueued, "broker", nil)

	return trace
}

// RecordEvent records an event in a trace
func (tm *TraceManager) RecordEvent(traceID string, eventType TraceEventType, source string, details map[string]string) {
	tm.mu.RLock()
	trace, exists := tm.traces[traceID]
	tm.mu.RUnlock()

	if !exists {
		tm.logger.Warn("Trace not found for event recording",
			logger.Field{Key: "traceID", Value: traceID},
			logger.Field{Key: "eventType", Value: eventType})
		return
	}

	trace.mu.Lock()
	defer trace.mu.Unlock()

	event := &TraceEvent{
		Timestamp: time.Now(),
		EventType: eventType,
		Source:    source,
		Details:   details,
	}

	trace.Events = append(trace.Events, event)

	tm.logger.Debug("Recorded trace event",
		logger.Field{Key: "traceID", Value: traceID},
		logger.Field{Key: "eventType", Value: eventType},
		logger.Field{Key: "source", Value: source})
}

// RecordError records an error event in a trace
func (tm *TraceManager) RecordError(traceID string, source string, err error, details map[string]string) {
	tm.mu.RLock()
	trace, exists := tm.traces[traceID]
	tm.mu.RUnlock()

	if !exists {
		return
	}

	trace.mu.Lock()
	defer trace.mu.Unlock()

	if details == nil {
		details = make(map[string]string)
	}

	event := &TraceEvent{
		Timestamp: time.Now(),
		EventType: TraceEventFailed,
		Source:    source,
		Details:   details,
		Error:     err.Error(),
	}

	trace.Events = append(trace.Events, event)
	trace.Status = "failed"
}

// CompleteTrace marks a trace as complete
func (tm *TraceManager) CompleteTrace(traceID string, status string) {
	tm.mu.Lock()
	trace, exists := tm.traces[traceID]
	if !exists {
		tm.mu.Unlock()
		return
	}
	delete(tm.traces, traceID)
	tm.mu.Unlock()

	trace.mu.Lock()
	trace.EndTime = time.Now()
	trace.Duration = trace.EndTime.Sub(trace.StartTime)
	trace.Status = status
	trace.mu.Unlock()

	// Store in persistent storage
	if tm.storage != nil {
		go func() {
			if err := tm.storage.Store(context.Background(), trace); err != nil {
				tm.logger.Error("Failed to store trace",
					logger.Field{Key: "traceID", Value: traceID},
					logger.Field{Key: "error", Value: err})
			}
		}()
	}

	if tm.onTraceComplete != nil {
		go tm.onTraceComplete(trace)
	}

	tm.logger.Debug("Completed trace",
		logger.Field{Key: "traceID", Value: traceID},
		logger.Field{Key: "duration", Value: trace.Duration},
		logger.Field{Key: "status", Value: status})
}

// GetTrace retrieves a trace by ID
func (tm *TraceManager) GetTrace(ctx context.Context, traceID string) (*MessageTrace, error) {
	// Check in-memory traces first
	tm.mu.RLock()
	trace, exists := tm.traces[traceID]
	tm.mu.RUnlock()

	if exists {
		return trace, nil
	}

	// Check persistent storage
	if tm.storage != nil {
		return tm.storage.Get(ctx, traceID)
	}

	return nil, fmt.Errorf("trace not found: %s", traceID)
}

// QueryTraces queries traces based on filter
func (tm *TraceManager) QueryTraces(ctx context.Context, filter TraceFilter) ([]*MessageTrace, error) {
	if tm.storage != nil {
		return tm.storage.Query(ctx, filter)
	}

	// Fallback to in-memory search
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	var results []*MessageTrace
	for _, trace := range tm.traces {
		if tm.matchesFilter(trace, filter) {
			results = append(results, trace)
		}
	}

	return results, nil
}

// matchesFilter checks if a trace matches the filter
func (tm *TraceManager) matchesFilter(trace *MessageTrace, filter TraceFilter) bool {
	if filter.QueueName != "" && trace.QueueName != filter.QueueName {
		return false
	}
	if filter.Status != "" && trace.Status != filter.Status {
		return false
	}
	if !filter.StartTime.IsZero() && trace.StartTime.Before(filter.StartTime) {
		return false
	}
	if !filter.EndTime.IsZero() && !trace.EndTime.IsZero() && trace.EndTime.After(filter.EndTime) {
		return false
	}
	if filter.MinDuration > 0 && trace.Duration < filter.MinDuration {
		return false
	}
	if filter.MaxDuration > 0 && trace.Duration > filter.MaxDuration {
		return false
	}
	return true
}

// exportLoop periodically exports traces to storage
func (tm *TraceManager) exportLoop() {
	ticker := time.NewTicker(tm.exportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.exportTraces()
		case <-tm.shutdown:
			return
		}
	}
}

// exportTraces exports active traces to storage
func (tm *TraceManager) exportTraces() {
	if tm.storage == nil {
		return
	}

	tm.mu.RLock()
	traces := make([]*MessageTrace, 0, len(tm.traces))
	for _, trace := range tm.traces {
		traces = append(traces, trace)
	}
	tm.mu.RUnlock()

	for _, trace := range traces {
		if err := tm.storage.Store(context.Background(), trace); err != nil {
			tm.logger.Error("Failed to export trace",
				logger.Field{Key: "traceID", Value: trace.TraceID},
				logger.Field{Key: "error", Value: err})
		}
	}
}

// cleanupLoop periodically cleans up old traces
func (tm *TraceManager) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tm.cleanup()
		case <-tm.shutdown:
			return
		}
	}
}

// cleanup removes old traces
func (tm *TraceManager) cleanup() {
	if tm.storage != nil {
		deleted, err := tm.storage.DeleteOlderThan(context.Background(), tm.retention)
		if err != nil {
			tm.logger.Error("Failed to cleanup old traces",
				logger.Field{Key: "error", Value: err})
		} else if deleted > 0 {
			tm.logger.Info("Cleaned up old traces",
				logger.Field{Key: "deleted", Value: deleted})
		}
	}
}

// SetOnTraceComplete sets callback for trace completion
func (tm *TraceManager) SetOnTraceComplete(fn func(*MessageTrace)) {
	tm.onTraceComplete = fn
}

// GetStats returns tracing statistics
func (tm *TraceManager) GetStats() map[string]any {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	activeTraces := len(tm.traces)

	return map[string]any{
		"active_traces":   activeTraces,
		"retention":       tm.retention,
		"export_interval": tm.exportInterval,
	}
}

// Shutdown gracefully shuts down the trace manager
func (tm *TraceManager) Shutdown(ctx context.Context) error {
	close(tm.shutdown)

	// Export remaining traces
	tm.exportTraces()

	if tm.storage != nil {
		return tm.storage.Close()
	}

	return nil
}

// InMemoryTraceStorage implements in-memory trace storage
type InMemoryTraceStorage struct {
	traces map[string]*MessageTrace
	mu     sync.RWMutex
}

// NewInMemoryTraceStorage creates a new in-memory trace storage
func NewInMemoryTraceStorage() *InMemoryTraceStorage {
	return &InMemoryTraceStorage{
		traces: make(map[string]*MessageTrace),
	}
}

func (s *InMemoryTraceStorage) Store(ctx context.Context, trace *MessageTrace) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Deep copy to avoid concurrent modifications
	data, err := json.Marshal(trace)
	if err != nil {
		return err
	}

	var traceCopy MessageTrace
	if err := json.Unmarshal(data, &traceCopy); err != nil {
		return err
	}

	s.traces[trace.TraceID] = &traceCopy
	return nil
}

func (s *InMemoryTraceStorage) Get(ctx context.Context, traceID string) (*MessageTrace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trace, exists := s.traces[traceID]
	if !exists {
		return nil, fmt.Errorf("trace not found: %s", traceID)
	}

	return trace, nil
}

func (s *InMemoryTraceStorage) Query(ctx context.Context, filter TraceFilter) ([]*MessageTrace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*MessageTrace
	for _, trace := range s.traces {
		results = append(results, trace)
	}

	// Apply pagination
	start := filter.Offset
	if start >= len(results) {
		return []*MessageTrace{}, nil
	}

	end := start + filter.Limit
	if filter.Limit == 0 || end > len(results) {
		end = len(results)
	}

	return results[start:end], nil
}

func (s *InMemoryTraceStorage) Delete(ctx context.Context, traceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.traces, traceID)
	return nil
}

func (s *InMemoryTraceStorage) DeleteOlderThan(ctx context.Context, duration time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-duration)
	deleted := 0

	for traceID, trace := range s.traces {
		if !trace.EndTime.IsZero() && trace.EndTime.Before(cutoff) {
			delete(s.traces, traceID)
			deleted++
		}
	}

	return deleted, nil
}

func (s *InMemoryTraceStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.traces = nil
	return nil
}

// MessageLifecycleTracker tracks the complete lifecycle of messages
type MessageLifecycleTracker struct {
	traceManager *TraceManager
	logger       logger.Logger
}

// NewMessageLifecycleTracker creates a new lifecycle tracker
func NewMessageLifecycleTracker(traceManager *TraceManager, log logger.Logger) *MessageLifecycleTracker {
	return &MessageLifecycleTracker{
		traceManager: traceManager,
		logger:       log,
	}
}

// TrackEnqueue tracks message enqueue
func (lt *MessageLifecycleTracker) TrackEnqueue(ctx context.Context, task *Task, queueName string) {
	lt.traceManager.StartTrace(ctx, task, queueName)
}

// TrackDequeue tracks message dequeue
func (lt *MessageLifecycleTracker) TrackDequeue(traceID, consumerID string) {
	lt.traceManager.RecordEvent(traceID, TraceEventDequeued, consumerID, nil)
}

// TrackProcessing tracks message processing start
func (lt *MessageLifecycleTracker) TrackProcessing(traceID, workerID string) {
	lt.traceManager.RecordEvent(traceID, TraceEventProcessing, workerID, nil)
}

// TrackCompletion tracks successful completion
func (lt *MessageLifecycleTracker) TrackCompletion(traceID string, duration time.Duration) {
	details := map[string]string{
		"duration": duration.String(),
	}
	lt.traceManager.RecordEvent(traceID, TraceEventCompleted, "worker", details)
	lt.traceManager.CompleteTrace(traceID, "completed")
}

// TrackError tracks processing error
func (lt *MessageLifecycleTracker) TrackError(traceID, source string, err error) {
	lt.traceManager.RecordError(traceID, source, err, nil)
}

// TrackRetry tracks retry attempt
func (lt *MessageLifecycleTracker) TrackRetry(traceID string, retryCount int) {
	details := map[string]string{
		"retry_count": fmt.Sprintf("%d", retryCount),
	}
	lt.traceManager.RecordEvent(traceID, TraceEventRetried, "broker", details)
}

// TrackDLQ tracks movement to dead letter queue
func (lt *MessageLifecycleTracker) TrackDLQ(traceID, reason string) {
	details := map[string]string{
		"reason": reason,
	}
	lt.traceManager.RecordEvent(traceID, TraceEventDLQ, "broker", details)
	lt.traceManager.CompleteTrace(traceID, "dlq")
}
