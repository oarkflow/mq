package dag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// MonitoringMetrics holds comprehensive metrics for DAG monitoring
type MonitoringMetrics struct {
	mu                   sync.RWMutex
	TasksTotal           int64
	TasksCompleted       int64
	TasksFailed          int64
	TasksCancelled       int64
	TasksInProgress      int64
	NodesExecuted        map[string]int64
	NodeExecutionTimes   map[string][]time.Duration
	NodeFailures         map[string]int64
	AverageExecutionTime time.Duration
	TotalExecutionTime   time.Duration
	StartTime            time.Time
	LastTaskCompletedAt  time.Time
	ActiveTasks          map[string]time.Time
	NodeProcessingStats  map[string]*NodeStats
}

// NodeStats holds statistics for individual nodes
type NodeStats struct {
	ExecutionCount   int64
	SuccessCount     int64
	FailureCount     int64
	TotalDuration    time.Duration
	AverageDuration  time.Duration
	MinDuration      time.Duration
	MaxDuration      time.Duration
	LastExecuted     time.Time
	LastSuccess      time.Time
	LastFailure      time.Time
	CurrentlyRunning int64
}

// NewMonitoringMetrics creates a new metrics instance
func NewMonitoringMetrics() *MonitoringMetrics {
	return &MonitoringMetrics{
		NodesExecuted:       make(map[string]int64),
		NodeExecutionTimes:  make(map[string][]time.Duration),
		NodeFailures:        make(map[string]int64),
		StartTime:           time.Now(),
		ActiveTasks:         make(map[string]time.Time),
		NodeProcessingStats: make(map[string]*NodeStats),
	}
}

// RecordTaskStart records the start of a task
func (m *MonitoringMetrics) RecordTaskStart(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TasksTotal++
	m.TasksInProgress++
	m.ActiveTasks[taskID] = time.Now()
}

// RecordTaskCompletion records task completion
func (m *MonitoringMetrics) RecordTaskCompletion(taskID string, status mq.Status) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TasksInProgress--
	if m.TasksInProgress < 0 {
		m.TasksInProgress = 0
	}

	switch status {
	case mq.Completed:
		m.TasksCompleted++
	case mq.Failed:
		m.TasksFailed++
	case mq.Cancelled:
		m.TasksCancelled++
	}

	m.LastTaskCompletedAt = time.Now()
	delete(m.ActiveTasks, taskID)
}

// RecordNodeExecution records node execution metrics
func (m *MonitoringMetrics) RecordNodeExecution(nodeID string, duration time.Duration, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize node stats if not exists
	if _, exists := m.NodeProcessingStats[nodeID]; !exists {
		m.NodeProcessingStats[nodeID] = &NodeStats{
			MinDuration: duration,
			MaxDuration: duration,
		}
	}

	stats := m.NodeProcessingStats[nodeID]
	stats.ExecutionCount++
	stats.TotalDuration += duration
	stats.AverageDuration = stats.TotalDuration / time.Duration(stats.ExecutionCount)
	stats.LastExecuted = time.Now()

	if duration < stats.MinDuration || stats.MinDuration == 0 {
		stats.MinDuration = duration
	}
	if duration > stats.MaxDuration {
		stats.MaxDuration = duration
	}

	if success {
		stats.SuccessCount++
		stats.LastSuccess = time.Now()
	} else {
		stats.FailureCount++
		stats.LastFailure = time.Now()
		m.NodeFailures[nodeID]++
	}

	// Legacy tracking
	m.NodesExecuted[nodeID]++
	m.NodeExecutionTimes[nodeID] = append(m.NodeExecutionTimes[nodeID], duration)

	// Keep only last 100 execution times per node to prevent memory bloat
	if len(m.NodeExecutionTimes[nodeID]) > 100 {
		m.NodeExecutionTimes[nodeID] = m.NodeExecutionTimes[nodeID][len(m.NodeExecutionTimes[nodeID])-100:]
	}

	// Calculate average execution time
	var totalDuration time.Duration
	var totalExecutions int64
	for _, durations := range m.NodeExecutionTimes {
		for _, d := range durations {
			totalDuration += d
			totalExecutions++
		}
	}
	if totalExecutions > 0 {
		m.AverageExecutionTime = totalDuration / time.Duration(totalExecutions)
	}

	m.TotalExecutionTime += duration
}

// RecordNodeStart records when a node starts processing
func (m *MonitoringMetrics) RecordNodeStart(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if stats, exists := m.NodeProcessingStats[nodeID]; exists {
		stats.CurrentlyRunning++
	} else {
		m.NodeProcessingStats[nodeID] = &NodeStats{
			CurrentlyRunning: 1,
		}
	}
}

// RecordNodeEnd records when a node finishes processing
func (m *MonitoringMetrics) RecordNodeEnd(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if stats, exists := m.NodeProcessingStats[nodeID]; exists {
		stats.CurrentlyRunning--
		if stats.CurrentlyRunning < 0 {
			stats.CurrentlyRunning = 0
		}
	}
}

// GetSnapshot returns a snapshot of current metrics
func (m *MonitoringMetrics) GetSnapshot() *MonitoringMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := &MonitoringMetrics{
		TasksTotal:           m.TasksTotal,
		TasksCompleted:       m.TasksCompleted,
		TasksFailed:          m.TasksFailed,
		TasksCancelled:       m.TasksCancelled,
		TasksInProgress:      m.TasksInProgress,
		AverageExecutionTime: m.AverageExecutionTime,
		TotalExecutionTime:   m.TotalExecutionTime,
		StartTime:            m.StartTime,
		LastTaskCompletedAt:  m.LastTaskCompletedAt,
		NodesExecuted:        make(map[string]int64),
		NodeExecutionTimes:   make(map[string][]time.Duration),
		NodeFailures:         make(map[string]int64),
		ActiveTasks:          make(map[string]time.Time),
		NodeProcessingStats:  make(map[string]*NodeStats),
	}

	// Deep copy maps
	for k, v := range m.NodesExecuted {
		snapshot.NodesExecuted[k] = v
	}
	for k, v := range m.NodeFailures {
		snapshot.NodeFailures[k] = v
	}
	for k, v := range m.ActiveTasks {
		snapshot.ActiveTasks[k] = v
	}
	for k, v := range m.NodeProcessingStats {
		statsCopy := *v
		snapshot.NodeProcessingStats[k] = &statsCopy
	}
	for k, v := range m.NodeExecutionTimes {
		timesCopy := make([]time.Duration, len(v))
		copy(timesCopy, v)
		snapshot.NodeExecutionTimes[k] = timesCopy
	}

	return snapshot
}

// GetNodeStats returns statistics for a specific node
func (m *MonitoringMetrics) GetNodeStats(nodeID string) *NodeStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if stats, exists := m.NodeProcessingStats[nodeID]; exists {
		statsCopy := *stats
		return &statsCopy
	}
	return nil
}

// Monitor provides comprehensive monitoring capabilities for DAG
type Monitor struct {
	dag        *DAG
	metrics    *MonitoringMetrics
	logger     logger.Logger
	thresholds *AlertThresholds
	handlers   []AlertHandler
	stopCh     chan struct{}
	running    bool
	mu         sync.RWMutex
}

// AlertThresholds defines thresholds for alerting
type AlertThresholds struct {
	MaxFailureRate      float64       `json:"max_failure_rate"`
	MaxExecutionTime    time.Duration `json:"max_execution_time"`
	MaxTasksInProgress  int64         `json:"max_tasks_in_progress"`
	MinSuccessRate      float64       `json:"min_success_rate"`
	MaxNodeFailures     int64         `json:"max_node_failures"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
}

// AlertHandler defines interface for handling alerts
type AlertHandler interface {
	HandleAlert(alert Alert) error
}

// Alert represents a monitoring alert
type Alert struct {
	ID          string         `json:"id"`
	Timestamp   time.Time      `json:"timestamp"`
	Severity    AlertSeverity  `json:"severity"`
	Type        AlertType      `json:"type"`
	Message     string         `json:"message"`
	Details     map[string]any `json:"details"`
	NodeID      string         `json:"node_id,omitempty"`
	TaskID      string         `json:"task_id,omitempty"`
	Threshold   any            `json:"threshold,omitempty"`
	ActualValue any            `json:"actual_value,omitempty"`
}

type AlertSeverity string

const (
	AlertSeverityInfo     AlertSeverity = "info"
	AlertSeverityWarning  AlertSeverity = "warning"
	AlertSeverityCritical AlertSeverity = "critical"
)

type AlertType string

const (
	AlertTypeFailureRate    AlertType = "failure_rate"
	AlertTypeExecutionTime  AlertType = "execution_time"
	AlertTypeTaskLoad       AlertType = "task_load"
	AlertTypeNodeFailures   AlertType = "node_failures"
	AlertTypeCircuitBreaker AlertType = "circuit_breaker"
	AlertTypeHealthCheck    AlertType = "health_check"
)

// NewMonitor creates a new DAG monitor
func NewMonitor(dag *DAG, logger logger.Logger) *Monitor {
	return &Monitor{
		dag:     dag,
		metrics: NewMonitoringMetrics(),
		logger:  logger,
		thresholds: &AlertThresholds{
			MaxFailureRate:      0.1, // 10%
			MaxExecutionTime:    5 * time.Minute,
			MaxTasksInProgress:  1000,
			MinSuccessRate:      0.9, // 90%
			MaxNodeFailures:     10,
			HealthCheckInterval: 30 * time.Second,
		},
		handlers: make([]AlertHandler, 0),
		stopCh:   make(chan struct{}),
	}
}

// Start begins monitoring
func (m *Monitor) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return
	}

	m.running = true
	go m.healthCheckRoutine(ctx)

	m.logger.Info("DAG monitoring started")
}

// Stop stops monitoring
func (m *Monitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return
	}

	m.running = false
	close(m.stopCh)

	m.logger.Info("DAG monitoring stopped")
}

// SetAlertThresholds updates alert thresholds
func (m *Monitor) SetAlertThresholds(thresholds *AlertThresholds) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.thresholds = thresholds
}

// AddAlertHandler adds an alert handler
func (m *Monitor) AddAlertHandler(handler AlertHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers = append(m.handlers, handler)
}

// GetMetrics returns current metrics
func (m *Monitor) GetMetrics() *MonitoringMetrics {
	return m.metrics.GetSnapshot()
}

// healthCheckRoutine performs periodic health checks
func (m *Monitor) healthCheckRoutine(ctx context.Context) {
	ticker := time.NewTicker(m.thresholds.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.performHealthCheck()
		}
	}
}

// performHealthCheck checks system health and triggers alerts
func (m *Monitor) performHealthCheck() {
	metrics := m.GetMetrics()

	// Check failure rate
	if metrics.TasksTotal > 0 {
		failureRate := float64(metrics.TasksFailed) / float64(metrics.TasksTotal)
		if failureRate > m.thresholds.MaxFailureRate {
			m.triggerAlert(Alert{
				ID:          mq.NewID(),
				Timestamp:   time.Now(),
				Severity:    AlertSeverityCritical,
				Type:        AlertTypeFailureRate,
				Message:     "High failure rate detected",
				Threshold:   m.thresholds.MaxFailureRate,
				ActualValue: failureRate,
				Details: map[string]any{
					"failed_tasks": metrics.TasksFailed,
					"total_tasks":  metrics.TasksTotal,
				},
			})
		}
	}

	// Check task load
	if metrics.TasksInProgress > m.thresholds.MaxTasksInProgress {
		m.triggerAlert(Alert{
			ID:          mq.NewID(),
			Timestamp:   time.Now(),
			Severity:    AlertSeverityWarning,
			Type:        AlertTypeTaskLoad,
			Message:     "High task load detected",
			Threshold:   m.thresholds.MaxTasksInProgress,
			ActualValue: metrics.TasksInProgress,
			Details: map[string]any{
				"tasks_in_progress": metrics.TasksInProgress,
			},
		})
	}

	// Check node failures
	for nodeID, failures := range metrics.NodeFailures {
		if failures > m.thresholds.MaxNodeFailures {
			m.triggerAlert(Alert{
				ID:          mq.NewID(),
				Timestamp:   time.Now(),
				Severity:    AlertSeverityCritical,
				Type:        AlertTypeNodeFailures,
				Message:     fmt.Sprintf("Node %s has too many failures", nodeID),
				NodeID:      nodeID,
				Threshold:   m.thresholds.MaxNodeFailures,
				ActualValue: failures,
				Details: map[string]any{
					"node_id":  nodeID,
					"failures": failures,
				},
			})
		}
	}

	// Check execution time
	if metrics.AverageExecutionTime > m.thresholds.MaxExecutionTime {
		m.triggerAlert(Alert{
			ID:          mq.NewID(),
			Timestamp:   time.Now(),
			Severity:    AlertSeverityWarning,
			Type:        AlertTypeExecutionTime,
			Message:     "Average execution time is too high",
			Threshold:   m.thresholds.MaxExecutionTime,
			ActualValue: metrics.AverageExecutionTime,
			Details: map[string]any{
				"average_execution_time": metrics.AverageExecutionTime.String(),
			},
		})
	}
}

// triggerAlert sends alerts to all registered handlers
func (m *Monitor) triggerAlert(alert Alert) {
	m.logger.Warn("Alert triggered",
		logger.Field{Key: "alert_id", Value: alert.ID},
		logger.Field{Key: "type", Value: string(alert.Type)},
		logger.Field{Key: "severity", Value: string(alert.Severity)},
		logger.Field{Key: "message", Value: alert.Message},
	)

	for _, handler := range m.handlers {
		go func(h AlertHandler, a Alert) {
			if err := h.HandleAlert(a); err != nil {
				m.logger.Error("Alert handler error",
					logger.Field{Key: "error", Value: err.Error()},
					logger.Field{Key: "alert_id", Value: a.ID},
				)
			}
		}(handler, alert)
	}
}
