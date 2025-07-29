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

	if startTime, exists := m.ActiveTasks[taskID]; exists {
		duration := time.Since(startTime)
		m.TotalExecutionTime += duration
		m.LastTaskCompletedAt = time.Now()
		delete(m.ActiveTasks, taskID)
		m.TasksInProgress--

		// Update average execution time
		if m.TasksCompleted > 0 {
			m.AverageExecutionTime = m.TotalExecutionTime / time.Duration(m.TasksCompleted+1)
		}
	}

	switch status {
	case mq.Completed:
		m.TasksCompleted++
	case mq.Failed:
		m.TasksFailed++
	case mq.Cancelled:
		m.TasksCancelled++
	}
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
	if len(m.NodeExecutionTimes[nodeID]) > 100 {
		// Keep only last 100 execution times
		m.NodeExecutionTimes[nodeID] = m.NodeExecutionTimes[nodeID][1:]
	}
	m.NodeExecutionTimes[nodeID] = append(m.NodeExecutionTimes[nodeID], duration)
}

// RecordNodeStart records when a node starts processing
func (m *MonitoringMetrics) RecordNodeStart(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if stats, exists := m.NodeProcessingStats[nodeID]; exists {
		stats.CurrentlyRunning++
	}
}

// RecordNodeEnd records when a node finishes processing
func (m *MonitoringMetrics) RecordNodeEnd(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if stats, exists := m.NodeProcessingStats[nodeID]; exists && stats.CurrentlyRunning > 0 {
		stats.CurrentlyRunning--
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
	for k, v := range m.NodeExecutionTimes {
		snapshot.NodeExecutionTimes[k] = make([]time.Duration, len(v))
		copy(snapshot.NodeExecutionTimes[k], v)
	}
	for k, v := range m.NodeProcessingStats {
		snapshot.NodeProcessingStats[k] = &NodeStats{
			ExecutionCount:   v.ExecutionCount,
			SuccessCount:     v.SuccessCount,
			FailureCount:     v.FailureCount,
			TotalDuration:    v.TotalDuration,
			AverageDuration:  v.AverageDuration,
			MinDuration:      v.MinDuration,
			MaxDuration:      v.MaxDuration,
			LastExecuted:     v.LastExecuted,
			LastSuccess:      v.LastSuccess,
			LastFailure:      v.LastFailure,
			CurrentlyRunning: v.CurrentlyRunning,
		}
	}

	return snapshot
}

// GetNodeStats returns statistics for a specific node
func (m *MonitoringMetrics) GetNodeStats(nodeID string) *NodeStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if stats, exists := m.NodeProcessingStats[nodeID]; exists {
		// Return a copy
		return &NodeStats{
			ExecutionCount:   stats.ExecutionCount,
			SuccessCount:     stats.SuccessCount,
			FailureCount:     stats.FailureCount,
			TotalDuration:    stats.TotalDuration,
			AverageDuration:  stats.AverageDuration,
			MinDuration:      stats.MinDuration,
			MaxDuration:      stats.MaxDuration,
			LastExecuted:     stats.LastExecuted,
			LastSuccess:      stats.LastSuccess,
			LastFailure:      stats.LastFailure,
			CurrentlyRunning: stats.CurrentlyRunning,
		}
	}
	return nil
}

// Monitor provides comprehensive monitoring capabilities for DAG
type Monitor struct {
	dag              *DAG
	metrics          *MonitoringMetrics
	logger           logger.Logger
	alertThresholds  *AlertThresholds
	webhookURL       string
	alertHandlers    []AlertHandler
	monitoringActive bool
	stopCh           chan struct{}
	mu               sync.RWMutex
}

// AlertThresholds defines thresholds for alerting
type AlertThresholds struct {
	MaxFailureRate      float64       // Maximum allowed failure rate (0.0 - 1.0)
	MaxExecutionTime    time.Duration // Maximum allowed execution time
	MaxTasksInProgress  int64         // Maximum allowed concurrent tasks
	MinSuccessRate      float64       // Minimum required success rate
	MaxNodeFailures     int64         // Maximum failures per node
	HealthCheckInterval time.Duration // How often to check health
}

// AlertHandler defines interface for handling alerts
type AlertHandler interface {
	HandleAlert(alert Alert) error
}

// Alert represents a monitoring alert
type Alert struct {
	Type      string
	Severity  string
	Message   string
	NodeID    string
	TaskID    string
	Timestamp time.Time
	Metrics   map[string]interface{}
}

// NewMonitor creates a new DAG monitor
func NewMonitor(dag *DAG, logger logger.Logger) *Monitor {
	return &Monitor{
		dag:     dag,
		metrics: NewMonitoringMetrics(),
		logger:  logger,
		alertThresholds: &AlertThresholds{
			MaxFailureRate:      0.1, // 10% failure rate
			MaxExecutionTime:    5 * time.Minute,
			MaxTasksInProgress:  1000,
			MinSuccessRate:      0.9, // 90% success rate
			MaxNodeFailures:     10,
			HealthCheckInterval: 30 * time.Second,
		},
		stopCh: make(chan struct{}),
	}
}

// Start begins monitoring
func (m *Monitor) Start(ctx context.Context) {
	m.mu.Lock()
	if m.monitoringActive {
		m.mu.Unlock()
		return
	}
	m.monitoringActive = true
	m.mu.Unlock()

	// Start health check routine
	go m.healthCheckRoutine(ctx)

	m.logger.Info("DAG monitoring started")
}

// Stop stops monitoring
func (m *Monitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.monitoringActive {
		return
	}

	close(m.stopCh)
	m.monitoringActive = false
	m.logger.Info("DAG monitoring stopped")
}

// SetAlertThresholds updates alert thresholds
func (m *Monitor) SetAlertThresholds(thresholds *AlertThresholds) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alertThresholds = thresholds
}

// AddAlertHandler adds an alert handler
func (m *Monitor) AddAlertHandler(handler AlertHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alertHandlers = append(m.alertHandlers, handler)
}

// GetMetrics returns current metrics
func (m *Monitor) GetMetrics() *MonitoringMetrics {
	return m.metrics.GetSnapshot()
}

// healthCheckRoutine performs periodic health checks
func (m *Monitor) healthCheckRoutine(ctx context.Context) {
	ticker := time.NewTicker(m.alertThresholds.HealthCheckInterval)
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
	snapshot := m.metrics.GetSnapshot()

	// Check failure rate
	if snapshot.TasksTotal > 0 {
		failureRate := float64(snapshot.TasksFailed) / float64(snapshot.TasksTotal)
		if failureRate > m.alertThresholds.MaxFailureRate {
			m.triggerAlert(Alert{
				Type:      "high_failure_rate",
				Severity:  "warning",
				Message:   fmt.Sprintf("High failure rate: %.2f%%", failureRate*100),
				Timestamp: time.Now(),
				Metrics: map[string]interface{}{
					"failure_rate": failureRate,
					"total_tasks":  snapshot.TasksTotal,
					"failed_tasks": snapshot.TasksFailed,
				},
			})
		}
	}

	// Check tasks in progress
	if snapshot.TasksInProgress > m.alertThresholds.MaxTasksInProgress {
		m.triggerAlert(Alert{
			Type:      "high_task_load",
			Severity:  "warning",
			Message:   fmt.Sprintf("High number of tasks in progress: %d", snapshot.TasksInProgress),
			Timestamp: time.Now(),
			Metrics: map[string]interface{}{
				"tasks_in_progress": snapshot.TasksInProgress,
				"threshold":         m.alertThresholds.MaxTasksInProgress,
			},
		})
	}

	// Check node failures
	for nodeID, failures := range snapshot.NodeFailures {
		if failures > m.alertThresholds.MaxNodeFailures {
			m.triggerAlert(Alert{
				Type:      "node_failures",
				Severity:  "error",
				Message:   fmt.Sprintf("Node %s has %d failures", nodeID, failures),
				NodeID:    nodeID,
				Timestamp: time.Now(),
				Metrics: map[string]interface{}{
					"node_id":  nodeID,
					"failures": failures,
				},
			})
		}
	}

	// Check execution time
	if snapshot.AverageExecutionTime > m.alertThresholds.MaxExecutionTime {
		m.triggerAlert(Alert{
			Type:      "slow_execution",
			Severity:  "warning",
			Message:   fmt.Sprintf("Average execution time is high: %v", snapshot.AverageExecutionTime),
			Timestamp: time.Now(),
			Metrics: map[string]interface{}{
				"average_execution_time": snapshot.AverageExecutionTime,
				"threshold":              m.alertThresholds.MaxExecutionTime,
			},
		})
	}
}

// triggerAlert sends alerts to all registered handlers
func (m *Monitor) triggerAlert(alert Alert) {
	m.logger.Warn("Alert triggered",
		logger.Field{Key: "type", Value: alert.Type},
		logger.Field{Key: "severity", Value: alert.Severity},
		logger.Field{Key: "message", Value: alert.Message},
	)

	for _, handler := range m.alertHandlers {
		if err := handler.HandleAlert(alert); err != nil {
			m.logger.Error("Alert handler failed",
				logger.Field{Key: "error", Value: err.Error()},
			)
		}
	}
}
