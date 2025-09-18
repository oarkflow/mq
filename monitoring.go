package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/mq/logger"
)

// MetricsServer provides comprehensive monitoring and metrics
type MetricsServer struct {
	broker        *Broker
	config        *MonitoringConfig
	logger        logger.Logger
	server        *http.Server
	registry      *DetailedMetricsRegistry
	healthChecker *SystemHealthChecker
	alertManager  *AlertManager
	isRunning     int32
	shutdown      chan struct{}
	wg            sync.WaitGroup
}

// DetailedMetricsRegistry stores and manages metrics with enhanced features
type DetailedMetricsRegistry struct {
	metrics map[string]*TimeSeries
	mu      sync.RWMutex
}

// TimeSeries represents a time series metric
type TimeSeries struct {
	Name        string            `json:"name"`
	Type        MetricType        `json:"type"`
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels"`
	Values      []TimeSeriesPoint `json:"values"`
	MaxPoints   int               `json:"max_points"`
	mu          sync.RWMutex
}

// TimeSeriesPoint represents a single point in a time series
type TimeSeriesPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// MetricType represents the type of metric
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

// SystemHealthChecker monitors system health
type SystemHealthChecker struct {
	checks  map[string]HealthCheck
	results map[string]*HealthCheckResult
	mu      sync.RWMutex
	logger  logger.Logger
}

// HealthCheck interface for health checks
type HealthCheck interface {
	Name() string
	Check(ctx context.Context) *HealthCheckResult
	Timeout() time.Duration
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	Name      string         `json:"name"`
	Status    HealthStatus   `json:"status"`
	Message   string         `json:"message"`
	Duration  time.Duration  `json:"duration"`
	Timestamp time.Time      `json:"timestamp"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// HealthStatus represents the health status
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
	HealthStatusWarning   HealthStatus = "warning"
	HealthStatusUnknown   HealthStatus = "unknown"
)

// AlertManager manages alerts and notifications
type AlertManager struct {
	rules     []AlertRule
	alerts    []ActiveAlert
	notifiers []AlertNotifier
	mu        sync.RWMutex
	logger    logger.Logger
}

// AlertRule defines conditions for triggering alerts
type AlertRule struct {
	Name        string            `json:"name"`
	Metric      string            `json:"metric"`
	Condition   string            `json:"condition"` // "gt", "lt", "eq", "gte", "lte"
	Threshold   float64           `json:"threshold"`
	Duration    time.Duration     `json:"duration"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	Enabled     bool              `json:"enabled"`
}

// ActiveAlert represents an active alert
type ActiveAlert struct {
	Rule        AlertRule         `json:"rule"`
	Value       float64           `json:"value"`
	StartsAt    time.Time         `json:"starts_at"`
	EndsAt      *time.Time        `json:"ends_at,omitempty"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	Status      AlertStatus       `json:"status"`
}

// AlertStatus represents the status of an alert
type AlertStatus string

const (
	AlertStatusFiring   AlertStatus = "firing"
	AlertStatusResolved AlertStatus = "resolved"
	AlertStatusSilenced AlertStatus = "silenced"
)

// AlertNotifier interface for alert notifications
type AlertNotifier interface {
	Notify(ctx context.Context, alert ActiveAlert) error
	Name() string
}

// NewMetricsServer creates a new metrics server
func NewMetricsServer(broker *Broker, config *MonitoringConfig, logger logger.Logger) *MetricsServer {
	return &MetricsServer{
		broker:        broker,
		config:        config,
		logger:        logger,
		registry:      NewDetailedMetricsRegistry(),
		healthChecker: NewSystemHealthChecker(logger),
		alertManager:  NewAlertManager(logger),
		shutdown:      make(chan struct{}),
	}
}

// NewMetricsRegistry creates a new metrics registry
func NewDetailedMetricsRegistry() *DetailedMetricsRegistry {
	return &DetailedMetricsRegistry{
		metrics: make(map[string]*TimeSeries),
	}
}

// RegisterMetric registers a new metric
func (mr *DetailedMetricsRegistry) RegisterMetric(name string, metricType MetricType, description string, labels map[string]string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	mr.metrics[name] = &TimeSeries{
		Name:        name,
		Type:        metricType,
		Description: description,
		Labels:      labels,
		Values:      make([]TimeSeriesPoint, 0),
		MaxPoints:   1000, // Keep last 1000 points
	}
}

// RecordValue records a value for a metric
func (mr *DetailedMetricsRegistry) RecordValue(name string, value float64) {
	mr.mu.RLock()
	metric, exists := mr.metrics[name]
	mr.mu.RUnlock()

	if !exists {
		return
	}

	metric.mu.Lock()
	defer metric.mu.Unlock()

	point := TimeSeriesPoint{
		Timestamp: time.Now(),
		Value:     value,
	}

	metric.Values = append(metric.Values, point)

	// Keep only the last MaxPoints
	if len(metric.Values) > metric.MaxPoints {
		metric.Values = metric.Values[len(metric.Values)-metric.MaxPoints:]
	}
}

// GetMetric returns a metric by name
func (mr *DetailedMetricsRegistry) GetMetric(name string) (*TimeSeries, bool) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	metric, exists := mr.metrics[name]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent external modification
	metric.mu.RLock()
	defer metric.mu.RUnlock()

	metricCopy := &TimeSeries{
		Name:        metric.Name,
		Type:        metric.Type,
		Description: metric.Description,
		Labels:      make(map[string]string),
		Values:      make([]TimeSeriesPoint, len(metric.Values)),
		MaxPoints:   metric.MaxPoints,
	}

	for k, v := range metric.Labels {
		metricCopy.Labels[k] = v
	}

	copy(metricCopy.Values, metric.Values)

	return metricCopy, true
}

// GetAllMetrics returns all metrics
func (mr *DetailedMetricsRegistry) GetAllMetrics() map[string]*TimeSeries {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	result := make(map[string]*TimeSeries)
	for name := range mr.metrics {
		result[name], _ = mr.GetMetric(name)
	}

	return result
}

// NewSystemHealthChecker creates a new system health checker
func NewSystemHealthChecker(logger logger.Logger) *SystemHealthChecker {
	checker := &SystemHealthChecker{
		checks:  make(map[string]HealthCheck),
		results: make(map[string]*HealthCheckResult),
		logger:  logger,
	}

	// Register default health checks
	checker.RegisterCheck(&MemoryHealthCheck{})
	checker.RegisterCheck(&GoRoutineHealthCheck{})
	checker.RegisterCheck(&DiskSpaceHealthCheck{})

	return checker
}

// RegisterCheck registers a health check
func (shc *SystemHealthChecker) RegisterCheck(check HealthCheck) {
	shc.mu.Lock()
	defer shc.mu.Unlock()
	shc.checks[check.Name()] = check
}

// RunChecks runs all health checks
func (shc *SystemHealthChecker) RunChecks(ctx context.Context) map[string]*HealthCheckResult {
	shc.mu.RLock()
	checks := make(map[string]HealthCheck)
	for name, check := range shc.checks {
		checks[name] = check
	}
	shc.mu.RUnlock()

	results := make(map[string]*HealthCheckResult)
	var wg sync.WaitGroup

	for name, check := range checks {
		wg.Add(1)
		go func(name string, check HealthCheck) {
			defer wg.Done()

			checkCtx, cancel := context.WithTimeout(ctx, check.Timeout())
			defer cancel()

			result := check.Check(checkCtx)
			results[name] = result

			shc.mu.Lock()
			shc.results[name] = result
			shc.mu.Unlock()
		}(name, check)
	}

	wg.Wait()
	return results
}

// GetOverallHealth returns the overall system health
func (shc *SystemHealthChecker) GetOverallHealth() HealthStatus {
	shc.mu.RLock()
	defer shc.mu.RUnlock()

	if len(shc.results) == 0 {
		return HealthStatusUnknown
	}

	hasUnhealthy := false
	hasWarning := false

	for _, result := range shc.results {
		switch result.Status {
		case HealthStatusUnhealthy:
			hasUnhealthy = true
		case HealthStatusWarning:
			hasWarning = true
		}
	}

	if hasUnhealthy {
		return HealthStatusUnhealthy
	}
	if hasWarning {
		return HealthStatusWarning
	}

	return HealthStatusHealthy
}

// MemoryHealthCheck checks memory usage
type MemoryHealthCheck struct{}

func (mhc *MemoryHealthCheck) Name() string {
	return "memory"
}

func (mhc *MemoryHealthCheck) Timeout() time.Duration {
	return 5 * time.Second
}

func (mhc *MemoryHealthCheck) Check(ctx context.Context) *HealthCheckResult {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Convert to MB
	allocMB := float64(m.Alloc) / 1024 / 1024
	sysMB := float64(m.Sys) / 1024 / 1024

	status := HealthStatusHealthy
	message := fmt.Sprintf("Memory usage: %.2f MB allocated, %.2f MB system", allocMB, sysMB)

	// Simple thresholds (should be configurable)
	if allocMB > 1000 { // 1GB
		status = HealthStatusWarning
		message += " (high memory usage)"
	}
	if allocMB > 2000 { // 2GB
		status = HealthStatusUnhealthy
		message += " (critical memory usage)"
	}

	return &HealthCheckResult{
		Name:      mhc.Name(),
		Status:    status,
		Message:   message,
		Timestamp: time.Now(),
		Metadata: map[string]any{
			"alloc_mb":   allocMB,
			"sys_mb":     sysMB,
			"gc_cycles":  m.NumGC,
			"goroutines": runtime.NumGoroutine(),
		},
	}
}

// GoRoutineHealthCheck checks goroutine count
type GoRoutineHealthCheck struct{}

func (ghc *GoRoutineHealthCheck) Name() string {
	return "goroutines"
}

func (ghc *GoRoutineHealthCheck) Timeout() time.Duration {
	return 5 * time.Second
}

func (ghc *GoRoutineHealthCheck) Check(ctx context.Context) *HealthCheckResult {
	count := runtime.NumGoroutine()

	status := HealthStatusHealthy
	message := fmt.Sprintf("Goroutines: %d", count)

	// Simple thresholds
	if count > 1000 {
		status = HealthStatusWarning
		message += " (high goroutine count)"
	}
	if count > 5000 {
		status = HealthStatusUnhealthy
		message += " (critical goroutine count)"
	}

	return &HealthCheckResult{
		Name:      ghc.Name(),
		Status:    status,
		Message:   message,
		Timestamp: time.Now(),
		Metadata: map[string]any{
			"count": count,
		},
	}
}

// DiskSpaceHealthCheck checks available disk space
type DiskSpaceHealthCheck struct{}

func (dshc *DiskSpaceHealthCheck) Name() string {
	return "disk_space"
}

func (dshc *DiskSpaceHealthCheck) Timeout() time.Duration {
	return 5 * time.Second
}

func (dshc *DiskSpaceHealthCheck) Check(ctx context.Context) *HealthCheckResult {
	// This is a simplified implementation
	// In production, you would check actual disk space
	return &HealthCheckResult{
		Name:      dshc.Name(),
		Status:    HealthStatusHealthy,
		Message:   "Disk space OK",
		Timestamp: time.Now(),
		Metadata: map[string]any{
			"available_gb": 100.0, // Placeholder
		},
	}
}

// NewAlertManager creates a new alert manager
func NewAlertManager(logger logger.Logger) *AlertManager {
	return &AlertManager{
		rules:     make([]AlertRule, 0),
		alerts:    make([]ActiveAlert, 0),
		notifiers: make([]AlertNotifier, 0),
		logger:    logger,
	}
}

// AddRule adds an alert rule
func (am *AlertManager) AddRule(rule AlertRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.rules = append(am.rules, rule)
}

// AddNotifier adds an alert notifier
func (am *AlertManager) AddNotifier(notifier AlertNotifier) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.notifiers = append(am.notifiers, notifier)
}

// EvaluateRules evaluates all alert rules against current metrics
func (am *AlertManager) EvaluateRules(registry *DetailedMetricsRegistry) {
	am.mu.Lock()
	defer am.mu.Unlock()

	now := time.Now()

	for _, rule := range am.rules {
		if !rule.Enabled {
			continue
		}

		metric, exists := registry.GetMetric(rule.Metric)
		if !exists {
			continue
		}

		if len(metric.Values) == 0 {
			continue
		}

		// Get the latest value
		latestValue := metric.Values[len(metric.Values)-1].Value

		// Check if condition is met
		conditionMet := false
		switch rule.Condition {
		case "gt":
			conditionMet = latestValue > rule.Threshold
		case "gte":
			conditionMet = latestValue >= rule.Threshold
		case "lt":
			conditionMet = latestValue < rule.Threshold
		case "lte":
			conditionMet = latestValue <= rule.Threshold
		case "eq":
			conditionMet = latestValue == rule.Threshold
		}

		// Find existing alert
		var existingAlert *ActiveAlert
		for i := range am.alerts {
			if am.alerts[i].Rule.Name == rule.Name && am.alerts[i].Status == AlertStatusFiring {
				existingAlert = &am.alerts[i]
				break
			}
		}

		if conditionMet {
			if existingAlert == nil {
				// Create new alert
				alert := ActiveAlert{
					Rule:        rule,
					Value:       latestValue,
					StartsAt:    now,
					Labels:      rule.Labels,
					Annotations: rule.Annotations,
					Status:      AlertStatusFiring,
				}
				am.alerts = append(am.alerts, alert)

				// Notify
				for _, notifier := range am.notifiers {
					go func(n AlertNotifier, a ActiveAlert) {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						if err := n.Notify(ctx, a); err != nil {
							am.logger.Error("Failed to send alert notification",
								logger.Field{Key: "notifier", Value: n.Name()},
								logger.Field{Key: "alert", Value: a.Rule.Name},
								logger.Field{Key: "error", Value: err.Error()})
						}
					}(notifier, alert)
				}
			} else {
				// Update existing alert
				existingAlert.Value = latestValue
			}
		} else if existingAlert != nil {
			// Resolve alert
			endTime := now
			existingAlert.EndsAt = &endTime
			existingAlert.Status = AlertStatusResolved

			// Notify resolution
			for _, notifier := range am.notifiers {
				go func(n AlertNotifier, a ActiveAlert) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := n.Notify(ctx, a); err != nil {
						am.logger.Error("Failed to send alert resolution notification",
							logger.Field{Key: "notifier", Value: n.Name()},
							logger.Field{Key: "alert", Value: a.Rule.Name},
							logger.Field{Key: "error", Value: err.Error()})
					}
				}(notifier, *existingAlert)
			}
		}
	}
}

// AddAlertRule adds an alert rule to the metrics server
func (ms *MetricsServer) AddAlertRule(rule AlertRule) {
	ms.alertManager.AddRule(rule)
}

// AddAlertNotifier adds an alert notifier to the metrics server
func (ms *MetricsServer) AddAlertNotifier(notifier AlertNotifier) {
	ms.alertManager.AddNotifier(notifier)
}

// Start starts the metrics server
func (ms *MetricsServer) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&ms.isRunning, 0, 1) {
		return fmt.Errorf("metrics server is already running")
	}

	// Register default metrics
	ms.registerDefaultMetrics()

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", ms.handleMetrics)
	mux.HandleFunc("/health", ms.handleHealth)
	mux.HandleFunc("/alerts", ms.handleAlerts)

	ms.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", ms.config.MetricsPort),
		Handler: mux,
	}

	// Start collection routines
	ms.wg.Add(1)
	go ms.metricsCollectionLoop(ctx)

	ms.wg.Add(1)
	go ms.healthCheckLoop(ctx)

	ms.wg.Add(1)
	go ms.alertEvaluationLoop(ctx)

	// Start HTTP server
	go func() {
		ms.logger.Info("Metrics server starting",
			logger.Field{Key: "port", Value: ms.config.MetricsPort})

		if err := ms.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			ms.logger.Error("Metrics server error",
				logger.Field{Key: "error", Value: err.Error()})
		}
	}()

	return nil
}

// Stop stops the metrics server
func (ms *MetricsServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&ms.isRunning, 1, 0) {
		return nil
	}

	close(ms.shutdown)

	// Stop HTTP server
	if ms.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ms.server.Shutdown(ctx)
	}

	// Wait for goroutines to finish
	ms.wg.Wait()

	ms.logger.Info("Metrics server stopped")
	return nil
}

// registerDefaultMetrics registers default system metrics
func (ms *MetricsServer) registerDefaultMetrics() {
	ms.registry.RegisterMetric("mq_broker_connections_total", MetricTypeGauge, "Total number of broker connections", nil)
	ms.registry.RegisterMetric("mq_messages_processed_total", MetricTypeCounter, "Total number of processed messages", nil)
	ms.registry.RegisterMetric("mq_messages_failed_total", MetricTypeCounter, "Total number of failed messages", nil)
	ms.registry.RegisterMetric("mq_queue_depth", MetricTypeGauge, "Current queue depth", nil)
	ms.registry.RegisterMetric("mq_memory_usage_bytes", MetricTypeGauge, "Memory usage in bytes", nil)
	ms.registry.RegisterMetric("mq_goroutines_total", MetricTypeGauge, "Total number of goroutines", nil)
	ms.registry.RegisterMetric("mq_gc_duration_seconds", MetricTypeGauge, "GC duration in seconds", nil)
}

// metricsCollectionLoop collects metrics periodically
func (ms *MetricsServer) metricsCollectionLoop(ctx context.Context) {
	defer ms.wg.Done()

	ticker := time.NewTicker(1 * time.Minute) // Default to 1 minute if not configured
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ms.shutdown:
			return
		case <-ticker.C:
			ms.collectSystemMetrics()
			ms.collectBrokerMetrics()
		}
	}
}

// collectSystemMetrics collects system-level metrics
func (ms *MetricsServer) collectSystemMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ms.registry.RecordValue("mq_memory_usage_bytes", float64(m.Alloc))
	ms.registry.RecordValue("mq_goroutines_total", float64(runtime.NumGoroutine()))
	ms.registry.RecordValue("mq_gc_duration_seconds", float64(m.PauseTotalNs)/1e9)
}

// collectBrokerMetrics collects broker-specific metrics
func (ms *MetricsServer) collectBrokerMetrics() {
	if ms.broker == nil {
		return
	}

	// Collect connection metrics
	activeConns := ms.broker.connectionPool.GetActiveConnections()
	ms.registry.RecordValue("mq_broker_connections_total", float64(activeConns))

	// Collect queue metrics
	totalDepth := 0
	ms.broker.queues.ForEach(func(name string, queue *Queue) bool {
		depth := len(queue.tasks)
		totalDepth += depth

		// Record per-queue metrics with labels
		queueMetric := fmt.Sprintf("mq_queue_depth{queue=\"%s\"}", name)
		ms.registry.RegisterMetric(queueMetric, MetricTypeGauge, "Queue depth for specific queue", map[string]string{"queue": name})
		ms.registry.RecordValue(queueMetric, float64(depth))

		return true
	})

	ms.registry.RecordValue("mq_queue_depth", float64(totalDepth))
}

// healthCheckLoop runs health checks periodically
func (ms *MetricsServer) healthCheckLoop(ctx context.Context) {
	defer ms.wg.Done()

	ticker := time.NewTicker(ms.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ms.shutdown:
			return
		case <-ticker.C:
			ms.healthChecker.RunChecks(ctx)
		}
	}
}

// alertEvaluationLoop evaluates alerts periodically
func (ms *MetricsServer) alertEvaluationLoop(ctx context.Context) {
	defer ms.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // Evaluate every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ms.shutdown:
			return
		case <-ticker.C:
			ms.alertManager.EvaluateRules(ms.registry)
		}
	}
}

// handleMetrics handles the /metrics endpoint
func (ms *MetricsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := ms.registry.GetAllMetrics()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"timestamp": time.Now(),
		"metrics":   metrics,
	})
}

// handleHealth handles the /health endpoint
func (ms *MetricsServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	results := ms.healthChecker.RunChecks(r.Context())
	overallHealth := ms.healthChecker.GetOverallHealth()

	response := map[string]any{
		"status":    overallHealth,
		"timestamp": time.Now(),
		"checks":    results,
	}

	w.Header().Set("Content-Type", "application/json")

	// Set HTTP status based on health
	switch overallHealth {
	case HealthStatusHealthy:
		w.WriteHeader(http.StatusOK)
	case HealthStatusWarning:
		w.WriteHeader(http.StatusOK) // Still OK but with warnings
	case HealthStatusUnhealthy:
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}

	json.NewEncoder(w).Encode(response)
}

// handleAlerts handles the /alerts endpoint
func (ms *MetricsServer) handleAlerts(w http.ResponseWriter, r *http.Request) {
	ms.alertManager.mu.RLock()
	alerts := make([]ActiveAlert, len(ms.alertManager.alerts))
	copy(alerts, ms.alertManager.alerts)
	ms.alertManager.mu.RUnlock()

	// Sort alerts by start time (newest first)
	sort.Slice(alerts, func(i, j int) bool {
		return alerts[i].StartsAt.After(alerts[j].StartsAt)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"timestamp": time.Now(),
		"alerts":    alerts,
	})
}

// LogNotifier sends alerts to logs
type LogNotifier struct {
	logger logger.Logger
}

func NewLogNotifier(logger logger.Logger) *LogNotifier {
	return &LogNotifier{logger: logger}
}

func (ln *LogNotifier) Name() string {
	return "log"
}

func (ln *LogNotifier) Notify(ctx context.Context, alert ActiveAlert) error {
	level := "info"
	if alert.Status == AlertStatusFiring {
		level = "error"
	}

	message := fmt.Sprintf("Alert %s: %s (value: %.2f, threshold: %.2f)",
		alert.Status, alert.Rule.Name, alert.Value, alert.Rule.Threshold)

	if level == "error" {
		ln.logger.Error(message,
			logger.Field{Key: "alert_name", Value: alert.Rule.Name},
			logger.Field{Key: "alert_status", Value: string(alert.Status)},
			logger.Field{Key: "value", Value: alert.Value},
			logger.Field{Key: "threshold", Value: alert.Rule.Threshold})
	} else {
		ln.logger.Info(message,
			logger.Field{Key: "alert_name", Value: alert.Rule.Name},
			logger.Field{Key: "alert_status", Value: string(alert.Status)},
			logger.Field{Key: "value", Value: alert.Value},
			logger.Field{Key: "threshold", Value: alert.Rule.Threshold})
	}

	return nil
}
