package mq

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// PerformanceOptimizer provides performance optimization features
type PerformanceOptimizer struct {
	metricsCollector *MetricsCollector
	workerPool       *Pool
	config           *PerformanceConfig
	isEnabled        int32
	shutdown         chan struct{}
	wg               sync.WaitGroup
}

// PerformanceConfig holds performance optimization settings
type PerformanceConfig struct {
	EnableGCOptimization     bool          `json:"enable_gc_optimization"`
	EnableMemoryPooling      bool          `json:"enable_memory_pooling"`
	EnableWorkerAutoscaling  bool          `json:"enable_worker_autoscaling"`
	GCTargetPercentage       int           `json:"gc_target_percentage"`
	MemoryPoolSize           int           `json:"memory_pool_size"`
	WorkerScalingInterval    time.Duration `json:"worker_scaling_interval"`
	PerformanceCheckInterval time.Duration `json:"performance_check_interval"`
	MaxWorkers               int           `json:"max_workers"`
	MinWorkers               int           `json:"min_workers"`
	TargetLatency            time.Duration `json:"target_latency"`
	MemoryThreshold          int64         `json:"memory_threshold"`
}

// MemoryPool provides object pooling for memory optimization
type MemoryPool struct {
	pool sync.Pool
	size int
}

// PerformanceMetrics holds performance-related metrics
type PerformanceMetrics struct {
	AvgTaskLatency    time.Duration `json:"avg_task_latency"`
	Throughput        float64       `json:"throughput"`
	MemoryUsage       int64         `json:"memory_usage"`
	GCCycles          uint32        `json:"gc_cycles"`
	HeapObjects       uint64        `json:"heap_objects"`
	WorkerUtilization float64       `json:"worker_utilization"`
	QueueDepth        int           `json:"queue_depth"`
	ErrorRate         float64       `json:"error_rate"`
	Timestamp         time.Time     `json:"timestamp"`
}

// NewPerformanceOptimizer creates a new performance optimizer
func NewPerformanceOptimizer(metricsCollector *MetricsCollector, workerPool *Pool) *PerformanceOptimizer {
	config := &PerformanceConfig{
		EnableGCOptimization:     true,
		EnableMemoryPooling:      true,
		EnableWorkerAutoscaling:  true,
		GCTargetPercentage:       80,
		MemoryPoolSize:           1024,
		WorkerScalingInterval:    30 * time.Second,
		PerformanceCheckInterval: 10 * time.Second,
		MaxWorkers:               100,
		MinWorkers:               1,
		TargetLatency:            100 * time.Millisecond,
		MemoryThreshold:          512 * 1024 * 1024, // 512MB
	}

	return &PerformanceOptimizer{
		metricsCollector: metricsCollector,
		workerPool:       workerPool,
		config:           config,
		shutdown:         make(chan struct{}),
	}
}

// Start starts the performance optimizer
func (po *PerformanceOptimizer) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&po.isEnabled, 0, 1) {
		return nil // Already started
	}

	// Log starting
	po.workerPool.logger.Info().Msg("Starting performance optimizer")

	// Start performance monitoring
	po.wg.Add(1)
	go po.performanceMonitor(ctx)

	// Start worker autoscaling if enabled
	if po.config.EnableWorkerAutoscaling {
		po.wg.Add(1)
		go po.workerAutoscaler(ctx)
	}

	// Start GC optimization if enabled
	if po.config.EnableGCOptimization {
		po.wg.Add(1)
		go po.gcOptimizer(ctx)
	}

	return nil
}

// Stop stops the performance optimizer
func (po *PerformanceOptimizer) Stop() error {
	if !atomic.CompareAndSwapInt32(&po.isEnabled, 1, 0) {
		return nil
	}

	// Log stopping
	po.workerPool.logger.Info().Msg("Stopping performance optimizer")
	close(po.shutdown)
	po.wg.Wait()
	return nil
}

// performanceMonitor continuously monitors system performance
func (po *PerformanceOptimizer) performanceMonitor(ctx context.Context) {
	defer po.wg.Done()

	ticker := time.NewTicker(po.config.PerformanceCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-po.shutdown:
			return
		case <-ticker.C:
			metrics := po.collectPerformanceMetrics()

			// Record metrics
			po.metricsCollector.RecordMetric("performance.avg_latency", float64(metrics.AvgTaskLatency.Nanoseconds()), nil)
			po.metricsCollector.RecordMetric("performance.throughput", metrics.Throughput, nil)
			po.metricsCollector.RecordMetric("performance.memory_usage", float64(metrics.MemoryUsage), nil)
			po.metricsCollector.RecordMetric("performance.gc_cycles", float64(metrics.GCCycles), nil)
			po.metricsCollector.RecordMetric("performance.worker_utilization", metrics.WorkerUtilization, nil)

			// Log performance issues
			po.checkPerformanceThresholds(metrics)
		}
	}
}

// collectPerformanceMetrics collects current performance metrics
func (po *PerformanceOptimizer) collectPerformanceMetrics() PerformanceMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Get task processing metrics from worker pool
	poolMetrics := po.workerPool.Metrics()

	// Calculate average latency
	var avgLatency time.Duration
	if poolMetrics.CompletedTasks > 0 {
		avgLatency = time.Duration(poolMetrics.ExecutionTime/poolMetrics.CompletedTasks) * time.Millisecond
	}

	// Calculate throughput (tasks per second)
	throughput := float64(poolMetrics.CompletedTasks) / time.Since(time.Now().Add(-time.Minute)).Seconds()

	// Calculate error rate
	var errorRate float64
	if poolMetrics.TotalTasks > 0 {
		errorRate = float64(poolMetrics.ErrorCount) / float64(poolMetrics.TotalTasks) * 100
	}

	// Calculate worker utilization (simplified)
	workerCount := atomic.LoadInt32(&po.workerPool.numOfWorkers)
	queueDepth := po.workerPool.GetQueueDepth()
	var utilization float64
	if workerCount > 0 {
		utilization = float64(queueDepth) / float64(workerCount) * 100
		if utilization > 100 {
			utilization = 100
		}
	}

	return PerformanceMetrics{
		AvgTaskLatency:    avgLatency,
		Throughput:        throughput,
		MemoryUsage:       int64(m.Alloc),
		GCCycles:          m.NumGC,
		HeapObjects:       m.HeapObjects,
		WorkerUtilization: utilization,
		QueueDepth:        queueDepth,
		ErrorRate:         errorRate,
		Timestamp:         time.Now(),
	}
}

// checkPerformanceThresholds checks if performance metrics exceed thresholds
func (po *PerformanceOptimizer) checkPerformanceThresholds(metrics PerformanceMetrics) {
	issues := []string{}

	// Check latency
	if metrics.AvgTaskLatency > po.config.TargetLatency {
		issues = append(issues, fmt.Sprintf("High latency: %v (target: %v)",
			metrics.AvgTaskLatency, po.config.TargetLatency))
	}

	// Check memory usage
	if metrics.MemoryUsage > po.config.MemoryThreshold {
		issues = append(issues, fmt.Sprintf("High memory usage: %d bytes (threshold: %d)",
			metrics.MemoryUsage, po.config.MemoryThreshold))
	}

	// Check worker utilization
	if metrics.WorkerUtilization > 90 {
		issues = append(issues, fmt.Sprintf("High worker utilization: %.2f%%", metrics.WorkerUtilization))
	}

	// Check error rate
	if metrics.ErrorRate > 5 {
		issues = append(issues, fmt.Sprintf("High error rate: %.2f%%", metrics.ErrorRate))
	}

	// Log issues
	if len(issues) > 0 {
		po.workerPool.logger.Warn().Msg(fmt.Sprintf("Performance issues detected: %v", issues))
	}
}

// workerAutoscaler automatically scales workers based on load
func (po *PerformanceOptimizer) workerAutoscaler(ctx context.Context) {
	defer po.wg.Done()

	ticker := time.NewTicker(po.config.WorkerScalingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-po.shutdown:
			return
		case <-ticker.C:
			po.adjustWorkerCount()
		}
	}
}

// adjustWorkerCount adjusts the number of workers based on current load
func (po *PerformanceOptimizer) adjustWorkerCount() {
	metrics := po.collectPerformanceMetrics()
	currentWorkers := int(atomic.LoadInt32(&po.workerPool.numOfWorkers))

	var targetWorkers int

	// Scale based on latency
	if metrics.AvgTaskLatency > po.config.TargetLatency*2 {
		// High latency - add workers
		targetWorkers = currentWorkers + 2
	} else if metrics.AvgTaskLatency > po.config.TargetLatency {
		// Moderate latency - add one worker
		targetWorkers = currentWorkers + 1
	} else if metrics.AvgTaskLatency < po.config.TargetLatency/2 && currentWorkers > po.config.MinWorkers {
		// Low latency - reduce workers
		targetWorkers = currentWorkers - 1
	} else {
		targetWorkers = currentWorkers
	}

	// Apply bounds
	if targetWorkers < po.config.MinWorkers {
		targetWorkers = po.config.MinWorkers
	}
	if targetWorkers > po.config.MaxWorkers {
		targetWorkers = po.config.MaxWorkers
	}

	// Scale based on queue depth
	queueDepth := metrics.QueueDepth
	if queueDepth > currentWorkers*10 {
		targetWorkers = min(targetWorkers+3, po.config.MaxWorkers)
	} else if queueDepth > currentWorkers*5 {
		targetWorkers = min(targetWorkers+1, po.config.MaxWorkers)
	}

	// Apply scaling
	if targetWorkers != currentWorkers {
		po.workerPool.AdjustWorkerCount(targetWorkers)
	}
}

// gcOptimizer optimizes garbage collection
func (po *PerformanceOptimizer) gcOptimizer(ctx context.Context) {
	defer po.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-po.shutdown:
			return
		case <-ticker.C:
			po.optimizeGC()
		}
	}
}

// optimizeGC performs garbage collection optimization
func (po *PerformanceOptimizer) optimizeGC() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Calculate memory usage percentage
	totalMemory := float64(m.Sys)
	usedMemory := float64(m.Alloc)
	usagePercent := (usedMemory / totalMemory) * 100

	// Force GC if memory usage is high
	if usagePercent > float64(po.config.GCTargetPercentage) {
		po.workerPool.logger.Info().Msg(fmt.Sprintf("Forcing garbage collection, memory usage: %.2f%%", usagePercent))
		runtime.GC()
	}
}

// NewMemoryPool creates a new memory pool
func NewMemoryPool(size int) *MemoryPool {
	return &MemoryPool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				return make([]byte, size)
			},
		},
	}
}

// Get gets a buffer from the pool
func (mp *MemoryPool) Get() []byte {
	return mp.pool.Get().([]byte)
}

// Put returns a buffer to the pool
func (mp *MemoryPool) Put(buf []byte) {
	if cap(buf) == mp.size {
		mp.pool.Put(buf[:0]) // Reset length but keep capacity
	}
}

// PerformanceMonitor provides real-time performance monitoring
type PerformanceMonitor struct {
	optimizer *PerformanceOptimizer
	metrics   chan PerformanceMetrics
	stop      chan struct{}
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor(optimizer *PerformanceOptimizer) *PerformanceMonitor {
	return &PerformanceMonitor{
		optimizer: optimizer,
		metrics:   make(chan PerformanceMetrics, 100),
		stop:      make(chan struct{}),
	}
}

// Start starts the performance monitor
func (pm *PerformanceMonitor) Start() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-pm.stop:
				return
			case <-ticker.C:
				select {
				case pm.metrics <- pm.optimizer.collectPerformanceMetrics():
				default:
					// Channel is full, skip this metric
				}
			}
		}
	}()
}

// Stop stops the performance monitor
func (pm *PerformanceMonitor) Stop() {
	close(pm.stop)
}

// GetMetrics returns the latest performance metrics
func (pm *PerformanceMonitor) GetMetrics() (PerformanceMetrics, bool) {
	select {
	case metrics := <-pm.metrics:
		return metrics, true
	default:
		return PerformanceMetrics{}, false
	}
}

// GetMetricsChannel returns the metrics channel
func (pm *PerformanceMonitor) GetMetricsChannel() <-chan PerformanceMetrics {
	return pm.metrics
}

// PerformanceAlert represents a performance alert
type PerformanceAlert struct {
	Type      string             `json:"type"`
	Severity  string             `json:"severity"`
	Message   string             `json:"message"`
	Metrics   PerformanceMetrics `json:"metrics"`
	Threshold any                `json:"threshold"`
	Timestamp time.Time          `json:"timestamp"`
	Details   map[string]any     `json:"details,omitempty"`
}

// PerformanceAlerter manages performance alerts
type PerformanceAlerter struct {
	alerts    []PerformanceAlert
	maxAlerts int
	mu        sync.RWMutex
}

// NewPerformanceAlerter creates a new performance alerter
func NewPerformanceAlerter(maxAlerts int) *PerformanceAlerter {
	return &PerformanceAlerter{
		alerts:    make([]PerformanceAlert, 0),
		maxAlerts: maxAlerts,
	}
}

// AddAlert adds a performance alert
func (pa *PerformanceAlerter) AddAlert(alert PerformanceAlert) {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	alert.Timestamp = time.Now()
	pa.alerts = append(pa.alerts, alert)

	// Keep only recent alerts
	if len(pa.alerts) > pa.maxAlerts {
		pa.alerts = pa.alerts[len(pa.alerts)-pa.maxAlerts:]
	}
}

// GetAlerts returns recent performance alerts
func (pa *PerformanceAlerter) GetAlerts(severity string, limit int) []PerformanceAlert {
	pa.mu.RLock()
	defer pa.mu.RUnlock()

	var filtered []PerformanceAlert
	for _, alert := range pa.alerts {
		if severity == "" || alert.Severity == severity {
			filtered = append(filtered, alert)
		}
	}

	// Return most recent alerts
	if len(filtered) > limit && limit > 0 {
		start := len(filtered) - limit
		return filtered[start:]
	}

	return filtered
}

// ClearAlerts clears all alerts
func (pa *PerformanceAlerter) ClearAlerts() {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	pa.alerts = pa.alerts[:0]
}

// PerformanceDashboard provides a web-based performance dashboard
type PerformanceDashboard struct {
	optimizer *PerformanceOptimizer
	alerter   *PerformanceAlerter
	monitor   *PerformanceMonitor
}

// NewPerformanceDashboard creates a new performance dashboard
func NewPerformanceDashboard(optimizer *PerformanceOptimizer, alerter *PerformanceAlerter, monitor *PerformanceMonitor) *PerformanceDashboard {
	return &PerformanceDashboard{
		optimizer: optimizer,
		alerter:   alerter,
		monitor:   monitor,
	}
}

// GetDashboardData returns data for the performance dashboard
func (pd *PerformanceDashboard) GetDashboardData() map[string]any {
	metrics, hasMetrics := pd.monitor.GetMetrics()
	alerts := pd.alerter.GetAlerts("", 10)

	data := map[string]any{
		"current_metrics": metrics,
		"has_metrics":     hasMetrics,
		"recent_alerts":   alerts,
		"config":          pd.optimizer.config,
		"timestamp":       time.Now(),
	}

	return data
}

// OptimizeForHighLoad optimizes the system for high load scenarios
func (po *PerformanceOptimizer) OptimizeForHighLoad() {
	po.workerPool.logger.Info().Msg("Optimizing for high load")

	// Increase worker count
	currentWorkers := int(atomic.LoadInt32(&po.workerPool.numOfWorkers))
	targetWorkers := min(currentWorkers*2, po.config.MaxWorkers)

	if targetWorkers > currentWorkers {
		po.workerPool.logger.Info().Msg(fmt.Sprintf("Scaling up workers for high load, from %d to %d", currentWorkers, targetWorkers))
		po.workerPool.AdjustWorkerCount(targetWorkers)
	}

	// Force garbage collection
	runtime.GC()

	// Adjust batch size for better throughput
	po.workerPool.SetBatchSize(10)
}

// OptimizeForLowLoad optimizes the system for low load scenarios
func (po *PerformanceOptimizer) OptimizeForLowLoad() {
	po.workerPool.logger.Info().Msg("Optimizing for low load")

	// Reduce worker count
	currentWorkers := int(atomic.LoadInt32(&po.workerPool.numOfWorkers))
	targetWorkers := max(currentWorkers/2, po.config.MinWorkers)

	if targetWorkers < currentWorkers {
		po.workerPool.logger.Info().Msg(fmt.Sprintf("Scaling down workers for low load, from %d to %d", currentWorkers, targetWorkers))
		po.workerPool.AdjustWorkerCount(targetWorkers)
	}

	// Reduce batch size
	po.workerPool.SetBatchSize(1)
}

// GetOptimizationRecommendations returns optimization recommendations
func (po *PerformanceOptimizer) GetOptimizationRecommendations() []string {
	metrics := po.collectPerformanceMetrics()
	recommendations := []string{}

	if metrics.AvgTaskLatency > po.config.TargetLatency {
		recommendations = append(recommendations,
			"Consider increasing worker count to reduce latency")
	}

	if metrics.MemoryUsage > po.config.MemoryThreshold {
		recommendations = append(recommendations,
			"Consider increasing memory limits or optimizing memory usage")
	}

	if metrics.WorkerUtilization > 90 {
		recommendations = append(recommendations,
			"High worker utilization detected, consider scaling up")
	}

	if metrics.ErrorRate > 5 {
		recommendations = append(recommendations,
			"High error rate detected, check for systemic issues")
	}

	if len(recommendations) == 0 {
		recommendations = append(recommendations, "System performance is optimal")
	}

	return recommendations
}
