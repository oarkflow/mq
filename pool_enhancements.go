package mq

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/mq/logger"
)

// WorkerHealth represents the health status of a worker
type WorkerHealth struct {
	WorkerID         int
	IsHealthy        bool
	LastHeartbeat    time.Time
	TasksProcessed   int64
	ErrorCount       int64
	AvgProcessTime   time.Duration
	CurrentTaskID    string
	CurrentTaskStart time.Time
	MemoryUsage      uint64
}

// WorkerHealthMonitor monitors worker health and performs auto-recovery
type WorkerHealthMonitor struct {
	pool              *Pool
	workers           map[int]*WorkerHealth
	mu                sync.RWMutex
	heartbeatTimeout  time.Duration
	checkInterval     time.Duration
	shutdown          chan struct{}
	logger            logger.Logger
	onUnhealthyWorker func(workerID int, health *WorkerHealth)
	onRecoveredWorker func(workerID int)
}

// NewWorkerHealthMonitor creates a new worker health monitor
func NewWorkerHealthMonitor(pool *Pool, heartbeatTimeout, checkInterval time.Duration, log logger.Logger) *WorkerHealthMonitor {
	if heartbeatTimeout == 0 {
		heartbeatTimeout = 30 * time.Second
	}
	if checkInterval == 0 {
		checkInterval = 10 * time.Second
	}

	monitor := &WorkerHealthMonitor{
		pool:             pool,
		workers:          make(map[int]*WorkerHealth),
		heartbeatTimeout: heartbeatTimeout,
		checkInterval:    checkInterval,
		shutdown:         make(chan struct{}),
		logger:           log,
	}

	go monitor.monitorLoop()

	return monitor
}

// RecordHeartbeat records a heartbeat from a worker
func (m *WorkerHealthMonitor) RecordHeartbeat(workerID int, taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	health, exists := m.workers[workerID]
	if !exists {
		health = &WorkerHealth{
			WorkerID:  workerID,
			IsHealthy: true,
		}
		m.workers[workerID] = health
	}

	health.LastHeartbeat = time.Now()
	health.IsHealthy = true
	health.CurrentTaskID = taskID
	if taskID != "" {
		health.CurrentTaskStart = time.Now()
	} else {
		health.CurrentTaskStart = time.Time{}
	}
}

// RecordTaskCompletion records task completion metrics
func (m *WorkerHealthMonitor) RecordTaskCompletion(workerID int, processingTime time.Duration, isError bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	health, exists := m.workers[workerID]
	if !exists {
		return
	}

	health.TasksProcessed++
	if isError {
		health.ErrorCount++
	}

	// Update average processing time
	if health.AvgProcessTime == 0 {
		health.AvgProcessTime = processingTime
	} else {
		health.AvgProcessTime = (health.AvgProcessTime + processingTime) / 2
	}

	health.CurrentTaskID = ""
	health.CurrentTaskStart = time.Time{}
}

// monitorLoop continuously monitors worker health
func (m *WorkerHealthMonitor) monitorLoop() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkWorkerHealth()
		case <-m.shutdown:
			return
		}
	}
}

// checkWorkerHealth checks the health of all workers
func (m *WorkerHealthMonitor) checkWorkerHealth() {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	for workerID, health := range m.workers {
		// Check if worker has timed out
		if now.Sub(health.LastHeartbeat) > m.heartbeatTimeout {
			if health.IsHealthy {
				health.IsHealthy = false
				m.logger.Warn("Worker unhealthy - heartbeat timeout",
					logger.Field{Key: "workerID", Value: workerID},
					logger.Field{Key: "lastHeartbeat", Value: health.LastHeartbeat},
					logger.Field{Key: "currentTask", Value: health.CurrentTaskID})

				if m.onUnhealthyWorker != nil {
					go m.onUnhealthyWorker(workerID, health)
				}

				// Attempt to restart the worker
				go m.restartWorker(workerID)
			}
		}

		// Check if worker is stuck on a task
		if !health.CurrentTaskStart.IsZero() && now.Sub(health.CurrentTaskStart) > m.heartbeatTimeout*2 {
			m.logger.Warn("Worker stuck on task",
				logger.Field{Key: "workerID", Value: workerID},
				logger.Field{Key: "taskID", Value: health.CurrentTaskID},
				logger.Field{Key: "duration", Value: now.Sub(health.CurrentTaskStart)})
		}

		// Update memory usage
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		health.MemoryUsage = memStats.Alloc
	}
}

// restartWorker attempts to restart an unhealthy worker
func (m *WorkerHealthMonitor) restartWorker(workerID int) {
	m.logger.Info("Attempting to restart unhealthy worker",
		logger.Field{Key: "workerID", Value: workerID})

	// Signal the pool to start a replacement worker
	m.pool.wg.Add(1)
	go m.pool.worker()

	// Mark as recovered
	m.mu.Lock()
	if health, exists := m.workers[workerID]; exists {
		health.IsHealthy = true
		health.LastHeartbeat = time.Now()
	}
	m.mu.Unlock()

	if m.onRecoveredWorker != nil {
		m.onRecoveredWorker(workerID)
	}

	m.logger.Info("Worker restarted successfully",
		logger.Field{Key: "workerID", Value: workerID})
}

// GetHealthStats returns health statistics for all workers
func (m *WorkerHealthMonitor) GetHealthStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthyCount := 0
	unhealthyCount := 0
	totalTasks := int64(0)
	totalErrors := int64(0)

	for _, health := range m.workers {
		if health.IsHealthy {
			healthyCount++
		} else {
			unhealthyCount++
		}
		totalTasks += health.TasksProcessed
		totalErrors += health.ErrorCount
	}

	return map[string]interface{}{
		"total_workers":         len(m.workers),
		"healthy_workers":       healthyCount,
		"unhealthy_workers":     unhealthyCount,
		"total_tasks_processed": totalTasks,
		"total_errors":          totalErrors,
	}
}

// GetWorkerHealth returns health info for a specific worker
func (m *WorkerHealthMonitor) GetWorkerHealth(workerID int) (*WorkerHealth, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	health, exists := m.workers[workerID]
	return health, exists
}

// SetOnUnhealthyWorker sets callback for unhealthy worker detection
func (m *WorkerHealthMonitor) SetOnUnhealthyWorker(fn func(workerID int, health *WorkerHealth)) {
	m.onUnhealthyWorker = fn
}

// SetOnRecoveredWorker sets callback for worker recovery
func (m *WorkerHealthMonitor) SetOnRecoveredWorker(fn func(workerID int)) {
	m.onRecoveredWorker = fn
}

// Shutdown stops the health monitor
func (m *WorkerHealthMonitor) Shutdown() {
	close(m.shutdown)
}

// DynamicScaler dynamically scales workers based on load
type DynamicScaler struct {
	pool               *Pool
	minWorkers         int
	maxWorkers         int
	scaleUpThreshold   float64 // Task queue utilization % to scale up
	scaleDownThreshold float64 // Task queue utilization % to scale down
	cooldownPeriod     time.Duration
	lastScaleTime      time.Time
	mu                 sync.RWMutex
	shutdown           chan struct{}
	logger             logger.Logger
	checkInterval      time.Duration
	onScaleUp          func(oldCount, newCount int)
	onScaleDown        func(oldCount, newCount int)
}

// NewDynamicScaler creates a new dynamic worker scaler
func NewDynamicScaler(pool *Pool, minWorkers, maxWorkers int, log logger.Logger) *DynamicScaler {
	scaler := &DynamicScaler{
		pool:               pool,
		minWorkers:         minWorkers,
		maxWorkers:         maxWorkers,
		scaleUpThreshold:   0.75, // Scale up when 75% full
		scaleDownThreshold: 0.25, // Scale down when 25% full
		cooldownPeriod:     30 * time.Second,
		shutdown:           make(chan struct{}),
		logger:             log,
		checkInterval:      10 * time.Second,
	}

	go scaler.scaleLoop()

	return scaler
}

// scaleLoop continuously monitors and scales workers
func (s *DynamicScaler) scaleLoop() {
	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.evaluateScaling()
		case <-s.shutdown:
			return
		}
	}
}

// evaluateScaling evaluates whether to scale up or down
func (s *DynamicScaler) evaluateScaling() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check cooldown period
	if time.Since(s.lastScaleTime) < s.cooldownPeriod {
		return
	}

	// Get current metrics
	currentWorkers := int(atomic.LoadInt32(&s.pool.numOfWorkers))

	s.pool.taskQueueLock.Lock()
	queueSize := len(s.pool.taskQueue)
	s.pool.taskQueueLock.Unlock()

	// Calculate utilization
	queueCapacity := cap(s.pool.taskQueue)
	if queueCapacity == 0 {
		return
	}

	utilization := float64(queueSize) / float64(queueCapacity)

	// Decide to scale up or down
	if utilization >= s.scaleUpThreshold && currentWorkers < s.maxWorkers {
		newWorkers := currentWorkers + 1
		if newWorkers > s.maxWorkers {
			newWorkers = s.maxWorkers
		}
		s.scaleUp(currentWorkers, newWorkers)
	} else if utilization <= s.scaleDownThreshold && currentWorkers > s.minWorkers {
		newWorkers := currentWorkers - 1
		if newWorkers < s.minWorkers {
			newWorkers = s.minWorkers
		}
		s.scaleDown(currentWorkers, newWorkers)
	}
}

// scaleUp increases the number of workers
func (s *DynamicScaler) scaleUp(oldCount, newCount int) {
	additionalWorkers := newCount - oldCount

	s.logger.Info("Scaling up workers",
		logger.Field{Key: "oldCount", Value: oldCount},
		logger.Field{Key: "newCount", Value: newCount})

	for i := 0; i < additionalWorkers; i++ {
		s.pool.wg.Add(1)
		go s.pool.worker()
	}

	atomic.StoreInt32(&s.pool.numOfWorkers, int32(newCount))
	s.lastScaleTime = time.Now()

	if s.onScaleUp != nil {
		s.onScaleUp(oldCount, newCount)
	}
}

// scaleDown decreases the number of workers
func (s *DynamicScaler) scaleDown(oldCount, newCount int) {
	s.logger.Info("Scaling down workers",
		logger.Field{Key: "oldCount", Value: oldCount},
		logger.Field{Key: "newCount", Value: newCount})

	atomic.StoreInt32(&s.pool.numOfWorkers, int32(newCount))
	s.lastScaleTime = time.Now()

	// Workers will naturally exit when they check numOfWorkers

	if s.onScaleDown != nil {
		s.onScaleDown(oldCount, newCount)
	}
}

// SetScaleUpThreshold sets the threshold for scaling up
func (s *DynamicScaler) SetScaleUpThreshold(threshold float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scaleUpThreshold = threshold
}

// SetScaleDownThreshold sets the threshold for scaling down
func (s *DynamicScaler) SetScaleDownThreshold(threshold float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scaleDownThreshold = threshold
}

// SetOnScaleUp sets callback for scale up events
func (s *DynamicScaler) SetOnScaleUp(fn func(oldCount, newCount int)) {
	s.onScaleUp = fn
}

// SetOnScaleDown sets callback for scale down events
func (s *DynamicScaler) SetOnScaleDown(fn func(oldCount, newCount int)) {
	s.onScaleDown = fn
}

// Shutdown stops the dynamic scaler
func (s *DynamicScaler) Shutdown() {
	close(s.shutdown)
}

// GracefulShutdownManager manages graceful shutdown of the pool
type GracefulShutdownManager struct {
	pool            *Pool
	timeout         time.Duration
	drainTasks      bool
	logger          logger.Logger
	onShutdownStart func()
	onShutdownEnd   func()
}

// NewGracefulShutdownManager creates a new graceful shutdown manager
func NewGracefulShutdownManager(pool *Pool, timeout time.Duration, drainTasks bool, log logger.Logger) *GracefulShutdownManager {
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &GracefulShutdownManager{
		pool:       pool,
		timeout:    timeout,
		drainTasks: drainTasks,
		logger:     log,
	}
}

// Shutdown performs a graceful shutdown
func (g *GracefulShutdownManager) Shutdown(ctx context.Context) error {
	if g.onShutdownStart != nil {
		g.onShutdownStart()
	}

	g.logger.Info("Starting graceful shutdown",
		logger.Field{Key: "timeout", Value: g.timeout},
		logger.Field{Key: "drainTasks", Value: g.drainTasks})

	// Stop accepting new tasks
	g.pool.gracefulShutdown = true

	if g.drainTasks {
		// Wait for existing tasks to complete
		g.logger.Info("Draining existing tasks")

		done := make(chan struct{})
		go func() {
			g.pool.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			g.logger.Info("All tasks completed successfully")
		case <-time.After(g.timeout):
			g.logger.Warn("Graceful shutdown timeout exceeded, forcing shutdown")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Stop the pool
	close(g.pool.stop)
	g.pool.wg.Wait()

	if g.onShutdownEnd != nil {
		g.onShutdownEnd()
	}

	g.logger.Info("Graceful shutdown complete")

	return nil
}

// SetOnShutdownStart sets callback for shutdown start
func (g *GracefulShutdownManager) SetOnShutdownStart(fn func()) {
	g.onShutdownStart = fn
}

// SetOnShutdownEnd sets callback for shutdown end
func (g *GracefulShutdownManager) SetOnShutdownEnd(fn func()) {
	g.onShutdownEnd = fn
}

// PoolEnhancedStats returns enhanced statistics about the pool
func PoolEnhancedStats(pool *Pool) map[string]interface{} {
	pool.taskQueueLock.Lock()
	queueLen := len(pool.taskQueue)
	queueCap := cap(pool.taskQueue)
	pool.taskQueueLock.Unlock()

	pool.overflowBufferLock.RLock()
	overflowLen := len(pool.overflowBuffer)
	pool.overflowBufferLock.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return map[string]interface{}{
		"workers": map[string]interface{}{
			"count":  atomic.LoadInt32(&pool.numOfWorkers),
			"paused": pool.paused,
		},
		"queue": map[string]interface{}{
			"length":      queueLen,
			"capacity":    queueCap,
			"utilization": float64(queueLen) / float64(queueCap) * 100,
		},
		"overflow": map[string]interface{}{
			"length": overflowLen,
		},
		"tasks": map[string]interface{}{
			"total":     atomic.LoadInt64(&pool.metrics.TotalTasks),
			"completed": atomic.LoadInt64(&pool.metrics.CompletedTasks),
			"errors":    atomic.LoadInt64(&pool.metrics.ErrorCount),
		},
		"memory": map[string]interface{}{
			"alloc":       memStats.Alloc,
			"total_alloc": memStats.TotalAlloc,
			"sys":         memStats.Sys,
			"num_gc":      memStats.NumGC,
		},
		"dlq": map[string]interface{}{
			"size": pool.dlq.Size(),
		},
	}
}
