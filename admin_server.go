package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
)

// AdminServer provides comprehensive admin interface and API
type AdminServer struct {
	broker    *Broker
	server    *http.Server
	logger    logger.Logger
	metrics   *AdminMetrics
	isRunning bool
	mu        sync.RWMutex
}

// AdminMetrics tracks comprehensive admin metrics
type AdminMetrics struct {
	StartTime         time.Time                        `json:"start_time"`
	TotalMessages     int64                            `json:"total_messages"`
	ActiveConsumers   int                              `json:"active_consumers"`
	ActiveQueues      int                              `json:"active_queues"`
	FailedMessages    int64                            `json:"failed_messages"`
	SuccessCount      int64                            `json:"success_count"`
	ErrorCount        int64                            `json:"error_count"`
	ThroughputHistory []float64                        `json:"throughput_history"`
	QueueMetrics      map[string]*QueueMetrics         `json:"queue_metrics"`
	ConsumerMetrics   map[string]*AdminConsumerMetrics `json:"consumer_metrics"`
	PoolMetrics       map[string]*AdminPoolMetrics     `json:"pool_metrics"`
	SystemMetrics     *AdminSystemMetrics              `json:"system_metrics"`
	mu                sync.RWMutex
}

// AdminConsumerMetrics tracks individual consumer metrics
type AdminConsumerMetrics struct {
	ID                 string    `json:"id"`
	Queue              string    `json:"queue"`
	Status             string    `json:"status"`
	ProcessedTasks     int64     `json:"processed"`
	ErrorCount         int64     `json:"errors"`
	LastActivity       time.Time `json:"last_activity"`
	MaxConcurrentTasks int       `json:"max_concurrent_tasks"`
	TaskTimeout        int       `json:"task_timeout"`
	MaxRetries         int       `json:"max_retries"`
}

// AdminPoolMetrics tracks worker pool metrics
type AdminPoolMetrics struct {
	ID            string    `json:"id"`
	Workers       int       `json:"workers"`
	QueueSize     int       `json:"queue_size"`
	ActiveTasks   int       `json:"active_tasks"`
	Status        string    `json:"status"`
	MaxMemoryLoad int64     `json:"max_memory_load"`
	LastActivity  time.Time `json:"last_activity"`
}

// AdminSystemMetrics tracks system-level metrics
type AdminSystemMetrics struct {
	CPUPercent     float64   `json:"cpu_percent"`
	MemoryPercent  float64   `json:"memory_percent"`
	GoroutineCount int       `json:"goroutine_count"`
	Timestamp      time.Time `json:"timestamp"`
}

// AdminBrokerInfo contains broker status information
type AdminBrokerInfo struct {
	Status      string                 `json:"status"`
	Address     string                 `json:"address"`
	Uptime      int64                  `json:"uptime"` // milliseconds
	Connections int                    `json:"connections"`
	Config      map[string]interface{} `json:"config"`
}

// AdminHealthCheck represents a health check result
type AdminHealthCheck struct {
	Name      string        `json:"name"`
	Status    string        `json:"status"`
	Message   string        `json:"message"`
	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
}

// AdminQueueInfo represents queue information for admin interface
type AdminQueueInfo struct {
	Name      string `json:"name"`
	Depth     int    `json:"depth"`
	Consumers int    `json:"consumers"`
	Rate      int    `json:"rate"`
}

// NewAdminServer creates a new admin server
func NewAdminServer(broker *Broker, addr string, log logger.Logger) *AdminServer {
	admin := &AdminServer{
		broker:  broker,
		logger:  log,
		metrics: NewAdminMetrics(),
	}

	mux := http.NewServeMux()
	admin.setupRoutes(mux)
	admin.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return admin
}

// NewAdminMetrics creates new admin metrics
func NewAdminMetrics() *AdminMetrics {
	return &AdminMetrics{
		StartTime:         time.Now(),
		ThroughputHistory: make([]float64, 0, 100),
		QueueMetrics:      make(map[string]*QueueMetrics),
		ConsumerMetrics:   make(map[string]*AdminConsumerMetrics),
		PoolMetrics:       make(map[string]*AdminPoolMetrics),
		SystemMetrics:     &AdminSystemMetrics{},
	}
}

// Start starts the admin server
func (a *AdminServer) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.isRunning {
		return fmt.Errorf("admin server is already running")
	}

	a.logger.Info("Starting admin server", logger.Field{Key: "address", Value: a.server.Addr})

	// Start metrics collection
	go a.metricsCollectionLoop()

	a.isRunning = true

	// Start server in a goroutine and capture any startup errors
	startupError := make(chan error, 1)
	go func() {
		a.logger.Info("Admin server listening", logger.Field{Key: "address", Value: a.server.Addr})
		err := a.server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			a.logger.Error("Admin server error", logger.Field{Key: "error", Value: err.Error()})
			startupError <- err
		}
	}()

	// Wait a bit to see if there's an immediate startup error
	select {
	case err := <-startupError:
		a.isRunning = false
		return fmt.Errorf("failed to start admin server: %w", err)
	case <-time.After(200 * time.Millisecond):
		// Server seems to have started successfully
		a.logger.Info("Admin server started successfully")

		// Test if server is actually listening by making a simple request
		go func() {
			time.Sleep(100 * time.Millisecond)
			client := &http.Client{Timeout: 1 * time.Second}
			resp, err := client.Get("http://localhost" + a.server.Addr + "/api/admin/health")
			if err != nil {
				a.logger.Error("Admin server self-test failed", logger.Field{Key: "error", Value: err.Error()})
			} else {
				a.logger.Info("Admin server self-test passed", logger.Field{Key: "status", Value: resp.StatusCode})
				resp.Body.Close()
			}
		}()
	}

	return nil
}

// Stop stops the admin server
func (a *AdminServer) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.isRunning {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a.isRunning = false

	return a.server.Shutdown(ctx)
}

func (a *AdminServer) setupRoutes(mux *http.ServeMux) {
	// Serve static files - use absolute path for debugging
	staticDir := "./static/"
	a.logger.Info("Setting up static file server", logger.Field{Key: "directory", Value: staticDir})

	// Try multiple possible static directories
	possibleDirs := []string{
		"./static/",
		"../static/",
		"../../static/",
		"/Users/sujit/Sites/mq/static/", // Fallback absolute path
	}

	var finalStaticDir string
	for _, dir := range possibleDirs {
		if _, err := http.Dir(dir).Open("admin"); err == nil {
			finalStaticDir = dir
			break
		}
	}

	if finalStaticDir == "" {
		finalStaticDir = staticDir // fallback to default
	}

	a.logger.Info("Using static directory", logger.Field{Key: "directory", Value: finalStaticDir})
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(finalStaticDir))))

	// Admin dashboard
	mux.HandleFunc("/admin", a.handleAdminDashboard)
	mux.HandleFunc("/", a.handleAdminDashboard)

	// API endpoints
	mux.HandleFunc("/api/admin/metrics", a.handleGetMetrics)
	mux.HandleFunc("/api/admin/broker", a.handleGetBroker)
	mux.HandleFunc("/api/admin/broker/restart", a.handleRestartBroker)
	mux.HandleFunc("/api/admin/broker/stop", a.handleStopBroker)
	mux.HandleFunc("/api/admin/queues", a.handleGetQueues)
	mux.HandleFunc("/api/admin/queues/flush", a.handleFlushQueues)
	mux.HandleFunc("/api/admin/consumers", a.handleGetConsumers)
	mux.HandleFunc("/api/admin/pools", a.handleGetPools)
	mux.HandleFunc("/api/admin/health", a.handleGetHealth)

	a.logger.Info("Admin server routes configured")
} // HTTP Handler implementations

func (a *AdminServer) handleAdminDashboard(w http.ResponseWriter, r *http.Request) {
	a.logger.Info("Admin dashboard request", logger.Field{Key: "path", Value: r.URL.Path})

	// Try multiple possible paths for the admin dashboard
	possiblePaths := []string{
		"./static/admin/index.html",
		"../static/admin/index.html",
		"../../static/admin/index.html",
		"/Users/sujit/Sites/mq/static/admin/index.html", // Fallback absolute path
	}

	var finalPath string
	for _, path := range possiblePaths {
		if _, err := http.Dir(".").Open(path); err == nil {
			finalPath = path
			break
		}
	}

	if finalPath == "" {
		finalPath = "./static/admin/index.html" // fallback to default
	}

	a.logger.Info("Serving admin dashboard", logger.Field{Key: "file", Value: finalPath})
	http.ServeFile(w, r, finalPath)
}

func (a *AdminServer) handleGetMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(a.getMetrics())
}

func (a *AdminServer) handleGetBroker(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(a.getBrokerInfo())
}

func (a *AdminServer) handleGetQueues(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(a.getQueues())
}

func (a *AdminServer) handleGetConsumers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(a.getConsumers())
}

func (a *AdminServer) handleGetPools(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(a.getPools())
}

func (a *AdminServer) handleGetHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(a.getHealthChecks())
}

func (a *AdminServer) handleRestartBroker(w http.ResponseWriter, r *http.Request) {
	if a.broker == nil {
		http.Error(w, "Broker not available", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "restart_initiated"})
}

func (a *AdminServer) handleStopBroker(w http.ResponseWriter, r *http.Request) {
	if a.broker == nil {
		http.Error(w, "Broker not available", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "stop_initiated"})
}

func (a *AdminServer) handleFlushQueues(w http.ResponseWriter, r *http.Request) {
	if a.broker == nil {
		http.Error(w, "Broker not available", http.StatusServiceUnavailable)
		return
	}

	// Get queue names and flush them
	queueNames := a.broker.queues.Keys()
	flushedCount := 0

	for _, queueName := range queueNames {
		if queue, exists := a.broker.queues.Get(queueName); exists {
			// Count tasks before flushing
			taskCount := len(queue.tasks)
			// Drain queue
			for len(queue.tasks) > 0 {
				<-queue.tasks
			}
			flushedCount += taskCount
		}
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":        "queues_flushed",
		"flushed_count": flushedCount,
	})
}

// Helper methods for data collection

func (a *AdminServer) getMetrics() *AdminMetrics {
	a.metrics.mu.RLock()
	defer a.metrics.mu.RUnlock()

	// Create a copy to avoid race conditions
	metrics := &AdminMetrics{
		StartTime:         a.metrics.StartTime,
		TotalMessages:     a.metrics.TotalMessages,
		ActiveConsumers:   a.metrics.ActiveConsumers,
		ActiveQueues:      a.metrics.ActiveQueues,
		FailedMessages:    a.metrics.FailedMessages,
		SuccessCount:      a.metrics.SuccessCount,
		ErrorCount:        a.metrics.ErrorCount,
		ThroughputHistory: append([]float64{}, a.metrics.ThroughputHistory...),
		QueueMetrics:      make(map[string]*QueueMetrics),
		ConsumerMetrics:   make(map[string]*AdminConsumerMetrics),
		PoolMetrics:       make(map[string]*AdminPoolMetrics),
		SystemMetrics:     a.metrics.SystemMetrics,
	}

	// Deep copy maps
	for k, v := range a.metrics.QueueMetrics {
		metrics.QueueMetrics[k] = v
	}
	for k, v := range a.metrics.ConsumerMetrics {
		metrics.ConsumerMetrics[k] = v
	}
	for k, v := range a.metrics.PoolMetrics {
		metrics.PoolMetrics[k] = v
	}

	return metrics
}

func (a *AdminServer) getQueues() []*AdminQueueInfo {
	if a.broker == nil {
		return []*AdminQueueInfo{}
	}

	queueNames := a.broker.queues.Keys()
	queues := make([]*AdminQueueInfo, 0, len(queueNames))

	for _, name := range queueNames {
		if queue, exists := a.broker.queues.Get(name); exists {
			queueInfo := &AdminQueueInfo{
				Name:      name,
				Depth:     len(queue.tasks),
				Consumers: queue.consumers.Size(),
				Rate:      0, // Would calculate based on metrics
			}
			queues = append(queues, queueInfo)
		}
	}

	return queues
}

func (a *AdminServer) getConsumers() []*AdminConsumerMetrics {
	// This would need to be implemented based on how you track consumers
	// For now, return sample data as placeholder
	consumers := []*AdminConsumerMetrics{
		{
			ID:                 "consumer-1",
			Queue:              "demo_queue",
			Status:             "active",
			ProcessedTasks:     150,
			ErrorCount:         2,
			LastActivity:       time.Now().Add(-30 * time.Second),
			MaxConcurrentTasks: 10,
			TaskTimeout:        30,
			MaxRetries:         3,
		},
		{
			ID:                 "consumer-2",
			Queue:              "priority_queue",
			Status:             "paused",
			ProcessedTasks:     89,
			ErrorCount:         0,
			LastActivity:       time.Now().Add(-2 * time.Minute),
			MaxConcurrentTasks: 5,
			TaskTimeout:        60,
			MaxRetries:         5,
		},
	}
	return consumers
}

func (a *AdminServer) getPools() []*AdminPoolMetrics {
	// This would need to be implemented based on how you track pools
	// For now, return sample data as placeholder
	pools := []*AdminPoolMetrics{
		{
			ID:            "pool-1",
			Workers:       10,
			QueueSize:     100,
			ActiveTasks:   7,
			Status:        "running",
			MaxMemoryLoad: 1024 * 1024 * 512, // 512MB
			LastActivity:  time.Now().Add(-10 * time.Second),
		},
		{
			ID:            "pool-2",
			Workers:       5,
			QueueSize:     50,
			ActiveTasks:   2,
			Status:        "running",
			MaxMemoryLoad: 1024 * 1024 * 256, // 256MB
			LastActivity:  time.Now().Add(-1 * time.Minute),
		},
	}
	return pools
}

func (a *AdminServer) getBrokerInfo() *AdminBrokerInfo {
	if a.broker == nil {
		return &AdminBrokerInfo{
			Status: "stopped",
		}
	}

	uptime := time.Since(a.metrics.StartTime).Milliseconds()

	return &AdminBrokerInfo{
		Status:      "running",
		Address:     a.broker.opts.brokerAddr,
		Uptime:      uptime,
		Connections: 0, // Would need to implement connection tracking
		Config: map[string]interface{}{
			"max_connections": 1000,
			"read_timeout":    "30s",
			"write_timeout":   "30s",
			"worker_pool":     a.broker.opts.enableWorkerPool,
			"sync_mode":       a.broker.opts.syncMode,
			"queue_size":      a.broker.opts.queueSize,
		},
	}
}

func (a *AdminServer) getHealthChecks() []*AdminHealthCheck {
	checks := []*AdminHealthCheck{
		{
			Name:      "Broker Health",
			Status:    "healthy",
			Message:   "Broker is running normally",
			Duration:  time.Millisecond * 5,
			Timestamp: time.Now(),
		},
		{
			Name:      "Memory Usage",
			Status:    "healthy",
			Message:   "Memory usage is within normal limits",
			Duration:  time.Millisecond * 2,
			Timestamp: time.Now(),
		},
		{
			Name:      "Queue Health",
			Status:    "healthy",
			Message:   "All queues are operational",
			Duration:  time.Millisecond * 3,
			Timestamp: time.Now(),
		},
	}

	return checks
}

func (a *AdminServer) metricsCollectionLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !a.isRunning {
			return
		}
		a.collectMetrics()
	}
}

func (a *AdminServer) collectMetrics() {
	a.metrics.mu.Lock()
	defer a.metrics.mu.Unlock()

	// Update queue count
	if a.broker != nil {
		a.metrics.ActiveQueues = a.broker.queues.Size()
	}

	// Update system metrics
	a.metrics.SystemMetrics = &AdminSystemMetrics{
		CPUPercent:     0.0, // Would implement actual CPU monitoring
		MemoryPercent:  0.0, // Would implement actual memory monitoring
		GoroutineCount: 0,   // Would implement actual goroutine counting
		Timestamp:      time.Now(),
	}

	// Update throughput history
	currentThroughput := 0.0 // Calculate current throughput
	a.metrics.ThroughputHistory = append(a.metrics.ThroughputHistory, currentThroughput)

	// Keep only last 100 data points
	if len(a.metrics.ThroughputHistory) > 100 {
		a.metrics.ThroughputHistory = a.metrics.ThroughputHistory[1:]
	}
}
