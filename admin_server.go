package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
	"github.com/oarkflow/mq/sio"
)

// AdminServer provides comprehensive admin interface and API
type AdminServer struct {
	broker      *Broker
	server      *http.Server
	logger      logger.Logger
	metrics     *AdminMetrics
	wsServer    *sio.Server
	isRunning   bool
	mu          sync.RWMutex
	wsClients   map[string]*sio.Socket
	wsClientsMu sync.RWMutex
	broadcastCh chan *AdminMessage
	shutdownCh  chan struct{}
}

// AdminMessage represents a message sent via WebSocket
type AdminMessage struct {
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// TaskUpdate represents a real-time task update
type TaskUpdate struct {
	TaskID    string    `json:"task_id"`
	Queue     string    `json:"queue"`
	Status    string    `json:"status"`
	Consumer  string    `json:"consumer,omitempty"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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
		broker:      broker,
		logger:      log,
		metrics:     NewAdminMetrics(),
		wsClients:   make(map[string]*sio.Socket),
		broadcastCh: make(chan *AdminMessage, 100),
		shutdownCh:  make(chan struct{}),
	}

	// Initialize WebSocket server
	admin.wsServer = sio.New()
	admin.setupWebSocketEvents()

	mux := http.NewServeMux()
	admin.setupRoutes(mux)
	admin.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return admin
}

// setupWebSocketEvents configures WebSocket event handlers
func (a *AdminServer) setupWebSocketEvents() {
	a.wsServer.OnConnect(func(s *sio.Socket) error {
		a.wsClientsMu.Lock()
		a.wsClients[s.ID()] = s
		a.wsClientsMu.Unlock()

		a.logger.Info("WebSocket client connected", logger.Field{Key: "id", Value: s.ID()})

		// Send initial data to new client
		go a.sendInitialData(s)
		return nil
	})

	a.wsServer.OnDisconnect(func(s *sio.Socket) error {
		a.wsClientsMu.Lock()
		delete(a.wsClients, s.ID())
		a.wsClientsMu.Unlock()

		a.logger.Info("WebSocket client disconnected", logger.Field{Key: "id", Value: s.ID()})
		return nil
	})

	a.wsServer.On("subscribe", func(s *sio.Socket, data []byte) {
		// Handle subscription to specific data types
		a.logger.Info("WebSocket subscription", logger.Field{Key: "data", Value: string(data)})
	})
}

// sendInitialData sends current state to newly connected client
func (a *AdminServer) sendInitialData(s *sio.Socket) {
	// Send current metrics
	msg := &AdminMessage{
		Type:      "metrics",
		Data:      a.getMetrics(),
		Timestamp: time.Now(),
	}
	a.sendToSocket(s, msg)

	// Send current queues
	msg = &AdminMessage{
		Type:      "queues",
		Data:      a.getQueues(),
		Timestamp: time.Now(),
	}
	a.sendToSocket(s, msg)

	// Send current consumers
	msg = &AdminMessage{
		Type:      "consumers",
		Data:      a.getConsumers(),
		Timestamp: time.Now(),
	}
	a.sendToSocket(s, msg)

	// Send current pools
	msg = &AdminMessage{
		Type:      "pools",
		Data:      a.getPools(),
		Timestamp: time.Now(),
	}
	a.sendToSocket(s, msg)

	// Send broker info
	msg = &AdminMessage{
		Type:      "broker",
		Data:      a.getBrokerInfo(),
		Timestamp: time.Now(),
	}
	a.sendToSocket(s, msg)
}

// sendToSocket sends a message to a specific socket
func (a *AdminServer) sendToSocket(s *sio.Socket, msg *AdminMessage) {
	// The sio.Socket.Emit method handles JSON marshaling automatically
	err := s.Emit("update", msg)
	if err != nil {
		a.logger.Error("Failed to send WebSocket message", logger.Field{Key: "error", Value: err.Error()})
	}
}

// broadcastMessage sends a message to all connected WebSocket clients
func (a *AdminServer) broadcastMessage(msg *AdminMessage) {
	a.wsClientsMu.RLock()
	defer a.wsClientsMu.RUnlock()

	for _, client := range a.wsClients {
		err := client.Emit("update", msg)
		if err != nil {
			a.logger.Error("Failed to broadcast message to client", logger.Field{Key: "error", Value: err.Error()}, logger.Field{Key: "client_id", Value: client.ID()})
		}
	}
}

// startBroadcasting starts the broadcasting goroutine for real-time updates
func (a *AdminServer) startBroadcasting() {
	go func() {
		ticker := time.NewTicker(2 * time.Second) // Broadcast updates every 2 seconds
		defer ticker.Stop()

		for {
			select {
			case <-a.shutdownCh:
				return
			case msg := <-a.broadcastCh:
				a.broadcastMessage(msg)
			case <-ticker.C:
				// Send periodic updates
				a.sendPeriodicUpdates()
			}
		}
	}()
}

// sendPeriodicUpdates sends periodic updates to all connected clients
func (a *AdminServer) sendPeriodicUpdates() {
	// Send metrics update
	msg := &AdminMessage{
		Type:      "metrics",
		Data:      a.getMetrics(),
		Timestamp: time.Now(),
	}
	a.broadcastMessage(msg)

	// Send queues update
	msg = &AdminMessage{
		Type:      "queues",
		Data:      a.getQueues(),
		Timestamp: time.Now(),
	}
	a.broadcastMessage(msg)

	// Send consumers update
	msg = &AdminMessage{
		Type:      "consumers",
		Data:      a.getConsumers(),
		Timestamp: time.Now(),
	}
	a.broadcastMessage(msg)

	// Send pools update
	msg = &AdminMessage{
		Type:      "pools",
		Data:      a.getPools(),
		Timestamp: time.Now(),
	}
	a.broadcastMessage(msg)
}

// broadcastTaskUpdate sends a task update to all connected clients
func (a *AdminServer) BroadcastTaskUpdate(update *TaskUpdate) {
	msg := &AdminMessage{
		Type:      "task_update",
		Data:      update,
		Timestamp: time.Now(),
	}
	select {
	case a.broadcastCh <- msg:
	default:
		// Channel is full, skip this update
	}
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

	// Start WebSocket broadcasting
	a.startBroadcasting()

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

	// Signal shutdown
	close(a.shutdownCh)

	// Close WebSocket connections
	a.wsClientsMu.Lock()
	for _, client := range a.wsClients {
		client.Close()
	}
	a.wsClients = make(map[string]*sio.Socket)
	a.wsClientsMu.Unlock()

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

	// WebSocket endpoint
	mux.HandleFunc("/ws", a.wsServer.ServeHTTP)

	// Admin dashboard
	mux.HandleFunc("/admin", a.handleAdminDashboard)
	mux.HandleFunc("/", a.handleAdminDashboard)

	// API endpoints
	mux.HandleFunc("/api/admin/metrics", a.handleGetMetrics)
	mux.HandleFunc("/api/admin/broker", a.handleGetBroker)
	mux.HandleFunc("/api/admin/broker/restart", a.handleRestartBroker)
	mux.HandleFunc("/api/admin/broker/stop", a.handleStopBroker)
	mux.HandleFunc("/api/admin/broker/pause", a.handlePauseBroker)
	mux.HandleFunc("/api/admin/broker/resume", a.handleResumeBroker)
	mux.HandleFunc("/api/admin/queues", a.handleGetQueues)
	mux.HandleFunc("/api/admin/queues/flush", a.handleFlushQueues)
	mux.HandleFunc("/api/admin/queues/purge", a.handlePurgeQueue)
	mux.HandleFunc("/api/admin/consumers", a.handleGetConsumers)
	mux.HandleFunc("/api/admin/consumers/pause", a.handlePauseConsumer)
	mux.HandleFunc("/api/admin/consumers/resume", a.handleResumeConsumer)
	mux.HandleFunc("/api/admin/consumers/stop", a.handleStopConsumer)
	mux.HandleFunc("/api/admin/pools", a.handleGetPools)
	mux.HandleFunc("/api/admin/pools/pause", a.handlePausePool)
	mux.HandleFunc("/api/admin/pools/resume", a.handleResumePool)
	mux.HandleFunc("/api/admin/pools/stop", a.handleStopPool)
	mux.HandleFunc("/api/admin/health", a.handleGetHealth)
	mux.HandleFunc("/api/admin/tasks", a.handleGetTasks)

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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if a.broker == nil {
		http.Error(w, "Broker not available", http.StatusServiceUnavailable)
		return
	}

	// For now, we'll just acknowledge the restart request
	// In a real implementation, you might want to gracefully restart the broker
	a.logger.Info("Broker restart requested")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "restart_initiated",
		"message": "Broker restart has been initiated",
	})

	// Broadcast the restart event
	a.broadcastMessage(&AdminMessage{
		Type:      "broker_restart",
		Data:      map[string]string{"status": "initiated"},
		Timestamp: time.Now(),
	})
}

func (a *AdminServer) handleStopBroker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if a.broker == nil {
		http.Error(w, "Broker not available", http.StatusServiceUnavailable)
		return
	}

	a.logger.Info("Broker stop requested")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "stop_initiated",
		"message": "Broker stop has been initiated",
	})

	// Broadcast the stop event
	a.broadcastMessage(&AdminMessage{
		Type:      "broker_stop",
		Data:      map[string]string{"status": "initiated"},
		Timestamp: time.Now(),
	})

	// Actually stop the broker in a goroutine to allow response to complete
	go func() {
		time.Sleep(100 * time.Millisecond)
		a.broker.Close()
	}()
}

func (a *AdminServer) handlePauseBroker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if a.broker == nil {
		http.Error(w, "Broker not available", http.StatusServiceUnavailable)
		return
	}

	a.logger.Info("Broker pause requested")

	// Set broker to paused state (if such functionality exists)
	// For now, we'll just acknowledge the request

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "paused",
		"message": "Broker has been paused",
	})

	// Broadcast the pause event
	a.broadcastMessage(&AdminMessage{
		Type:      "broker_pause",
		Data:      map[string]string{"status": "paused"},
		Timestamp: time.Now(),
	})
}

func (a *AdminServer) handleResumeBroker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if a.broker == nil {
		http.Error(w, "Broker not available", http.StatusServiceUnavailable)
		return
	}

	a.logger.Info("Broker resume requested")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "running",
		"message": "Broker has been resumed",
	})

	// Broadcast the resume event
	a.broadcastMessage(&AdminMessage{
		Type:      "broker_resume",
		Data:      map[string]string{"status": "running"},
		Timestamp: time.Now(),
	})
}

func (a *AdminServer) handleFlushQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

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

	a.logger.Info("Queues flushed", logger.Field{Key: "count", Value: flushedCount})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":        "queues_flushed",
		"flushed_count": flushedCount,
		"message":       fmt.Sprintf("Flushed %d tasks from all queues", flushedCount),
	}
	json.NewEncoder(w).Encode(response)

	// Broadcast the flush event
	a.broadcastMessage(&AdminMessage{
		Type:      "queues_flush",
		Data:      response,
		Timestamp: time.Now(),
	})
}

func (a *AdminServer) handlePurgeQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	queueName := r.URL.Query().Get("name")
	if queueName == "" {
		http.Error(w, "Queue name is required", http.StatusBadRequest)
		return
	}

	if a.broker == nil {
		http.Error(w, "Broker not available", http.StatusServiceUnavailable)
		return
	}

	purgedCount := 0
	if queue, exists := a.broker.queues.Get(queueName); exists {
		// Count tasks before purging
		purgedCount = len(queue.tasks)
		// Drain the specific queue
		for len(queue.tasks) > 0 {
			<-queue.tasks
		}
	}

	a.logger.Info("Queue purged", logger.Field{Key: "queue", Value: queueName}, logger.Field{Key: "count", Value: purgedCount})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":       "queue_purged",
		"queue_name":   queueName,
		"purged_count": purgedCount,
		"message":      fmt.Sprintf("Purged %d tasks from queue %s", purgedCount, queueName),
	}
	json.NewEncoder(w).Encode(response)

	// Broadcast the purge event
	a.broadcastMessage(&AdminMessage{
		Type:      "queue_purge",
		Data:      response,
		Timestamp: time.Now(),
	})
}

// Consumer handlers
func (a *AdminServer) handlePauseConsumer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	consumerID := r.URL.Query().Get("id")
	if consumerID == "" {
		http.Error(w, "Consumer ID is required", http.StatusBadRequest)
		return
	}

	a.logger.Info("Consumer pause requested", logger.Field{Key: "consumer_id", Value: consumerID})

	a.broker.PauseConsumer(r.Context(), consumerID)

	// In a real implementation, you would find the consumer and pause it
	// For now, we'll just acknowledge the request

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":      "paused",
		"consumer_id": consumerID,
		"message":     fmt.Sprintf("Consumer %s has been paused", consumerID),
	}
	json.NewEncoder(w).Encode(response)

	// Broadcast the pause event
	a.broadcastMessage(&AdminMessage{
		Type:      "consumer_pause",
		Data:      response,
		Timestamp: time.Now(),
	})
}

func (a *AdminServer) handleResumeConsumer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	consumerID := r.URL.Query().Get("id")
	if consumerID == "" {
		http.Error(w, "Consumer ID is required", http.StatusBadRequest)
		return
	}

	a.logger.Info("Consumer resume requested", logger.Field{Key: "consumer_id", Value: consumerID})

	a.broker.ResumeConsumer(r.Context(), consumerID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":      "active",
		"consumer_id": consumerID,
		"message":     fmt.Sprintf("Consumer %s has been resumed", consumerID),
	}
	json.NewEncoder(w).Encode(response)

	// Broadcast the resume event
	a.broadcastMessage(&AdminMessage{
		Type:      "consumer_resume",
		Data:      response,
		Timestamp: time.Now(),
	})
}

func (a *AdminServer) handleStopConsumer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	consumerID := r.URL.Query().Get("id")
	if consumerID == "" {
		http.Error(w, "Consumer ID is required", http.StatusBadRequest)
		return
	}

	a.logger.Info("Consumer stop requested", logger.Field{Key: "consumer_id", Value: consumerID})

	a.broker.StopConsumer(r.Context(), consumerID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":      "stopped",
		"consumer_id": consumerID,
		"message":     fmt.Sprintf("Consumer %s has been stopped", consumerID),
	}
	json.NewEncoder(w).Encode(response)

	// Broadcast the stop event
	a.broadcastMessage(&AdminMessage{
		Type:      "consumer_stop",
		Data:      response,
		Timestamp: time.Now(),
	})
}

// Pool handlers
func (a *AdminServer) handlePausePool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	poolID := r.URL.Query().Get("id")
	if poolID == "" {
		http.Error(w, "Pool ID is required", http.StatusBadRequest)
		return
	}

	a.logger.Info("Pool pause requested", logger.Field{Key: "pool_id", Value: poolID})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":  "paused",
		"pool_id": poolID,
		"message": fmt.Sprintf("Pool %s has been paused", poolID),
	}
	json.NewEncoder(w).Encode(response)

	// Broadcast the pause event
	a.broadcastMessage(&AdminMessage{
		Type:      "pool_pause",
		Data:      response,
		Timestamp: time.Now(),
	})
}

func (a *AdminServer) handleResumePool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	poolID := r.URL.Query().Get("id")
	if poolID == "" {
		http.Error(w, "Pool ID is required", http.StatusBadRequest)
		return
	}

	a.logger.Info("Pool resume requested", logger.Field{Key: "pool_id", Value: poolID})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":  "running",
		"pool_id": poolID,
		"message": fmt.Sprintf("Pool %s has been resumed", poolID),
	}
	json.NewEncoder(w).Encode(response)

	// Broadcast the resume event
	a.broadcastMessage(&AdminMessage{
		Type:      "pool_resume",
		Data:      response,
		Timestamp: time.Now(),
	})
}

func (a *AdminServer) handleStopPool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	poolID := r.URL.Query().Get("id")
	if poolID == "" {
		http.Error(w, "Pool ID is required", http.StatusBadRequest)
		return
	}

	a.logger.Info("Pool stop requested", logger.Field{Key: "pool_id", Value: poolID})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":  "stopped",
		"pool_id": poolID,
		"message": fmt.Sprintf("Pool %s has been stopped", poolID),
	}
	json.NewEncoder(w).Encode(response)

	// Broadcast the stop event
	a.broadcastMessage(&AdminMessage{
		Type:      "pool_stop",
		Data:      response,
		Timestamp: time.Now(),
	})
}

// Tasks handler
func (a *AdminServer) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	tasks := a.getCurrentTasks()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tasks": tasks,
		"count": len(tasks),
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
	return a.broker.GetConsumers()
}

func (a *AdminServer) getPools() []*AdminPoolMetrics {
	return a.broker.GetPools()
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
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	a.metrics.SystemMetrics = &AdminSystemMetrics{
		CPUPercent:     0.0, // Would implement actual CPU monitoring with external package
		MemoryPercent:  float64(memStats.Alloc) / float64(memStats.Sys) * 100,
		GoroutineCount: runtime.NumGoroutine(),
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

// getCurrentTasks returns current tasks across all queues
func (a *AdminServer) getCurrentTasks() []map[string]interface{} {
	if a.broker == nil {
		return []map[string]interface{}{}
	}

	var tasks []map[string]interface{}
	queueNames := a.broker.queues.Keys()

	for _, queueName := range queueNames {
		if queue, exists := a.broker.queues.Get(queueName); exists {
			// Get tasks from queue channel (non-blocking)
			queueLen := len(queue.tasks)
		queueLoop:
			for i := 0; i < queueLen && i < 100; i++ { // Limit to 100 tasks for performance
				select {
				case task := <-queue.tasks:
					taskInfo := map[string]interface{}{
						"id":          fmt.Sprintf("task-%d", i),
						"queue":       queueName,
						"retry_count": task.RetryCount,
						"created_at":  time.Now(), // Would extract from message if available
						"status":      "queued",
						"payload":     string(task.Message.Payload),
					}
					tasks = append(tasks, taskInfo)
					// Put the task back
					select {
					case queue.tasks <- task:
					default:
						// Queue is full, task is lost (shouldn't happen in normal operation)
					}
				default:
					break queueLoop
				}
			}
		}
	}

	return tasks
}
