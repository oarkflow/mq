package dag

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/oarkflow/mq/logger"
)

// EnhancedAPIHandler provides enhanced API endpoints for DAG management
type EnhancedAPIHandler struct {
	dag    *DAG
	logger logger.Logger
}

// NewEnhancedAPIHandler creates a new enhanced API handler
func NewEnhancedAPIHandler(dag *DAG) *EnhancedAPIHandler {
	return &EnhancedAPIHandler{
		dag:    dag,
		logger: dag.Logger(),
	}
}

// RegisterRoutes registers all enhanced API routes
func (h *EnhancedAPIHandler) RegisterRoutes(mux *http.ServeMux) {
	// Monitoring endpoints
	mux.HandleFunc("/api/dag/metrics", h.getMetrics)
	mux.HandleFunc("/api/dag/node-stats", h.getNodeStats)
	mux.HandleFunc("/api/dag/health", h.getHealth)

	// Management endpoints
	mux.HandleFunc("/api/dag/validate", h.validateDAG)
	mux.HandleFunc("/api/dag/topology", h.getTopology)
	mux.HandleFunc("/api/dag/critical-path", h.getCriticalPath)
	mux.HandleFunc("/api/dag/statistics", h.getStatistics)

	// Configuration endpoints
	mux.HandleFunc("/api/dag/config", h.handleConfig)
	mux.HandleFunc("/api/dag/rate-limit", h.handleRateLimit)
	mux.HandleFunc("/api/dag/retry-config", h.handleRetryConfig)

	// Transaction endpoints
	mux.HandleFunc("/api/dag/transaction", h.handleTransaction)

	// Performance endpoints
	mux.HandleFunc("/api/dag/optimize", h.optimizePerformance)
	mux.HandleFunc("/api/dag/circuit-breaker", h.getCircuitBreakerStatus)

	// Cache endpoints
	mux.HandleFunc("/api/dag/cache/clear", h.clearCache)
	mux.HandleFunc("/api/dag/cache/stats", h.getCacheStats)
}

// getMetrics returns monitoring metrics
func (h *EnhancedAPIHandler) getMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metrics := h.dag.GetMonitoringMetrics()
	if metrics == nil {
		http.Error(w, "Monitoring not enabled", http.StatusServiceUnavailable)
		return
	}

	h.respondJSON(w, metrics)
}

// getNodeStats returns statistics for a specific node or all nodes
func (h *EnhancedAPIHandler) getNodeStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("nodeId")

	if nodeID != "" {
		stats := h.dag.GetNodeStats(nodeID)
		if stats == nil {
			http.Error(w, "Node not found or monitoring not enabled", http.StatusNotFound)
			return
		}
		h.respondJSON(w, stats)
	} else {
		// Return stats for all nodes
		allStats := make(map[string]*NodeStats)
		h.dag.nodes.ForEach(func(id string, _ *Node) bool {
			if stats := h.dag.GetNodeStats(id); stats != nil {
				allStats[id] = stats
			}
			return true
		})
		h.respondJSON(w, allStats)
	}
}

// getHealth returns DAG health status
func (h *EnhancedAPIHandler) getHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"uptime":    time.Since(h.dag.monitor.metrics.StartTime),
	}

	metrics := h.dag.GetMonitoringMetrics()
	if metrics != nil {
		// Check if failure rate is too high
		if metrics.TasksTotal > 0 {
			failureRate := float64(metrics.TasksFailed) / float64(metrics.TasksTotal)
			if failureRate > 0.1 { // 10% failure rate threshold
				health["status"] = "degraded"
				health["reason"] = fmt.Sprintf("High failure rate: %.2f%%", failureRate*100)
			}
		}

		// Check if too many tasks are in progress
		if metrics.TasksInProgress > 1000 {
			health["status"] = "warning"
			health["reason"] = fmt.Sprintf("High task load: %d tasks in progress", metrics.TasksInProgress)
		}

		health["metrics"] = map[string]interface{}{
			"total_tasks":       metrics.TasksTotal,
			"completed_tasks":   metrics.TasksCompleted,
			"failed_tasks":      metrics.TasksFailed,
			"tasks_in_progress": metrics.TasksInProgress,
		}
	}

	h.respondJSON(w, health)
}

// validateDAG validates the DAG structure
func (h *EnhancedAPIHandler) validateDAG(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := h.dag.ValidateDAG()
	response := map[string]interface{}{
		"valid":     err == nil,
		"timestamp": time.Now(),
	}

	if err != nil {
		response["error"] = err.Error()
		w.WriteHeader(http.StatusBadRequest)
	}

	h.respondJSON(w, response)
}

// getTopology returns the topological order of nodes
func (h *EnhancedAPIHandler) getTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topology, err := h.dag.GetTopologicalOrder()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.respondJSON(w, map[string]interface{}{
		"topology": topology,
		"count":    len(topology),
	})
}

// getCriticalPath returns the critical path of the DAG
func (h *EnhancedAPIHandler) getCriticalPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path, err := h.dag.GetCriticalPath()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.respondJSON(w, map[string]interface{}{
		"critical_path": path,
		"length":        len(path),
	})
}

// getStatistics returns DAG statistics
func (h *EnhancedAPIHandler) getStatistics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := h.dag.GetDAGStatistics()
	h.respondJSON(w, stats)
}

// handleConfig handles DAG configuration operations
func (h *EnhancedAPIHandler) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		config := h.dag.GetConfiguration()
		h.respondJSON(w, config)

	case http.MethodPut:
		var config DAGConfig
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		if err := h.dag.UpdateConfiguration(&config); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		h.respondJSON(w, map[string]string{"status": "updated"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleRateLimit handles rate limiting configuration
func (h *EnhancedAPIHandler) handleRateLimit(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			NodeID            string  `json:"node_id"`
			RequestsPerSecond float64 `json:"requests_per_second"`
			Burst             int     `json:"burst"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		h.dag.SetRateLimit(req.NodeID, req.RequestsPerSecond, req.Burst)
		h.respondJSON(w, map[string]string{"status": "rate limit set"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleRetryConfig handles retry configuration
func (h *EnhancedAPIHandler) handleRetryConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		var config RetryConfig
		if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		h.dag.SetRetryConfig(&config)
		h.respondJSON(w, map[string]string{"status": "retry config updated"})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleTransaction handles transaction operations
func (h *EnhancedAPIHandler) handleTransaction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			TaskID string `json:"task_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		tx := h.dag.BeginTransaction(req.TaskID)
		if tx == nil {
			http.Error(w, "Failed to start transaction", http.StatusInternalServerError)
			return
		}

		h.respondJSON(w, map[string]interface{}{
			"transaction_id": tx.ID,
			"task_id":        tx.TaskID,
			"status":         "started",
		})

	case http.MethodPut:
		txID := r.URL.Query().Get("id")
		action := r.URL.Query().Get("action")

		if txID == "" {
			http.Error(w, "Transaction ID required", http.StatusBadRequest)
			return
		}

		var err error
		switch action {
		case "commit":
			err = h.dag.CommitTransaction(txID)
		case "rollback":
			err = h.dag.RollbackTransaction(txID)
		default:
			http.Error(w, "Invalid action. Use 'commit' or 'rollback'", http.StatusBadRequest)
			return
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		h.respondJSON(w, map[string]string{
			"transaction_id": txID,
			"status":         action + "ted",
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// optimizePerformance triggers performance optimization
func (h *EnhancedAPIHandler) optimizePerformance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := h.dag.OptimizePerformance()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.respondJSON(w, map[string]interface{}{
		"status":    "optimization completed",
		"timestamp": time.Now(),
	})
}

// getCircuitBreakerStatus returns circuit breaker status for nodes
func (h *EnhancedAPIHandler) getCircuitBreakerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("nodeId")

	if nodeID != "" {
		h.dag.circuitBreakersMu.RLock()
		cb, exists := h.dag.circuitBreakers[nodeID]
		h.dag.circuitBreakersMu.RUnlock()

		if !exists {
			http.Error(w, "Circuit breaker not found for node", http.StatusNotFound)
			return
		}

		status := map[string]interface{}{
			"node_id": nodeID,
			"state":   h.getCircuitBreakerStateName(cb.GetState()),
		}

		h.respondJSON(w, status)
	} else {
		// Return status for all circuit breakers
		h.dag.circuitBreakersMu.RLock()
		allStatus := make(map[string]interface{})
		for nodeID, cb := range h.dag.circuitBreakers {
			allStatus[nodeID] = h.getCircuitBreakerStateName(cb.GetState())
		}
		h.dag.circuitBreakersMu.RUnlock()

		h.respondJSON(w, allStatus)
	}
}

// clearCache clears the DAG cache
func (h *EnhancedAPIHandler) clearCache(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Clear next/prev node caches
	h.dag.nextNodesCache = nil
	h.dag.prevNodesCache = nil

	h.respondJSON(w, map[string]interface{}{
		"status":    "cache cleared",
		"timestamp": time.Now(),
	})
}

// getCacheStats returns cache statistics
func (h *EnhancedAPIHandler) getCacheStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := map[string]interface{}{
		"next_nodes_cache_size": len(h.dag.nextNodesCache),
		"prev_nodes_cache_size": len(h.dag.prevNodesCache),
		"timestamp":             time.Now(),
	}

	h.respondJSON(w, stats)
}

// Helper methods

func (h *EnhancedAPIHandler) respondJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (h *EnhancedAPIHandler) getCircuitBreakerStateName(state CircuitBreakerState) string {
	switch state {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// WebSocketHandler provides real-time monitoring via WebSocket
type WebSocketHandler struct {
	dag    *DAG
	logger logger.Logger
}

// NewWebSocketHandler creates a new WebSocket handler
func NewWebSocketHandler(dag *DAG) *WebSocketHandler {
	return &WebSocketHandler{
		dag:    dag,
		logger: dag.Logger(),
	}
}

// HandleWebSocket handles WebSocket connections for real-time monitoring
func (h *WebSocketHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// This would typically use a WebSocket library like gorilla/websocket
	// For now, we'll implement a basic structure

	// Upgrade HTTP connection to WebSocket
	// conn, err := websocket.Upgrade(w, r, nil)
	// if err != nil {
	//     h.logger.Error("WebSocket upgrade failed", logger.Field{Key: "error", Value: err.Error()})
	//     return
	// }
	// defer conn.Close()

	// Start monitoring loop
	// h.startMonitoringLoop(conn)
}

// AlertWebhookHandler handles webhook alerts
type AlertWebhookHandler struct {
	logger logger.Logger
}

// NewAlertWebhookHandler creates a new alert webhook handler
func NewAlertWebhookHandler(logger logger.Logger) *AlertWebhookHandler {
	return &AlertWebhookHandler{
		logger: logger,
	}
}

// HandleAlert implements the AlertHandler interface
func (h *AlertWebhookHandler) HandleAlert(alert Alert) error {
	h.logger.Warn("Alert received via webhook",
		logger.Field{Key: "type", Value: alert.Type},
		logger.Field{Key: "severity", Value: alert.Severity},
		logger.Field{Key: "message", Value: alert.Message},
		logger.Field{Key: "timestamp", Value: alert.Timestamp},
	)

	// Here you would typically send the alert to external systems
	// like Slack, email, PagerDuty, etc.

	return nil
}
