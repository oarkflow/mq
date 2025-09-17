package dag

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
)

// debugDAGTaskStart logs debug information when a task starts at DAG level
func (tm *DAG) debugDAGTaskStart(ctx context.Context, task *mq.Task, startTime time.Time) {
	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		payload = map[string]any{"raw_payload": string(task.Payload)}
	}
	tm.Logger().Info("🚀 [DEBUG] DAG task processing started",
		logger.Field{Key: "dag_name", Value: tm.name},
		logger.Field{Key: "dag_key", Value: tm.key},
		logger.Field{Key: "task_id", Value: task.ID},
		logger.Field{Key: "task_topic", Value: task.Topic},
		logger.Field{Key: "timestamp", Value: startTime.Format(time.RFC3339)},
		logger.Field{Key: "start_node", Value: tm.startNode},
		logger.Field{Key: "has_page_node", Value: tm.hasPageNode},
		logger.Field{Key: "is_paused", Value: tm.paused},
		logger.Field{Key: "payload_size", Value: len(task.Payload)},
		logger.Field{Key: "payload_preview", Value: tm.getDAGPayloadPreview(payload)},
		logger.Field{Key: "debug_enabled", Value: tm.debug},
	)
}

// debugDAGTaskComplete logs debug information when a task completes at DAG level
func (tm *DAG) debugDAGTaskComplete(ctx context.Context, task *mq.Task, result mq.Result, duration time.Duration, startTime time.Time) {
	var resultPayload map[string]any
	if len(result.Payload) > 0 {
		if err := json.Unmarshal(result.Payload, &resultPayload); err != nil {
			resultPayload = map[string]any{"raw_payload": string(result.Payload)}
		}
	}

	tm.Logger().Info("🏁 [DEBUG] DAG task processing completed",
		logger.Field{Key: "dag_name", Value: tm.name},
		logger.Field{Key: "dag_key", Value: tm.key},
		logger.Field{Key: "task_id", Value: task.ID},
		logger.Field{Key: "task_topic", Value: task.Topic},
		logger.Field{Key: "result_topic", Value: result.Topic},
		logger.Field{Key: "timestamp", Value: time.Now().Format(time.RFC3339)},
		logger.Field{Key: "total_duration", Value: duration.String()},
		logger.Field{Key: "status", Value: string(result.Status)},
		logger.Field{Key: "has_error", Value: result.Error != nil},
		logger.Field{Key: "error_message", Value: tm.getDAGErrorMessage(result.Error)},
		logger.Field{Key: "result_size", Value: len(result.Payload)},
		logger.Field{Key: "result_preview", Value: tm.getDAGPayloadPreview(resultPayload)},
		logger.Field{Key: "is_last", Value: result.Last},
		logger.Field{Key: "metrics", Value: tm.GetTaskMetrics()},
	)
}

// getDAGPayloadPreview returns a truncated version of the payload for debug logging
func (tm *DAG) getDAGPayloadPreview(payload map[string]any) string {
	if payload == nil {
		return "null"
	}

	preview := make(map[string]any)
	count := 0
	maxFields := 3 // Limit to first 3 fields for DAG level logging

	for key, value := range payload {
		if count >= maxFields {
			preview["..."] = fmt.Sprintf("and %d more fields", len(payload)-maxFields)
			break
		}

		// Truncate string values if they're too long
		if strVal, ok := value.(string); ok && len(strVal) > 50 {
			preview[key] = strVal[:47] + "..."
		} else {
			preview[key] = value
		}
		count++
	}

	previewBytes, _ := json.Marshal(preview)
	return string(previewBytes)
}

// getDAGErrorMessage safely extracts error message
func (tm *DAG) getDAGErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// GetCircuitBreakerStatus returns circuit breaker status for a node
func (tm *DAG) GetCircuitBreakerStatus(nodeID string) CircuitBreakerState {
	tm.circuitBreakersMu.RLock()
	defer tm.circuitBreakersMu.RUnlock()

	if cb, exists := tm.circuitBreakers[nodeID]; exists {
		return cb.GetState()
	}
	return CircuitClosed
}

// Transaction Management Methods

// BeginTransaction starts a new transaction
func (tm *DAG) BeginTransaction(taskID string) *Transaction {
	if tm.transactionManager != nil {
		return tm.transactionManager.BeginTransaction(taskID)
	}
	return nil
}

// CommitTransaction commits a transaction
func (tm *DAG) CommitTransaction(txID string) error {
	if tm.transactionManager != nil {
		return tm.transactionManager.CommitTransaction(txID)
	}
	return fmt.Errorf("transaction manager not initialized")
}

// RollbackTransaction rolls back a transaction
func (tm *DAG) RollbackTransaction(txID string) error {
	if tm.transactionManager != nil {
		return tm.transactionManager.RollbackTransaction(txID)
	}
	return fmt.Errorf("transaction manager not initialized")
}

// GetTransaction retrieves transaction details
func (tm *DAG) GetTransaction(txID string) (*Transaction, error) {
	if tm.transactionManager != nil {
		return tm.transactionManager.GetTransaction(txID)
	}
	return nil, fmt.Errorf("transaction manager not initialized")
}

// Enhanced DAG Methods for Production-Ready Features

// InitializeActivityLogger initializes the activity logger for the DAG
func (tm *DAG) InitializeActivityLogger(config ActivityLoggerConfig, persistence ActivityPersistence) {
	tm.activityLogger = NewActivityLogger(tm.name, config, persistence, tm.Logger())

	// Add activity logging hooks to existing components
	if tm.monitor != nil {
		tm.monitor.AddAlertHandler(&ActivityAlertHandler{activityLogger: tm.activityLogger})
	}

	tm.Logger().Info("Activity logger initialized for DAG",
		logger.Field{Key: "dag_name", Value: tm.name})
}

// GetActivityLogger returns the activity logger instance
func (tm *DAG) GetActivityLogger() *ActivityLogger {
	return tm.activityLogger
}

// LogActivity logs an activity entry
func (tm *DAG) LogActivity(ctx context.Context, level ActivityLevel, activityType ActivityType, message string, details map[string]interface{}) {
	if tm.activityLogger != nil {
		tm.activityLogger.LogWithContext(ctx, level, activityType, message, details)
	}
}

// GetActivityStats returns activity statistics
func (tm *DAG) GetActivityStats(filter ActivityFilter) (ActivityStats, error) {
	if tm.activityLogger != nil {
		return tm.activityLogger.GetStats(filter)
	}
	return ActivityStats{}, fmt.Errorf("activity logger not initialized")
}

// GetActivities retrieves activities based on filter
func (tm *DAG) GetActivities(filter ActivityFilter) ([]ActivityEntry, error) {
	if tm.activityLogger != nil {
		return tm.activityLogger.GetActivities(filter)
	}
	return nil, fmt.Errorf("activity logger not initialized")
}

// AddActivityHook adds an activity hook
func (tm *DAG) AddActivityHook(hook ActivityHook) {
	if tm.activityLogger != nil {
		tm.activityLogger.AddHook(hook)
	}
}

// FlushActivityLogs flushes activity logs to persistence
func (tm *DAG) FlushActivityLogs() error {
	if tm.activityLogger != nil {
		return tm.activityLogger.Flush()
	}
	return fmt.Errorf("activity logger not initialized")
}

// Clone creates a deep copy of the DAG
func (tm *DAG) Clone() *DAG {
	newDAG := NewDAG(tm.name+"_clone", tm.key, tm.finalResult)

	// Copy nodes
	tm.nodes.ForEach(func(id string, node *Node) bool {
		newDAG.AddNode(node.NodeType, node.Label, node.ID, node.processor)
		return true
	})

	// Copy edges
	tm.nodes.ForEach(func(id string, node *Node) bool {
		for _, edge := range node.Edges {
			newDAG.AddEdge(edge.Type, edge.Label, edge.From.ID, edge.To.ID)
		}
		return true
	})

	// Copy conditions
	for fromNode, conditions := range tm.conditions {
		newDAG.AddCondition(fromNode, conditions)
	}

	// Copy start node
	newDAG.SetStartNode(tm.startNode)

	return newDAG
}

// Export exports the DAG structure to a serializable format
func (tm *DAG) Export() map[string]any {
	export := map[string]any{
		"name":       tm.name,
		"key":        tm.key,
		"start_node": tm.startNode,
		"nodes":      make([]map[string]any, 0),
		"edges":      make([]map[string]any, 0),
		"conditions": tm.conditions,
	}

	// Export nodes
	tm.nodes.ForEach(func(id string, node *Node) bool {
		nodeData := map[string]any{
			"id":       node.ID,
			"label":    node.Label,
			"type":     node.NodeType.String(),
			"is_ready": node.isReady,
		}
		export["nodes"] = append(export["nodes"].([]map[string]any), nodeData)
		return true
	})

	// Export edges
	tm.nodes.ForEach(func(id string, node *Node) bool {
		for _, edge := range node.Edges {
			edgeData := map[string]any{
				"from":  edge.From.ID,
				"to":    edge.To.ID,
				"label": edge.Label,
				"type":  edge.Type.String(),
			}
			export["edges"] = append(export["edges"].([]map[string]any), edgeData)
		}
		return true
	})

	return export
}

// GetDAGStatistics returns comprehensive DAG statistics
func (tm *DAG) GetDAGStatistics() map[string]any {
	if tm.validator == nil {
		return map[string]any{"error": "validator not initialized"}
	}
	return tm.validator.GetNodeStatistics()
}

// StartMonitoring starts the monitoring system
func (tm *DAG) StartMonitoring(ctx context.Context) {
	if tm.monitor != nil {
		tm.monitor.Start(ctx)
	}
}

// StopMonitoring stops the monitoring system
func (tm *DAG) StopMonitoring() {
	if tm.monitor != nil {
		tm.monitor.Stop()
	}
}

// GetMonitoringMetrics returns current monitoring metrics
func (tm *DAG) GetMonitoringMetrics() *MonitoringMetrics {
	if tm.monitor != nil {
		return tm.monitor.GetMetrics()
	}
	return nil
}

// GetNodeStats returns statistics for a specific node
func (tm *DAG) GetNodeStats(nodeID string) *NodeStats {
	if tm.monitor != nil && tm.monitor.metrics != nil {
		return tm.monitor.metrics.GetNodeStats(nodeID)
	}
	return nil
}

// SetAlertThresholds configures alert thresholds
func (tm *DAG) SetAlertThresholds(thresholds *AlertThresholds) {
	if tm.monitor != nil {
		tm.monitor.SetAlertThresholds(thresholds)
	}
}

// AddAlertHandler adds an alert handler
func (tm *DAG) AddAlertHandler(handler AlertHandler) {
	if tm.monitor != nil {
		tm.monitor.AddAlertHandler(handler)
	}
}

// Configuration Management Methods

// GetConfiguration returns current DAG configuration
func (tm *DAG) GetConfiguration() *DAGConfig {
	if tm.configManager != nil {
		return tm.configManager.GetConfig()
	}
	return DefaultDAGConfig()
}

// UpdateConfiguration updates the DAG configuration
func (tm *DAG) UpdateConfiguration(config *DAGConfig) error {
	if tm.configManager != nil {
		return tm.configManager.UpdateConfiguration(config)
	}
	return fmt.Errorf("config manager not initialized")
}

// AddConfigWatcher adds a configuration change watcher
func (tm *DAG) AddConfigWatcher(watcher ConfigWatcher) {
	if tm.configManager != nil {
		tm.configManager.AddWatcher(watcher)
	}
}

// Rate Limiting Methods

// SetRateLimit sets rate limit for a specific node
func (tm *DAG) SetRateLimit(nodeID string, requestsPerSecond float64, burst int) {
	if tm.rateLimiter != nil {
		tm.rateLimiter.SetNodeLimit(nodeID, requestsPerSecond, burst)
	}
}

// CheckRateLimit checks if request is allowed for a node
func (tm *DAG) CheckRateLimit(nodeID string) bool {
	if tm.rateLimiter != nil {
		return tm.rateLimiter.Allow(nodeID)
	}
	return true
}

// Retry and Circuit Breaker Methods

// SetRetryConfig sets the retry configuration
func (tm *DAG) SetRetryConfig(config *RetryConfig) {
	if tm.retryManager != nil {
		tm.retryManager.SetGlobalConfig(config)
	}
}

// AddNodeWithRetry adds a node with specific retry configuration
func (tm *DAG) AddNodeWithRetry(nodeType NodeType, name, nodeID string, handler mq.Processor, retryConfig *RetryConfig, startNode ...bool) *DAG {
	tm.AddNode(nodeType, name, nodeID, handler, startNode...)
	if tm.retryManager != nil {
		tm.retryManager.SetNodeConfig(nodeID, retryConfig)
	}
	return tm
}

// ActivityAlertHandler handles alerts by logging them as activities
type ActivityAlertHandler struct {
	activityLogger *ActivityLogger
}

func (h *ActivityAlertHandler) HandleAlert(alert Alert) error {
	if h.activityLogger != nil {
		h.activityLogger.Log(
			ActivityLevelWarn,
			ActivityTypeAlert,
			alert.Message,
			map[string]any{
				"alert_type":      alert.Type,
				"alert_severity":  alert.Severity,
				"alert_node_id":   alert.NodeID,
				"alert_timestamp": alert.Timestamp,
			},
		)
	}
	return nil
}
