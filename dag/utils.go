package dag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2/middleware/session"
	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	dagstorage "github.com/oarkflow/mq/dag/storage"
	"github.com/oarkflow/mq/logger"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
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
func (tm *DAG) LogActivity(ctx context.Context, level ActivityLevel, activityType ActivityType, message string, details map[string]any) {
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

// Clone creates a deep copy of the DAG instance as a separate instance
// This function creates a completely independent copy of the DAG with all its nodes,
// edges, conditions, and internal state. The cloned DAG can be modified without
// affecting the original DAG.
func (d *DAG) Clone() *DAG {
	// Create new DAG instance with basic fields
	clone := &DAG{
		// Primitive fields (shallow copy)
		key:           d.key,
		name:          d.name,
		startNode:     d.startNode,
		consumerTopic: d.consumerTopic,
		report:        d.report,
		httpPrefix:    d.httpPrefix,
		hasPageNode:   d.hasPageNode,
		paused:        d.paused,
		debug:         d.debug,
		Error:         d.Error, // Error is safe to shallow copy

		// Function pointers (shallow copy)
		finalResult:              d.finalResult,
		reportNodeResultCallback: d.reportNodeResultCallback,
		PreProcessHook:           d.PreProcessHook,
		PostProcessHook:          d.PostProcessHook,

		// Initialize storage maps
		nodes:         memory.New[string, *Node](),
		taskManager:   memory.New[string, *TaskManager](),
		iteratorNodes: memory.New[string, []Edge](),

		// Initialize other maps
		conditions:      make(map[string]map[string]string),
		nextNodesCache:  &sync.Map{},
		prevNodesCache:  &sync.Map{},
		circuitBreakers: make(map[string]*CircuitBreaker),
		nodeMiddlewares: make(map[string][]mq.Handler),

		// Initialize slices
		globalMiddlewares: make([]mq.Handler, 0),

		// Initialize mutexes
		circuitBreakersMu: sync.RWMutex{},
		middlewaresMu:     sync.RWMutex{},

		// Create new task storage
		taskStorage: dagstorage.NewMemoryTaskStorage(),
	}

	// Deep copy nodes
	d.nodes.ForEach(func(nodeID string, node *Node) bool {
		clonedNode := d.cloneNode(node)
		clone.nodes.Set(nodeID, clonedNode)
		return true
	})

	// Deep copy iterator nodes
	d.iteratorNodes.ForEach(func(nodeID string, edges []Edge) bool {
		clonedEdges := make([]Edge, len(edges))
		for i, edge := range edges {
			clonedEdges[i] = d.cloneEdge(edge, clone.nodes)
		}
		clone.iteratorNodes.Set(nodeID, clonedEdges)
		return true
	})

	// Deep copy conditions
	for nodeID, conds := range d.conditions {
		cloneConds := make(map[string]string)
		for k, v := range conds {
			cloneConds[k] = v
		}
		clone.conditions[nodeID] = cloneConds
	}

	// Deep copy caches
	if d.nextNodesCache != nil {
		d.nextNodesCache.Range(func(key, value any) bool {
			nodeID := key.(string)
			nodes := value.([]*Node)
			clonedNodes := make([]*Node, len(nodes))
			for i, node := range nodes {
				// Find the cloned node by ID
				if clonedNode, exists := clone.nodes.Get(node.ID); exists {
					clonedNodes[i] = clonedNode
				}
			}
			clone.nextNodesCache.Store(nodeID, clonedNodes)
			return true
		})
	}

	if d.prevNodesCache != nil {
		d.prevNodesCache.Range(func(key, value any) bool {
			nodeID := key.(string)
			nodes := value.([]*Node)
			clonedNodes := make([]*Node, len(nodes))
			for i, node := range nodes {
				// Find the cloned node by ID
				if clonedNode, exists := clone.nodes.Get(node.ID); exists {
					clonedNodes[i] = clonedNode
				}
			}
			clone.prevNodesCache.Store(nodeID, clonedNodes)
			return true
		})
	}

	// Deep copy circuit breakers
	for nodeID, cb := range d.circuitBreakers {
		// Create new circuit breaker with same config
		if cb.config != nil {
			newCB := NewCircuitBreaker(cb.config, nil) // Logger will be set later if needed
			clone.circuitBreakers[nodeID] = newCB
		}
	}

	// Deep copy node middlewares
	for nodeID, handlers := range d.nodeMiddlewares {
		clonedHandlers := make([]mq.Handler, len(handlers))
		copy(clonedHandlers, handlers)
		clone.nodeMiddlewares[nodeID] = clonedHandlers
	}

	// Deep copy global middlewares
	clone.globalMiddlewares = make([]mq.Handler, len(d.globalMiddlewares))
	copy(clone.globalMiddlewares, d.globalMiddlewares)

	// Deep copy metrics
	if d.metrics != nil {
		clone.metrics = &TaskMetrics{
			NotStarted: d.metrics.NotStarted,
			Queued:     d.metrics.Queued,
			Cancelled:  d.metrics.Cancelled,
			Completed:  d.metrics.Completed,
			Failed:     d.metrics.Failed,
		}
	}

	// Initialize server with minimal configuration to prevent nil pointer panics
	// The cloned DAG will need to be properly configured before use
	clone.server = mq.NewBroker(
		mq.WithCallback(clone.onTaskCallback),
		mq.WithConsumerOnSubscribe(clone.onConsumerJoin),
		mq.WithConsumerOnClose(clone.onConsumerClose),
	)

	// Initialize logger-dependent managers with null logger as fallback
	nullLogger := &logger.NullLogger{}
	clone.validator = NewDAGValidator(clone)
	clone.monitor = NewMonitor(clone, nullLogger)
	clone.retryManager = NewNodeRetryManager(nil, nullLogger)
	clone.rateLimiter = NewRateLimiter(nullLogger)
	clone.cache = NewDAGCache(5*time.Minute, 1000, nullLogger)
	clone.configManager = NewConfigManager(nullLogger)
	clone.batchProcessor = NewBatchProcessor(clone, 50, 5*time.Second, nullLogger)
	clone.transactionManager = NewTransactionManager(clone, nullLogger)
	clone.cleanupManager = NewCleanupManager(clone, 10*time.Minute, 1*time.Hour, 1000, nullLogger)
	clone.performanceOptimizer = NewPerformanceOptimizer(clone, clone.monitor, clone.configManager, nullLogger)

	// Note: Shared resources like consumer, pool, scheduler, Notifier
	// are intentionally NOT cloned as they represent shared system resources.
	// The cloned DAG will need to initialize these separately if needed.

	// Note: Manager objects are initialized with null logger as fallback.
	// The cloned DAG should be properly configured with a real logger before use.

	return clone
}

// cloneNode creates a deep copy of a Node
func (d *DAG) cloneNode(node *Node) *Node {
	clonedNode := &Node{
		Label:    node.Label,
		ID:       node.ID,
		NodeType: node.NodeType,
		isReady:  node.isReady,
		Timeout:  node.Timeout,
		Debug:    node.Debug,
		IsFirst:  node.IsFirst,
		IsLast:   node.IsLast,
	}

	// Deep copy edges
	clonedNode.Edges = make([]Edge, len(node.Edges))
	for i, edge := range node.Edges {
		clonedNode.Edges[i] = d.cloneEdge(edge, nil) // Will be updated later with cloned nodes
	}

	// Clone processor - this is complex as it could be a DAG or other processor
	// For now, we'll shallow copy and let the caller handle processor-specific cloning
	clonedNode.processor = node.processor

	return clonedNode
}

// cloneEdge creates a copy of an Edge, optionally updating node references
func (d *DAG) cloneEdge(edge Edge, nodeMap storage.IMap[string, *Node]) Edge {
	clonedEdge := Edge{
		FromSource: edge.FromSource,
		Label:      edge.Label,
		Type:       edge.Type,
	}

	// Update node references if nodeMap is provided
	if nodeMap != nil {
		if clonedFrom, exists := nodeMap.Get(edge.From.ID); exists {
			clonedEdge.From = clonedFrom
		} else {
			clonedEdge.From = edge.From // Keep original if clone not found
		}

		if clonedTo, exists := nodeMap.Get(edge.To.ID); exists {
			clonedEdge.To = clonedTo
		} else {
			clonedEdge.To = edge.To // Keep original if clone not found
		}
	} else {
		// Keep original node references if no nodeMap provided
		clonedEdge.From = edge.From
		clonedEdge.To = edge.To
	}

	return clonedEdge
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

func CanSession(ctx context.Context, key string) bool {
	sess, ok := ctx.Value("session").(*session.Session)
	if !ok || sess == nil {
		return false
	}
	if authenticated, exists := sess.Get(key).(bool); exists && authenticated {
		return true
	}
	return false
}

func GetSession(ctx context.Context, key string) (any, bool) {
	sess, ok := ctx.Value("session").(*session.Session)
	if !ok || sess == nil {
		return nil, false
	}
	value := sess.Get(key)
	if value != nil {
		return value, true
	}
	return nil, false
}
