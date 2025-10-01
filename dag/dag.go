package dag

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/oarkflow/form"
	"github.com/oarkflow/json"
	"golang.org/x/time/rate"

	dagstorage "github.com/oarkflow/mq/dag/storage"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/logger"
	"github.com/oarkflow/mq/sio"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

// New task metrics type.
type TaskMetrics struct {
	mu         sync.Mutex
	NotStarted int
	Queued     int
	Cancelled  int
	Completed  int
	Failed     int
}

// NodeMiddleware represents middleware configuration for a specific node
type NodeMiddleware struct {
	Node        string
	Middlewares []mq.Handler
}

type Node struct {
	processor mq.Processor
	Label     string
	ID        string
	Edges     []Edge
	NodeType  NodeType
	isReady   bool
	Timeout   time.Duration // ...new field for node-level timeout...
	Debug     bool          // Individual node debug mode
	IsFirst   bool          // Identifier for the first node in the DAG
	IsLast    bool          // Identifier for last nodes in the DAG (can be multiple)
}

// SetTimeout allows setting a maximum processing duration for the node.
func (n *Node) SetTimeout(d time.Duration) {
	n.Timeout = d
}

// SetDebug enables or disables debug mode for this specific node.
func (n *Node) SetDebug(enabled bool) {
	n.Debug = enabled
}

// IsDebugEnabled checks if debug is enabled for this node or globally.
func (n *Node) IsDebugEnabled(dagDebug bool) bool {
	return n.Debug || dagDebug
}

type Edge struct {
	From       *Node
	FromSource string
	To         *Node
	Label      string
	Type       EdgeType
}

type DAG struct {
	nodes                    storage.IMap[string, *Node]
	taskManager              storage.IMap[string, *TaskManager]
	iteratorNodes            storage.IMap[string, []Edge]
	conditions               map[string]map[string]string
	Error                    error
	parentDAG                *DAG
	nodeIDInParentDAG        string
	consumer                 *mq.Consumer
	finalResult              func(taskID string, result mq.Result)
	pool                     *mq.Pool
	scheduler                *mq.Scheduler
	Notifier                 *sio.Server
	server                   *mq.Broker
	reportNodeResultCallback func(mq.Result)
	key                      string
	consumerTopic            string
	startNode                string
	name                     string
	report                   string
	hasPageNode              bool
	paused                   bool
	httpPrefix               string
	nextNodesCache           *sync.Map // map[string][]*Node - thread-safe cache
	prevNodesCache           *sync.Map // map[string][]*Node - thread-safe cache
	// New hook fields:
	PreProcessHook  func(ctx context.Context, node *Node, taskID string, payload json.RawMessage) context.Context
	PostProcessHook func(ctx context.Context, node *Node, taskID string, result mq.Result)
	metrics         *TaskMetrics // <-- new field for task metrics

	// Enhanced features
	validator            *DAGValidator
	monitor              *Monitor
	retryManager         *NodeRetryManager
	rateLimiter          *RateLimiter
	cache                *DAGCache
	configManager        *ConfigManager
	batchProcessor       *BatchProcessor
	transactionManager   *TransactionManager
	cleanupManager       *CleanupManager
	webhookManager       *WebhookManager
	performanceOptimizer *PerformanceOptimizer
	activityLogger       *ActivityLogger

	// Circuit breakers per node
	circuitBreakers   map[string]*CircuitBreaker
	circuitBreakersMu sync.RWMutex

	// Debug configuration
	debug bool // Global debug mode for the entire DAG

	// Middleware configuration
	globalMiddlewares []mq.Handler
	nodeMiddlewares   map[string][]mq.Handler
	middlewaresMu     sync.RWMutex

	// Task storage for persistence
	taskStorage dagstorage.TaskStorage
}

// SetPreProcessHook configures a function to be called before each node is processed.
func (tm *DAG) SetPreProcessHook(hook func(ctx context.Context, node *Node, taskID string, payload json.RawMessage) context.Context) {
	tm.PreProcessHook = hook
}

// SetPostProcessHook configures a function to be called after each node is processed.
func (tm *DAG) SetPostProcessHook(hook func(ctx context.Context, node *Node, taskID string, result mq.Result)) {
	tm.PostProcessHook = hook
}

// SetDebug enables or disables debug mode for the entire DAG.
func (tm *DAG) SetDebug(enabled bool) {
	tm.debug = enabled
}

// IsDebugEnabled returns whether debug mode is enabled for the DAG.
func (tm *DAG) IsDebugEnabled() bool {
	return tm.debug
}

// SetNodeDebug enables or disables debug mode for a specific node.
func (tm *DAG) SetNodeDebug(nodeID string, enabled bool) error {
	node, exists := tm.nodes.Get(nodeID)
	if !exists {
		return fmt.Errorf("node with ID '%s' not found", nodeID)
	}
	node.SetDebug(enabled)
	return nil
}

// SetAllNodesDebug enables or disables debug mode for all nodes in the DAG.
func (tm *DAG) SetAllNodesDebug(enabled bool) {
	tm.nodes.ForEach(func(nodeID string, node *Node) bool {
		node.SetDebug(enabled)
		return true
	})
}

// GetDebugInfo returns debug information about the DAG and its nodes.
func (tm *DAG) GetDebugInfo() map[string]any {
	debugInfo := map[string]any{
		"dag_name":          tm.name,
		"dag_key":           tm.key,
		"dag_debug_enabled": tm.debug,
		"start_node":        tm.startNode,
		"has_page_node":     tm.hasPageNode,
		"is_paused":         tm.paused,
		"nodes":             make(map[string]map[string]any),
	}

	nodesInfo := debugInfo["nodes"].(map[string]map[string]any)
	tm.nodes.ForEach(func(nodeID string, node *Node) bool {
		nodesInfo[nodeID] = map[string]any{
			"id":            node.ID,
			"label":         node.Label,
			"type":          node.NodeType.String(),
			"debug_enabled": node.Debug,
			"has_timeout":   node.Timeout > 0,
			"timeout":       node.Timeout.String(),
			"edge_count":    len(node.Edges),
		}
		return true
	})

	return debugInfo
}

// EnableEnhancedFeatures configures the DAG with enhanced features
func (tm *DAG) EnableEnhancedFeatures(config *EnhancedDAGConfig) error {
	if config == nil {
		return fmt.Errorf("enhanced DAG config cannot be nil")
	}

	// Get the logger from the server
	var dagLogger logger.Logger
	if tm.server != nil {
		dagLogger = tm.server.Options().Logger()
	} else {
		// Create a null logger as fallback
		dagLogger = &logger.NullLogger{}
	}

	// Initialize enhanced features if needed
	if config.EnableStateManagement {
		// State management is already built into the DAG
		tm.SetDebug(true) // Enable debug for better state tracking
	}

	if config.EnableAdvancedRetry {
		// Initialize retry manager if not already present
		if tm.retryManager == nil {
			tm.retryManager = NewNodeRetryManager(nil, dagLogger)
		}
	}

	if config.EnableMetrics {
		// Initialize metrics if not already present
		if tm.metrics == nil {
			tm.metrics = &TaskMetrics{}
		}
	}

	if config.MaxConcurrentExecutions > 0 {
		// Set up rate limiting
		if tm.rateLimiter == nil {
			tm.rateLimiter = NewRateLimiter(dagLogger)
		}
	}

	return nil
}

// Use adds global middleware handlers that will be executed for all nodes in the DAG
func (tm *DAG) Use(handlers ...mq.Handler) {
	tm.middlewaresMu.Lock()
	defer tm.middlewaresMu.Unlock()
	tm.globalMiddlewares = append(tm.globalMiddlewares, handlers...)
}

// UseNodeMiddlewares adds middleware handlers for specific nodes
func (tm *DAG) UseNodeMiddlewares(nodeMiddlewares ...NodeMiddleware) {
	tm.middlewaresMu.Lock()
	defer tm.middlewaresMu.Unlock()
	for _, nm := range nodeMiddlewares {
		if nm.Node == "" {
			continue
		}
		if tm.nodeMiddlewares[nm.Node] == nil {
			tm.nodeMiddlewares[nm.Node] = make([]mq.Handler, 0)
		}
		tm.nodeMiddlewares[nm.Node] = append(tm.nodeMiddlewares[nm.Node], nm.Middlewares...)
	}
}

// executeMiddlewares executes a chain of middlewares and then the final handler
func (tm *DAG) executeMiddlewares(ctx context.Context, task *mq.Task, middlewares []mq.Handler, finalHandler func(context.Context, *mq.Task) mq.Result) mq.Result {
	if len(middlewares) == 0 {
		return finalHandler(ctx, task)
	}

	// For this implementation, we'll execute middlewares sequentially
	// Each middleware can modify the task/context and return a result
	// If a middleware returns a result with Status == Completed and no error,
	// we consider it as "continue to next"

	for _, middleware := range middlewares {
		result := middleware(ctx, task)
		// If middleware returns an error or failed status, short-circuit
		if result.Error != nil || result.Status == mq.Failed {
			return result
		}
		// Update task payload if middleware modified it
		if len(result.Payload) > 0 {
			task.Payload = result.Payload
		}
		// Update context if middleware provided one
		if result.Ctx != nil {
			ctx = result.Ctx
		}
	}

	// All middlewares passed, execute final handler
	return finalHandler(ctx, task)
}

// getNodeMiddlewares returns all middlewares for a specific node (global + node-specific)
func (tm *DAG) getNodeMiddlewares(nodeID string) []mq.Handler {
	tm.middlewaresMu.RLock()
	defer tm.middlewaresMu.RUnlock()

	var allMiddlewares []mq.Handler

	// Add global middlewares first
	allMiddlewares = append(allMiddlewares, tm.globalMiddlewares...)

	// Add node-specific middlewares
	if nodeMiddlewares, exists := tm.nodeMiddlewares[nodeID]; exists {
		allMiddlewares = append(allMiddlewares, nodeMiddlewares...)
	}

	return allMiddlewares
}

func NewDAG(name, key string, finalResultCallback func(taskID string, result mq.Result), opts ...mq.Option) *DAG {
	callback := func(ctx context.Context, result mq.Result) error { return nil }
	d := &DAG{
		name:            name,
		key:             key,
		nodes:           memory.New[string, *Node](),
		taskManager:     memory.New[string, *TaskManager](),
		iteratorNodes:   memory.New[string, []Edge](),
		conditions:      make(map[string]map[string]string),
		finalResult:     finalResultCallback,
		metrics:         &TaskMetrics{}, // <-- initialize metrics
		circuitBreakers: make(map[string]*CircuitBreaker),
		nextNodesCache:  &sync.Map{},
		prevNodesCache:  &sync.Map{},
		nodeMiddlewares: make(map[string][]mq.Handler),
		taskStorage:     dagstorage.NewMemoryTaskStorage(), // Initialize default memory storage
	}

	opts = append(opts,
		mq.WithCallback(d.onTaskCallback),
		mq.WithConsumerOnSubscribe(d.onConsumerJoin),
		mq.WithConsumerOnClose(d.onConsumerClose),
	)
	d.server = mq.NewBroker(opts...)

	// Initialize all enhanced features for DAG broker
	enhancedConfig := mq.DefaultBrokerEnhancedConfig()
	enhancedConfig.Logger = d.server.Options().Logger()
	enhancedConfig.EnableEnhancements = true

	// Initialize enhanced features (DLQ, WAL, ACK, dedup, flow control, etc.)
	if err := d.server.InitializeEnhancements(enhancedConfig); err != nil {
		log.Printf("[WARN] Failed to initialize enhanced features for DAG: %v", err)
	} else {
		log.Printf("[INFO] Enhanced features initialized for DAG: %s", name)
	}

	// Now initialize enhanced features that need the server
	logger := d.server.Options().Logger()
	d.validator = NewDAGValidator(d)
	d.monitor = NewMonitor(d, logger)
	d.retryManager = NewNodeRetryManager(nil, logger)
	d.rateLimiter = NewRateLimiter(logger)
	d.cache = NewDAGCache(5*time.Minute, 1000, logger)
	d.configManager = NewConfigManager(logger)
	d.batchProcessor = NewBatchProcessor(d, 50, 5*time.Second, logger)
	d.transactionManager = NewTransactionManager(d, logger)
	d.cleanupManager = NewCleanupManager(d, 10*time.Minute, 1*time.Hour, 1000, logger)
	d.performanceOptimizer = NewPerformanceOptimizer(d, d.monitor, d.configManager, logger)

	options := d.server.Options()
	d.pool = mq.NewPool(
		options.NumOfWorkers(),
		mq.WithTaskQueueSize(options.QueueSize()),
		mq.WithMaxMemoryLoad(options.MaxMemoryLoad()),
		mq.WithHandler(d.ProcessTask),
		mq.WithPoolCallback(callback),
		mq.WithTaskStorage(options.Storage()),
	)
	d.scheduler = mq.NewScheduler(d.pool)
	d.pool.Start(d.server.Options().NumOfWorkers())
	return d
}

// New method to update task metrics.
func (d *DAG) updateTaskMetrics(taskID string, result mq.Result, duration time.Duration) {
	d.metrics.mu.Lock()
	defer d.metrics.mu.Unlock()
	switch result.Status {
	case mq.Completed:
		d.metrics.Completed++
	case mq.Failed:
		d.metrics.Failed++
	case mq.Cancelled:
		d.metrics.Cancelled++
	}
	if d.debug {
		d.Logger().Info("Updating task metrics",
			logger.Field{Key: "taskID", Value: taskID},
			logger.Field{Key: "lastExecuted", Value: time.Now()},
			logger.Field{Key: "duration", Value: duration},
			logger.Field{Key: "success", Value: result.Status},
		)
	}
}

// Getter for task metrics.
func (d *DAG) GetTaskMetrics() TaskMetrics {
	d.metrics.mu.Lock()
	defer d.metrics.mu.Unlock()
	return TaskMetrics{
		NotStarted: d.metrics.NotStarted,
		Queued:     d.metrics.Queued,
		Cancelled:  d.metrics.Cancelled,
		Completed:  d.metrics.Completed,
		Failed:     d.metrics.Failed,
	}
}

func (tm *DAG) SetKey(key string) {
	tm.key = key
}

func (tm *DAG) ReportNodeResult(callback func(mq.Result)) {
	tm.reportNodeResultCallback = callback
}

func (tm *DAG) onTaskCallback(ctx context.Context, result mq.Result) mq.Result {
	if manager, ok := tm.taskManager.Get(result.TaskID); ok && result.Topic != "" {
		manager.onNodeCompleted(nodeResult{
			ctx:    ctx,
			nodeID: result.Topic,
			status: result.Status,
			result: result,
		})
	}
	return mq.Result{}
}

func (tm *DAG) GetType() string {
	return tm.key
}

func (tm *DAG) Stop(ctx context.Context) error {
	tm.nodes.ForEach(func(_ string, n *Node) bool {
		err := n.processor.Stop(ctx)
		if err != nil {
			return false
		}
		return true
	})
	return nil
}

func (tm *DAG) GetKey() string {
	return tm.key
}

func (tm *DAG) SetNotifyResponse(callback mq.Callback) {
	tm.server.SetNotifyHandler(callback)
}

func (tm *DAG) Logger() logger.Logger {
	return tm.server.Options().Logger()
}

func (tm *DAG) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Enhanced processing with monitoring and rate limiting
	startTime := time.Now()

	// Debug logging at DAG level
	if tm.IsDebugEnabled() {
		tm.debugDAGTaskStart(ctx, task, startTime)
	}

	// Record task start in monitoring
	if tm.monitor != nil {
		tm.monitor.metrics.RecordTaskStart(task.ID)
	}

	// Check rate limiting
	if tm.rateLimiter != nil && !tm.rateLimiter.Allow(task.Topic) {
		if err := tm.rateLimiter.Wait(ctx, task.Topic); err != nil {
			return mq.Result{
				Error: fmt.Errorf("rate limit exceeded for node %s: %w", task.Topic, err),
				Ctx:   ctx,
			}
		}
	}

	// Get circuit breaker for the node
	circuitBreaker := tm.getOrCreateCircuitBreaker(task.Topic)

	var result mq.Result

	// Execute with circuit breaker protection
	err := circuitBreaker.Execute(func() error {
		result = tm.processTaskInternal(ctx, task)
		return result.Error
	})

	if err != nil && result.Error == nil {
		result.Error = err
		result.Ctx = ctx
	}

	// Record completion
	duration := time.Since(startTime)
	if tm.monitor != nil {
		tm.monitor.metrics.RecordTaskCompletion(task.ID, result.Status)
		tm.monitor.metrics.RecordNodeExecution(task.Topic, duration, result.Error == nil)
	}

	// Update internal metrics
	tm.updateTaskMetrics(task.ID, result, duration)

	// Debug logging at DAG level for task completion
	if tm.IsDebugEnabled() {
		tm.debugDAGTaskComplete(ctx, task, result, duration, startTime)
	}

	// Trigger webhooks if configured
	if tm.webhookManager != nil {
		event := WebhookEvent{
			Type:      "task_completed",
			TaskID:    task.ID,
			NodeID:    task.Topic,
			Timestamp: time.Now(),
			Data: map[string]any{
				"status":   string(result.Status),
				"duration": duration.String(),
				"success":  result.Error == nil,
			},
		}
		tm.webhookManager.TriggerWebhook(event)
	}

	return result
}

func (tm *DAG) processTaskInternal(ctx context.Context, task *mq.Task) mq.Result {
	ctx = context.WithValue(ctx, "task_id", task.ID)
	userContext := form.UserContext(ctx)
	next := userContext.Get("next")

	// Ensure we have a TaskManager and a fresh resultCh for this synchronous wait.
	manager, ok := tm.taskManager.Get(task.ID)
	resultCh := make(chan mq.Result, 1) // buffered to avoid trivial races
	if !ok {
		manager = NewTaskManager(tm, task.ID, resultCh, tm.iteratorNodes.Clone(), tm.taskStorage)
		tm.taskManager.Set(task.ID, manager)
		if tm.debug {
			tm.Logger().Info("Processing task",
				logger.Field{Key: "taskID", Value: task.ID},
				logger.Field{Key: "phase", Value: "start"},
				logger.Field{Key: "timestamp", Value: time.Now()},
			)
		}
	} else {
		if manager.result != nil && manager.result.Status == mq.Completed {
			currentKey := tm.getCurrentNode(manager)
			currentNode := strings.Split(currentKey, Delimiter)[0]
			isLast, err := tm.IsLastNode(currentNode)
			if err == nil && isLast {
				return *manager.result
			}
		}

		// Replace the manager's result channel so waiting here is wired to this call.
		manager.resultCh = resultCh
		tm.Logger().Info("Resuming task",
			logger.Field{Key: "taskID", Value: task.ID},
			logger.Field{Key: "phase", Value: "resume"},
			logger.Field{Key: "timestamp", Value: time.Now()},
		)
	}

	// Determine the current node and possibly handle sub-DAGs
	currentKey := tm.getCurrentNode(manager)
	currentNode := strings.Split(currentKey, Delimiter)[0]
	node, exists := tm.nodes.Get(currentNode)

	if node != nil {
		if subDag, isSubDAG := node.processor.(*DAG); isSubDAG {
			tm.Logger().Info("Processing subDAG",
				logger.Field{Key: "subDAG", Value: subDag.name},
				logger.Field{Key: "taskID", Value: task.ID},
			)
			result := subDag.processTaskInternal(ctx, task)
			if result.Error != nil {
				return result
			}

			// Handle ResetTo from subDAG - if subDAG resets, parent should reset to subDAG node
			if result.ResetTo != "" {
				tm.Logger().Info("SubDAG requested reset",
					logger.Field{Key: "subDAG", Value: subDag.name},
					logger.Field{Key: "resetTo", Value: result.ResetTo},
					logger.Field{Key: "taskID", Value: task.ID},
				)

				// Notify parent task manager to handle the reset properly
				if manager, ok := tm.taskManager.Get(task.ID); ok {
					// For subDAG reset, we need to handle it differently
					// The subDAG has already processed the reset internally, but we need to ensure
					// the parent DAG shows the subDAG page again

					// Clear downstream nodes that depend on the subDAG
					tm.clearDownstreamNodesForSubDAGReset(manager, currentNode)

					// Reset the subDAG node state to allow re-execution
					if state, exists := manager.taskStates.Get(currentNode); exists {
						state.Status = mq.Pending
						state.UpdatedAt = time.Now()
						state.Result = mq.Result{}
					}

					// Clear current node result to force re-execution
					manager.currentNodeResult.Del(currentNode)

					// Re-enqueue the task for the subDAG node with the error payload
					manager.enqueueTask(ctx, currentNode, task.ID, result.Payload)

					// Return the result from subDAG (which may contain error messages)
					// but ensure ResetTo is not propagated to avoid infinite loops
					result.ResetTo = ""
					return result
				}
			}

			// Check if subDAG returned a page result (indicated by Topic being set)
			if result.Topic != "" {
				// Check if the topic corresponds to a page node in the subDAG
				if subDagNode, exists := subDag.nodes.Get(result.Topic); exists && subDagNode.NodeType == Page {
					return result
				}
				// Also check if the topic corresponds to a page node in the parent DAG
				if parentNode, exists := tm.nodes.Get(result.Topic); exists && parentNode.NodeType == Page {
					return result
				}
			}
			if m, ok := subDag.taskManager.Get(task.ID); !subDag.hasPageNode || (ok && m != nil && m.result != nil) {
				task.Payload = result.Payload
			}
		}
	}

	method, _ := ctx.Value("method").(string)
	if method == "GET" && exists && node.NodeType == Page {
		ctx = context.WithValue(ctx, "initial_node", currentNode)
		if manager.result != nil {
			task.Payload = manager.result.Payload
		}
	} else if next == "true" {
		nodes, err := tm.GetNextNodes(currentNode)
		if err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		if len(nodes) > 0 {
			ctx = context.WithValue(ctx, "initial_node", nodes[0].ID)
		}
	}

	// Merge previous node result into task payload if present
	if currentNodeResult, hasResult := manager.currentNodeResult.Get(currentKey); hasResult {
		var taskPayload, resultPayload map[string]any
		if err := json.Unmarshal(task.Payload, &taskPayload); err == nil {
			if err = json.Unmarshal(currentNodeResult.Payload, &resultPayload); err == nil {
				for key, val := range resultPayload {
					taskPayload[key] = val
				}
				if newPayload, err := json.Marshal(taskPayload); err == nil {
					task.Payload = newPayload
				} else if tm.debug {
					tm.Logger().Error("Error marshalling merged payload",
						logger.Field{Key: "error", Value: err})
				}
			} else if tm.debug {
				tm.Logger().Error("Error unmarshalling current node result payload",
					logger.Field{Key: "error", Value: err})
			}
		} else if tm.debug {
			tm.Logger().Error("Error unmarshalling task payload",
				logger.Field{Key: "error", Value: err})
		}
	}

	// Determine initial node to start processing from
	firstNode, err := tm.parseInitialNode(ctx)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	node, _ = tm.nodes.Get(firstNode)
	task.Topic = firstNode
	ctx = context.WithValue(ctx, ContextIndex, "0")
	manager.ProcessTask(ctx, firstNode, task.Payload)

	// WAITING PHASE (no time.After / sleeps)
	// If DAG has page nodes, block until an external SubmitPage sends an mq.Result into resultCh.
	// For non-page flows, do the same: block until one result arrives or the ctx is cancelled.
	waitForResult := func() mq.Result {
		var results []mq.Result

		// Block until first result or cancellation.
		select {
		case r := <-resultCh:
			results = append(results, r)
		case <-ctx.Done():
			return mq.Result{
				Error: fmt.Errorf("context cancelled while waiting for task result: %w", ctx.Err()),
				Ctx:   ctx,
			}
		}

		// Drain any already-queued results without waiting (non-blocking).
		for {
			select {
			case r := <-resultCh:
				results = append(results, r)
				// keep draining until empty
				continue
			default:
				// return the most recent result
				return results[len(results)-1]
			}
		}
	}

	if tm.hasPageNode {
		return waitForResult()
	}
	return waitForResult()
}

func (tm *DAG) ProcessTaskNew(ctx context.Context, task *mq.Task) mq.Result {
	ctx = context.WithValue(ctx, "task_id", task.ID)
	userContext := form.UserContext(ctx)
	next := userContext.Get("next")

	manager, ok := tm.taskManager.Get(task.ID)
	resultCh := make(chan mq.Result, 1)
	if !ok {
		manager = NewTaskManager(tm, task.ID, resultCh, tm.iteratorNodes.Clone(), tm.taskStorage)
		tm.taskManager.Set(task.ID, manager)
		if tm.debug {
			tm.Logger().Info("Processing task",
				logger.Field{Key: "taskID", Value: task.ID},
				logger.Field{Key: "phase", Value: "start"},
				logger.Field{Key: "timestamp", Value: time.Now()},
			)
		}
	} else {
		manager.resultCh = resultCh
		tm.Logger().Info("Resuming task",
			logger.Field{Key: "taskID", Value: task.ID},
			logger.Field{Key: "phase", Value: "resume"},
			logger.Field{Key: "timestamp", Value: time.Now()},
		)
	}

	currentKey := tm.getCurrentNode(manager)
	currentNode := strings.Split(currentKey, Delimiter)[0]
	node, exists := tm.nodes.Get(currentNode)
	method, _ := ctx.Value("method").(string)
	if method == "GET" && exists && node.NodeType == Page {
		ctx = context.WithValue(ctx, "initial_node", currentNode)
		if manager.result != nil {
			task.Payload = manager.result.Payload
			tm.Logger().Debug("Merged previous result payload into task",
				logger.Field{Key: "taskID", Value: task.ID})
		}
	} else if next == "true" {
		nodes, err := tm.GetNextNodes(currentNode)
		if err != nil {
			tm.Logger().Error("Error retrieving next nodes",
				logger.Field{Key: "error", Value: err})
			return mq.Result{Error: err, Ctx: ctx}
		}
		if len(nodes) > 0 {
			ctx = context.WithValue(ctx, "initial_node", nodes[0].ID)
		}
	}

	// Merge any previous node result into the task payload
	if currentNodeResult, hasResult := manager.currentNodeResult.Get(currentKey); hasResult {
		var taskPayload, resultPayload map[string]any
		if err := json.Unmarshal(task.Payload, &taskPayload); err == nil {
			if err = json.Unmarshal(currentNodeResult.Payload, &resultPayload); err == nil {
				for key, val := range resultPayload {
					taskPayload[key] = val
				}
				if newPayload, err := json.Marshal(taskPayload); err != nil {
					tm.Logger().Error("Error marshalling merged payload",
						logger.Field{Key: "error", Value: err})
				} else {
					task.Payload = newPayload
					tm.Logger().Debug("Merged previous node result into task payload",
						logger.Field{Key: "taskID", Value: task.ID})
				}
			} else {
				tm.Logger().Error("Error unmarshalling current node result payload",
					logger.Field{Key: "error", Value: err})
			}
		} else {
			tm.Logger().Error("Error unmarshalling task payload",
				logger.Field{Key: "error", Value: err})
		}
	}

	firstNode, err := tm.parseInitialNode(ctx)
	if err != nil {
		tm.Logger().Error("Error parsing initial node", logger.Field{Key: "error", Value: err})
		return mq.Result{Error: err, Ctx: ctx}
	}
	node, _ = tm.nodes.Get(firstNode)
	task.Topic = firstNode
	ctx = context.WithValue(ctx, ContextIndex, "0")

	tm.Logger().Info("Dispatching task to TaskManager",
		logger.Field{Key: "firstNode", Value: firstNode},
		logger.Field{Key: "taskID", Value: task.ID})
	manager.ProcessTask(ctx, firstNode, task.Payload)

	// WAITING (no polling; wait for a result or ctx cancellation)
	waitForResult := func() mq.Result {
		select {
		case result := <-resultCh:
			tm.Logger().Info("Received task result",
				logger.Field{Key: "taskID", Value: task.ID})
			return result
		case <-ctx.Done():
			tm.Logger().Error("Task cancelled due to context",
				logger.Field{Key: "taskID", Value: task.ID})
			return mq.Result{
				Error: fmt.Errorf("context cancelled while waiting for task result: %w", ctx.Err()),
				Ctx:   ctx,
			}
		}
	}

	if tm.hasPageNode {
		tm.Logger().Info("Page node detected; pausing task until user processes HTML",
			logger.Field{Key: "taskID", Value: task.ID})
		return waitForResult()
	}

	// non-page flows also wait until a result is available or ctx cancelled
	return waitForResult()
}

func (tm *DAG) Process(ctx context.Context, payload []byte) mq.Result {
	var taskID string
	userCtx := form.UserContext(ctx)
	if val := userCtx.Get("task_id"); val != "" {
		taskID = val
	} else {
		taskID = mq.NewID()
	}
	rs := tm.ProcessTask(ctx, mq.NewTask(taskID, payload, "", mq.WithDAG(tm)))
	return rs
}

func (tm *DAG) Validate() error {
	report, hasCycle, err := tm.ClassifyEdges()
	if hasCycle || err != nil {
		tm.Error = err
		return err
	}
	tm.report = report

	// Build caches for next and previous nodes
	tm.nextNodesCache = &sync.Map{}
	tm.prevNodesCache = &sync.Map{}
	tm.nodes.ForEach(func(key string, node *Node) bool {
		var next []*Node
		for _, edge := range node.Edges {
			next = append(next, edge.To)
		}
		if conds, exists := tm.conditions[node.ID]; exists {
			for _, targetKey := range conds {
				if targetNode, ok := tm.nodes.Get(targetKey); ok {
					next = append(next, targetNode)
				}
			}
		}
		tm.nextNodesCache.Store(node.ID, next)
		return true
	})
	tm.nodes.ForEach(func(key string, _ *Node) bool {
		var prev []*Node
		tm.nodes.ForEach(func(_ string, n *Node) bool {
			for _, edge := range n.Edges {
				if edge.To.ID == key {
					prev = append(prev, n)
				}
			}
			if conds, exists := tm.conditions[n.ID]; exists {
				for _, targetKey := range conds {
					if targetKey == key {
						prev = append(prev, n)
					}
				}
			}
			return true
		})
		tm.prevNodesCache.Store(key, prev)
		return true
	})
	return nil
}

// New method to reset the DAG state.
func (tm *DAG) Reset() {
	// Stop all task managers.
	tm.taskManager.ForEach(func(k string, manager *TaskManager) bool {
		manager.Stop()
		return true
	})
	// Clear caches.
	tm.nextNodesCache = nil
	tm.prevNodesCache = nil

	// Clear task managers map
	tm.taskManager = memory.New[string, *TaskManager]()

	tm.Logger().Info("DAG has been reset")
}

// New method to cancel a running task.
func (tm *DAG) CancelTask(taskID string) error {
	manager, ok := tm.taskManager.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}
	// Stop the task manager to cancel the task.
	manager.Stop()
	tm.metrics.mu.Lock()
	tm.metrics.Cancelled++ // <-- update cancelled metric
	tm.metrics.mu.Unlock()
	return nil
}

// CleanupCompletedTasks removes completed task managers to prevent memory leaks
func (tm *DAG) CleanupCompletedTasks() {
	completedTasks := make([]string, 0)

	tm.taskManager.ForEach(func(taskID string, manager *TaskManager) bool {
		// Check if task manager has completed tasks
		hasActiveTasks := false
		manager.taskStates.ForEach(func(nodeID string, state *TaskState) bool {
			if state.Status == mq.Processing || state.Status == mq.Pending {
				hasActiveTasks = true
				return false // Stop iteration
			}
			return true
		})

		// If no active tasks, mark for cleanup
		if !hasActiveTasks {
			completedTasks = append(completedTasks, taskID)
		}

		return true
	})

	// Remove completed task managers
	for _, taskID := range completedTasks {
		if manager, exists := tm.taskManager.Get(taskID); exists {
			manager.Stop()
			tm.taskManager.Del(taskID)
			tm.Logger().Debug("Cleaned up completed task manager",
				logger.Field{Key: "taskID", Value: taskID})
		}
	}

	if len(completedTasks) > 0 {
		tm.Logger().Info("Cleaned up completed task managers",
			logger.Field{Key: "count", Value: len(completedTasks)})
	}
}

func (tm *DAG) GetReport() string {
	return tm.report
}

func (tm *DAG) Start(ctx context.Context, addr string) error {
	go func() {
		defer mq.RecoverPanic(mq.RecoverTitle)
		if err := tm.server.Start(ctx); err != nil {
			panic(err)
		}
	}()

	if !tm.server.SyncMode() {
		tm.nodes.ForEach(func(_ string, con *Node) bool {
			go func(con *Node) {
				defer mq.RecoverPanic(mq.RecoverTitle)
				limiter := rate.NewLimiter(rate.Every(1*time.Second), 1)
				for {
					err := con.processor.Consume(ctx)
					if err != nil {
						log.Printf("[ERROR] - Consumer %s failed to start: %v", con.ID, err)
					} else {
						log.Printf("[INFO] - Consumer %s started successfully", con.ID)
						break
					}
					limiter.Wait(ctx)
				}
			}(con)
			return true
		})
	}
	app := fiber.New()
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))
	tm.Handlers(app, "/")
	return app.Listen(addr)
}

func (tm *DAG) ScheduleTask(ctx context.Context, payload []byte, opts ...mq.SchedulerOption) mq.Result {
	if tm.scheduler == nil {
		return mq.Result{Error: errors.New("scheduler not defined"), Ctx: ctx}
	}
	var taskID string
	userCtx := form.UserContext(ctx)
	if val := userCtx.Get("task_id"); val != "" {
		taskID = val
	} else {
		taskID = mq.NewID()
	}
	t := mq.NewTask(taskID, payload, "", mq.WithDAG(tm))

	ctx = context.WithValue(ctx, "task_id", taskID)
	userContext := form.UserContext(ctx)
	next := userContext.Get("next")
	manager, ok := tm.taskManager.Get(taskID)
	resultCh := make(chan mq.Result, 1)
	if !ok {
		manager = NewTaskManager(tm, taskID, resultCh, tm.iteratorNodes.Clone(), tm.taskStorage)
		tm.taskManager.Set(taskID, manager)
	} else {
		manager.resultCh = resultCh
	}
	currentKey := tm.getCurrentNode(manager)
	currentNode := strings.Split(currentKey, Delimiter)[0]
	node, exists := tm.nodes.Get(currentNode)
	method, ok := ctx.Value("method").(string)
	if method == "GET" && exists && node.NodeType == Page {
		ctx = context.WithValue(ctx, "initial_node", currentNode)
	} else if next == "true" {
		nodes, err := tm.GetNextNodes(currentNode)
		if err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		if len(nodes) > 0 {
			ctx = context.WithValue(ctx, "initial_node", nodes[0].ID)
		}
	}
	if currentNodeResult, hasResult := manager.currentNodeResult.Get(currentKey); hasResult {
		var taskPayload, resultPayload map[string]any
		if err := json.Unmarshal(payload, &taskPayload); err == nil {
			if err = json.Unmarshal(currentNodeResult.Payload, &resultPayload); err == nil {
				for key, val := range resultPayload {
					taskPayload[key] = val
				}
				payload, _ = json.Marshal(taskPayload)
			}
		}
	}
	firstNode, err := tm.parseInitialNode(ctx)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	node, ok = tm.nodes.Get(firstNode)
	t.Topic = firstNode
	ctx = context.WithValue(ctx, ContextIndex, "0")

	// Update metrics for a new task.
	tm.metrics.mu.Lock()
	tm.metrics.NotStarted++
	tm.metrics.Queued++
	tm.metrics.mu.Unlock()

	headers, ok := mq.GetHeaders(ctx)
	ctxx := context.Background()
	if ok {
		ctxx = mq.SetHeaders(ctxx, headers.AsMap())
	}
	tm.scheduler.AddTask(ctxx, t, opts...)
	return mq.Result{CreatedAt: t.CreatedAt, TaskID: t.ID, Topic: t.Topic, Status: mq.Pending}
}

// GetStatus returns a summary of the DAG including node and task counts.
func (tm *DAG) GetStatus() map[string]any {
	status := make(map[string]any)
	// Count nodes
	nodeCount := 0
	tm.nodes.ForEach(func(_ string, _ *Node) bool {
		nodeCount++
		return true
	})
	status["nodes_count"] = nodeCount

	// Count tasks (if available, using taskManager's ForEach)
	taskCount := 0
	tm.taskManager.ForEach(func(_ string, _ *TaskManager) bool {
		taskCount++
		return true
	})
	status["task_count"] = taskCount

	return status
}

// GetEdgeCount returns the total number of edges
func (tm *DAG) GetEdgeCount() int {
	count := 0
	tm.nodes.ForEach(func(id string, node *Node) bool {
		count += len(node.Edges)
		return true
	})

	// Add conditional edges
	for _, conditions := range tm.conditions {
		count += len(conditions)
	}

	return count
}

// Enhanced Monitoring and Management Methods

// ValidateDAG validates the DAG structure using the enhanced validator
func (tm *DAG) ValidateDAG() error {
	if tm.validator == nil {
		return fmt.Errorf("validator not initialized")
	}
	return tm.validator.ValidateStructure()
}

// GetTopologicalOrder returns nodes in topological order
func (tm *DAG) GetTopologicalOrder() ([]string, error) {
	if tm.validator == nil {
		return nil, fmt.Errorf("validator not initialized")
	}
	return tm.validator.GetTopologicalOrder()
}

// GetCriticalPath returns the critical path of the DAG
func (tm *DAG) GetCriticalPath() ([]string, error) {
	if tm.validator == nil {
		return nil, fmt.Errorf("validator not initialized")
	}
	return tm.validator.GetCriticalPath()
}

// Batch Processing Methods

// SetBatchProcessingEnabled enables or disables batch processing
func (tm *DAG) SetBatchProcessingEnabled(enabled bool) {
	if tm.batchProcessor != nil && enabled {
		// Configure batch processor with processing function
		tm.batchProcessor.SetProcessFunc(func(tasks []*mq.Task) error {
			// Process tasks in batch
			for _, task := range tasks {
				tm.ProcessTask(context.Background(), task)
			}
			return nil
		})
	}
}

// Webhook Methods

// SetWebhookManager sets the webhook manager
func (tm *DAG) SetWebhookManager(manager *WebhookManager) {
	tm.webhookManager = manager
}

// AddWebhook adds a webhook configuration
func (tm *DAG) AddWebhook(event string, config WebhookConfig) {
	if tm.webhookManager != nil {
		tm.webhookManager.AddWebhook(event, config)
	}
}

// Performance Optimization Methods

// OptimizePerformance triggers performance optimization
func (tm *DAG) OptimizePerformance() error {
	if tm.performanceOptimizer != nil {
		return tm.performanceOptimizer.OptimizePerformance()
	}
	return fmt.Errorf("performance optimizer not initialized")
}

// Cleanup Methods

// StartCleanup starts the cleanup manager
func (tm *DAG) StartCleanup(ctx context.Context) {
	if tm.cleanupManager != nil {
		tm.cleanupManager.Start(ctx)
	}
}

// StopCleanup stops the cleanup manager
func (tm *DAG) StopCleanup() {
	if tm.cleanupManager != nil {
		tm.cleanupManager.Stop()
	}
}

// Enhanced Stop method with proper cleanup
func (tm *DAG) StopEnhanced(ctx context.Context) error {
	// Stop monitoring
	tm.StopMonitoring()

	// Stop cleanup manager
	tm.StopCleanup()

	// Stop batch processor
	if tm.batchProcessor != nil {
		tm.batchProcessor.Stop()
	}

	// Stop cache cleanup
	if tm.cache != nil {
		tm.cache.Stop()
	}

	// Flush activity logs
	if tm.activityLogger != nil {
		tm.activityLogger.Flush()
	}

	// Stop all task managers
	tm.taskManager.ForEach(func(taskID string, manager *TaskManager) bool {
		manager.Stop()
		return true
	})

	// Clear all caches
	tm.nextNodesCache = nil
	tm.prevNodesCache = nil

	// Stop underlying components
	return tm.Stop(ctx)
}

// GetPreviousPageNode returns the last page node that was executed before the current node
func (tm *DAG) GetPreviousPageNode(nodeID string) (*Node, error) {
	currentNode := strings.Split(nodeID, Delimiter)[0]
	// Check if current node exists
	_, exists := tm.nodes.Get(currentNode)
	if !exists {
		fmt.Println(tm.nodes.Keys())
		return nil, fmt.Errorf("current node %s not found", currentNode)
	}

	// Get topological order to determine execution sequence
	topologicalOrder, err := tm.GetTopologicalOrder()
	if err != nil {
		return nil, fmt.Errorf("failed to get topological order: %w", err)
	}

	// Find the index of the current node in topological order
	currentIndex := -1
	for i, nodeIDInOrder := range topologicalOrder {
		if nodeIDInOrder == currentNode {
			currentIndex = i
			break
		}
	}

	if currentIndex == -1 {
		return nil, fmt.Errorf("current node %s not found in topological order", currentNode)
	}

	// Iterate backwards from current node to find the last page node
	for i := currentIndex - 1; i >= 0; i-- {
		nodeIDInOrder := topologicalOrder[i]
		if node, ok := tm.nodes.Get(nodeIDInOrder); ok && node.NodeType == Page {
			return node, nil
		}
	}

	return nil, fmt.Errorf("no previous page node found")
}

// clearDownstreamNodesForSubDAGReset clears all downstream nodes when a subDAG resets
// This ensures that when a subDAG resets, the workflow doesn't continue with stale data
func (tm *DAG) clearDownstreamNodesForSubDAGReset(manager *TaskManager, subDAGNodeID string) {
	// Get all nodes that come after the subDAG node
	downstreamNodes := tm.getAllDownstreamNodes(subDAGNodeID)

	if tm.debug {
		tm.Logger().Info("Clearing downstream nodes for subDAG reset",
			logger.Field{Key: "subDAGNodeID", Value: subDAGNodeID},
			logger.Field{Key: "downstreamCount", Value: len(downstreamNodes)})
	}

	// Clear task states for all downstream nodes
	for _, downstreamNodeID := range downstreamNodes {
		if state, exists := manager.taskStates.Get(downstreamNodeID); exists {
			state.Status = mq.Pending
			state.UpdatedAt = time.Now()
			state.Result = mq.Result{} // Clear previous result
			if tm.debug {
				tm.Logger().Debug("Cleared task state for downstream node",
					logger.Field{Key: "nodeID", Value: downstreamNodeID})
			}
		}
		// Also clear any cached results for this node
		manager.currentNodeResult.Del(downstreamNodeID)
		// Clear any deferred tasks for this node
		manager.deferredTasks.ForEach(func(taskID string, tsk *task) bool {
			if strings.Split(tsk.nodeID, Delimiter)[0] == downstreamNodeID {
				manager.deferredTasks.Del(taskID)
				if tm.debug {
					tm.Logger().Debug("Cleared deferred task for downstream node",
						logger.Field{Key: "nodeID", Value: downstreamNodeID},
						logger.Field{Key: "taskID", Value: taskID})
				}
			}
			return true
		})
	}
}

// getAllDownstreamNodes returns all nodes that come after the given node in the workflow
func (tm *DAG) getAllDownstreamNodes(nodeID string) []string {
	visited := make(map[string]bool)
	var result []string

	// Use BFS to find all downstream nodes
	queue := []string{nodeID}
	visited[nodeID] = true

	for len(queue) > 0 {
		currentNodeID := queue[0]
		queue = queue[1:]

		// Get all nodes that this node points to
		if node, exists := tm.nodes.Get(currentNodeID); exists {
			for _, edge := range node.Edges {
				if edge.Type == Simple || edge.Type == Iterator {
					targetNodeID := edge.To.ID
					if !visited[targetNodeID] {
						visited[targetNodeID] = true
						result = append(result, targetNodeID)
						queue = append(queue, targetNodeID)
					}
				}
			}
		}
	}

	return result
}
