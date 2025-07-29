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

type Node struct {
	processor mq.Processor
	Label     string
	ID        string
	Edges     []Edge
	NodeType  NodeType
	isReady   bool
	Timeout   time.Duration // ...new field for node-level timeout...
}

// SetTimeout allows setting a maximum processing duration for the node.
func (n *Node) SetTimeout(d time.Duration) {
	n.Timeout = d
}

type Edge struct {
	From  *Node
	To    *Node
	Label string
	Type  EdgeType
}

type DAG struct {
	nodes                    storage.IMap[string, *Node]
	taskManager              storage.IMap[string, *TaskManager]
	iteratorNodes            storage.IMap[string, []Edge]
	Error                    error
	conditions               map[string]map[string]string
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
	nextNodesCache           map[string][]*Node
	prevNodesCache           map[string][]*Node
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

	// Circuit breakers per node
	circuitBreakers   map[string]*CircuitBreaker
	circuitBreakersMu sync.RWMutex
}

// SetPreProcessHook configures a function to be called before each node is processed.
func (tm *DAG) SetPreProcessHook(hook func(ctx context.Context, node *Node, taskID string, payload json.RawMessage) context.Context) {
	tm.PreProcessHook = hook
}

// SetPostProcessHook configures a function to be called after each node is processed.
func (tm *DAG) SetPostProcessHook(hook func(ctx context.Context, node *Node, taskID string, result mq.Result)) {
	tm.PostProcessHook = hook
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
		nextNodesCache:  make(map[string][]*Node),
		prevNodesCache:  make(map[string][]*Node),
	}

	opts = append(opts,
		mq.WithCallback(d.onTaskCallback),
		mq.WithConsumerOnSubscribe(d.onConsumerJoin),
		mq.WithConsumerOnClose(d.onConsumerClose),
	)
	d.server = mq.NewBroker(opts...)

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
	d.Logger().Info("Updating task metrics",
		logger.Field{Key: "taskID", Value: taskID},
		logger.Field{Key: "lastExecuted", Value: time.Now()},
		logger.Field{Key: "duration", Value: duration},
		logger.Field{Key: "success", Value: result.Status},
	)
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

func (tm *DAG) SetStartNode(node string) {
	tm.startNode = node
}

func (tm *DAG) GetStartNode() string {
	return tm.startNode
}

func (tm *DAG) AddCondition(fromNode string, conditions map[string]string) *DAG {
	tm.conditions[fromNode] = conditions
	return tm
}

func (tm *DAG) AddNode(nodeType NodeType, name, nodeID string, handler mq.Processor, startNode ...bool) *DAG {
	if tm.Error != nil {
		return tm
	}
	con := mq.NewConsumer(nodeID, nodeID, handler.ProcessTask, mq.WithBrokerURL(tm.server.Options().BrokerAddr()))
	n := &Node{
		Label:     name,
		ID:        nodeID,
		NodeType:  nodeType,
		processor: con,
	}
	if tm.server != nil && tm.server.SyncMode() {
		n.isReady = true
	}
	tm.nodes.Set(nodeID, n)
	if len(startNode) > 0 && startNode[0] {
		tm.startNode = nodeID
	}
	if nodeType == Page && !tm.hasPageNode {
		tm.hasPageNode = true
	}
	return tm
}

func (tm *DAG) AddDeferredNode(nodeType NodeType, name, key string, firstNode ...bool) error {
	if tm.server.SyncMode() {
		return fmt.Errorf("DAG cannot have deferred node in Sync Mode")
	}
	tm.nodes.Set(key, &Node{
		Label:    name,
		ID:       key,
		NodeType: nodeType,
	})
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return nil
}

func (tm *DAG) IsReady() bool {
	var isReady bool
	tm.nodes.ForEach(func(_ string, n *Node) bool {
		if !n.isReady {
			return false
		}
		isReady = true
		return true
	})
	return isReady
}

func (tm *DAG) AddEdge(edgeType EdgeType, label, from string, targets ...string) *DAG {
	if tm.Error != nil {
		return tm
	}
	if edgeType == Iterator {
		tm.iteratorNodes.Set(from, []Edge{})
	}
	node, ok := tm.nodes.Get(from)
	if !ok {
		tm.Error = fmt.Errorf("node not found %s", from)
		return tm
	}
	for _, target := range targets {
		if targetNode, ok := tm.nodes.Get(target); ok {
			edge := Edge{From: node, To: targetNode, Type: edgeType, Label: label}
			node.Edges = append(node.Edges, edge)
			if edgeType != Iterator {
				if edges, ok := tm.iteratorNodes.Get(node.ID); ok {
					edges = append(edges, edge)
					tm.iteratorNodes.Set(node.ID, edges)
				}
			}
		}
	}
	return tm
}

func (tm *DAG) getCurrentNode(manager *TaskManager) string {
	if manager.currentNodePayload.Size() == 0 {
		return ""
	}
	return manager.currentNodePayload.Keys()[0]
}

func (tm *DAG) Logger() logger.Logger {
	return tm.server.Options().Logger()
}

func (tm *DAG) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Enhanced processing with monitoring and rate limiting
	startTime := time.Now()

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

	// Trigger webhooks if configured
	if tm.webhookManager != nil {
		event := WebhookEvent{
			Type:      "task_completed",
			TaskID:    task.ID,
			NodeID:    task.Topic,
			Timestamp: time.Now(),
			Data: map[string]interface{}{
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
	manager, ok := tm.taskManager.Get(task.ID)
	resultCh := make(chan mq.Result, 1)
	if !ok {
		manager = NewTaskManager(tm, task.ID, resultCh, tm.iteratorNodes.Clone())
		tm.taskManager.Set(task.ID, manager)
		tm.Logger().Info("Processing task",
			logger.Field{Key: "taskID", Value: task.ID},
			logger.Field{Key: "phase", Value: "start"},
			logger.Field{Key: "timestamp", Value: time.Now()},
		)
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
	method, ok := ctx.Value("method").(string)
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
	if currentNodeResult, hasResult := manager.currentNodeResult.Get(currentKey); hasResult {
		var taskPayload, resultPayload map[string]any
		if err := json.Unmarshal(task.Payload, &taskPayload); err == nil {
			if err = json.Unmarshal(currentNodeResult.Payload, &resultPayload); err == nil {
				for key, val := range resultPayload {
					taskPayload[key] = val
				}
				task.Payload, _ = json.Marshal(taskPayload)
			}
		}
	}
	firstNode, err := tm.parseInitialNode(ctx)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	node, ok = tm.nodes.Get(firstNode)
	task.Topic = firstNode
	ctx = context.WithValue(ctx, ContextIndex, "0")
	manager.ProcessTask(ctx, firstNode, task.Payload)
	if tm.hasPageNode {
		return <-resultCh
	}
	select {
	case result := <-resultCh:
		return result
	case <-time.After(30 * time.Second):
		return mq.Result{
			Error: fmt.Errorf("timeout waiting for task result"),
			Ctx:   ctx,
		}
	}
}

func (tm *DAG) ProcessTaskNew(ctx context.Context, task *mq.Task) mq.Result {
	ctx = context.WithValue(ctx, "task_id", task.ID)
	userContext := form.UserContext(ctx)
	next := userContext.Get("next")
	manager, ok := tm.taskManager.Get(task.ID)
	resultCh := make(chan mq.Result, 1)
	if !ok {
		manager = NewTaskManager(tm, task.ID, resultCh, tm.iteratorNodes.Clone())
		tm.taskManager.Set(task.ID, manager)
		tm.Logger().Info("Processing task",
			logger.Field{Key: "taskID", Value: task.ID},
			logger.Field{Key: "phase", Value: "start"},
			logger.Field{Key: "timestamp", Value: time.Now()},
		)
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

	// Parse the initial node from context.
	firstNode, err := tm.parseInitialNode(ctx)
	if err != nil {
		tm.Logger().Error("Error parsing initial node", logger.Field{Key: "error", Value: err})
		return mq.Result{Error: err, Ctx: ctx}
	}
	node, ok = tm.nodes.Get(firstNode)
	task.Topic = firstNode
	ctx = context.WithValue(ctx, ContextIndex, "0")

	// Dispatch the task to the TaskManager.
	tm.Logger().Info("Dispatching task to TaskManager",
		logger.Field{Key: "firstNode", Value: firstNode},
		logger.Field{Key: "taskID", Value: task.ID})
	manager.ProcessTask(ctx, firstNode, task.Payload)

	// Wait for task result. If there's an HTML page node, the task will pause.
	var result mq.Result
	if tm.hasPageNode {
		if !result.Last {
			tm.Logger().Info("Page node detected; pausing task until user processes HTML",
				logger.Field{Key: "taskID", Value: task.ID})
		}
		result = <-resultCh
	} else {
		select {
		case result = <-resultCh:
			tm.Logger().Info("Received task result",
				logger.Field{Key: "taskID", Value: task.ID})
		case <-time.After(30 * time.Second):
			result = mq.Result{
				Error: fmt.Errorf("timeout waiting for task result"),
				Ctx:   ctx,
			}
			tm.Logger().Error("Task result timeout",
				logger.Field{Key: "taskID", Value: task.ID})
		}
	}

	if result.Last {
		tm.Logger().Info("Task completed",
			logger.Field{Key: "taskID", Value: task.ID},
			logger.Field{Key: "lastExecuted", Value: time.Now()},
			logger.Field{Key: "success", Value: result.Error == nil},
		)
	}
	return result
}

func (tm *DAG) Process(ctx context.Context, payload []byte) mq.Result {
	var taskID string
	userCtx := form.UserContext(ctx)
	if val := userCtx.Get("task_id"); val != "" {
		taskID = val
	} else {
		taskID = mq.NewID()
	}
	return tm.ProcessTask(ctx, mq.NewTask(taskID, payload, "", mq.WithDAG(tm)))
}

func (tm *DAG) Validate() error {
	report, hasCycle, err := tm.ClassifyEdges()
	if hasCycle || err != nil {
		tm.Error = err
		return err
	}
	tm.report = report

	// Build caches for next and previous nodes
	tm.nextNodesCache = make(map[string][]*Node)
	tm.prevNodesCache = make(map[string][]*Node)
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
		tm.nextNodesCache[node.ID] = next
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
		tm.prevNodesCache[key] = prev
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

func (tm *DAG) GetReport() string {
	return tm.report
}

func (tm *DAG) AddDAGNode(nodeType NodeType, name string, key string, dag *DAG, firstNode ...bool) *DAG {
	dag.AssignTopic(key)
	tm.nodes.Set(key, &Node{
		Label:     name,
		ID:        key,
		NodeType:  nodeType,
		processor: dag,
		isReady:   true,
	})
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return tm
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
		manager = NewTaskManager(tm, taskID, resultCh, tm.iteratorNodes.Clone())
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
func (tm *DAG) GetStatus() map[string]interface{} {
	status := make(map[string]interface{})
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

// RemoveNode removes the node with the given nodeID and adjusts the edges.
// For example, if A -> B and B -> C exist and B is removed, a new edge A -> C is created.
func (tm *DAG) RemoveNode(nodeID string) error {
	node, exists := tm.nodes.Get(nodeID)
	if !exists {
		return fmt.Errorf("node %s does not exist", nodeID)
	}
	// Collect incoming edges (from nodes pointing to the removed node).
	var incomingEdges []Edge
	tm.nodes.ForEach(func(_ string, n *Node) bool {
		for _, edge := range n.Edges {
			if edge.To.ID == nodeID {
				incomingEdges = append(incomingEdges, Edge{
					From:  n,
					To:    node,
					Label: edge.Label,
					Type:  edge.Type,
				})
			}
		}
		return true
	})
	// Get outgoing edges from the node being removed.
	outgoingEdges := node.Edges
	// For each incoming edge and each outgoing edge, create a new edge A -> C.
	for _, inEdge := range incomingEdges {
		for _, outEdge := range outgoingEdges {
			// Avoid creating self-loop.
			if inEdge.From.ID != outEdge.To.ID {
				newEdge := Edge{
					From:  inEdge.From,
					To:    outEdge.To,
					Label: inEdge.Label + "_" + outEdge.Label,
					Type:  Simple, // Use Simple edge type for adjusted flows.
				}
				// Append new edge if one doesn't already exist.
				existsNewEdge := false
				for _, e := range inEdge.From.Edges {
					if e.To.ID == newEdge.To.ID {
						existsNewEdge = true
						break
					}
				}
				if !existsNewEdge {
					inEdge.From.Edges = append(inEdge.From.Edges, newEdge)
				}
			}
		}
	}
	// Remove all edges that are connected to the removed node.
	tm.nodes.ForEach(func(_ string, n *Node) bool {
		var updatedEdges []Edge
		for _, edge := range n.Edges {
			if edge.To.ID != nodeID {
				updatedEdges = append(updatedEdges, edge)
			}
		}
		n.Edges = updatedEdges
		return true
	})
	// Remove any conditions referencing the removed node.
	for key, cond := range tm.conditions {
		if key == nodeID {
			delete(tm.conditions, key)
		} else {
			for when, target := range cond {
				if target == nodeID {
					delete(cond, when)
				}
			}
		}
	}
	// Remove the node from the DAG.
	tm.nodes.Del(nodeID)
	// Invalidate caches.
	tm.nextNodesCache = nil
	tm.prevNodesCache = nil
	tm.Logger().Info("Node removed and edges adjusted",
		logger.Field{Key: "removed_node", Value: nodeID})
	return nil
}

// getOrCreateCircuitBreaker gets or creates a circuit breaker for a node
func (tm *DAG) getOrCreateCircuitBreaker(nodeID string) *CircuitBreaker {
	tm.circuitBreakersMu.RLock()
	cb, exists := tm.circuitBreakers[nodeID]
	tm.circuitBreakersMu.RUnlock()

	if exists {
		return cb
	}

	tm.circuitBreakersMu.Lock()
	defer tm.circuitBreakersMu.Unlock()

	// Double-check after acquiring write lock
	if cb, exists := tm.circuitBreakers[nodeID]; exists {
		return cb
	}

	// Create new circuit breaker with default config
	config := &CircuitBreakerConfig{
		FailureThreshold: 5,
		ResetTimeout:     30 * time.Second,
		HalfOpenMaxCalls: 3,
	}

	cb = NewCircuitBreaker(config, tm.Logger())
	tm.circuitBreakers[nodeID] = cb

	return cb
}

// Enhanced DAG methods for new features

// ValidateDAG validates the DAG structure
func (tm *DAG) ValidateDAG() error {
	if tm.validator == nil {
		return fmt.Errorf("validator not initialized")
	}
	return tm.validator.ValidateStructure()
}

// StartMonitoring starts DAG monitoring
func (tm *DAG) StartMonitoring(ctx context.Context) {
	if tm.monitor != nil {
		tm.monitor.Start(ctx)
	}
	if tm.cleanupManager != nil {
		tm.cleanupManager.Start(ctx)
	}
}

// StopMonitoring stops DAG monitoring
func (tm *DAG) StopMonitoring() {
	if tm.monitor != nil {
		tm.monitor.Stop()
	}
	if tm.cleanupManager != nil {
		tm.cleanupManager.Stop()
	}
	if tm.cache != nil {
		tm.cache.Stop()
	}
	if tm.batchProcessor != nil {
		tm.batchProcessor.Stop()
	}
}

// SetRateLimit sets rate limit for a node
func (tm *DAG) SetRateLimit(nodeID string, requestsPerSecond float64, burst int) {
	if tm.rateLimiter != nil {
		tm.rateLimiter.SetNodeLimit(nodeID, requestsPerSecond, burst)
	}
}

// SetWebhookManager sets the webhook manager
func (tm *DAG) SetWebhookManager(webhookManager *WebhookManager) {
	tm.webhookManager = webhookManager
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
	if tm.monitor != nil {
		return tm.monitor.metrics.GetNodeStats(nodeID)
	}
	return nil
}

// OptimizePerformance runs performance optimization
func (tm *DAG) OptimizePerformance() error {
	if tm.performanceOptimizer != nil {
		return tm.performanceOptimizer.OptimizePerformance()
	}
	return fmt.Errorf("performance optimizer not initialized")
}

// BeginTransaction starts a new transaction for task execution
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

// GetTopologicalOrder returns nodes in topological order
func (tm *DAG) GetTopologicalOrder() ([]string, error) {
	if tm.validator != nil {
		return tm.validator.GetTopologicalOrder()
	}
	return nil, fmt.Errorf("validator not initialized")
}

// GetCriticalPath finds the longest path in the DAG
func (tm *DAG) GetCriticalPath() ([]string, error) {
	if tm.validator != nil {
		return tm.validator.GetCriticalPath()
	}
	return nil, fmt.Errorf("validator not initialized")
}

// GetDAGStatistics returns comprehensive DAG statistics
func (tm *DAG) GetDAGStatistics() map[string]interface{} {
	if tm.validator != nil {
		return tm.validator.GetNodeStatistics()
	}
	return make(map[string]interface{})
}

// SetRetryConfig sets retry configuration for the DAG
func (tm *DAG) SetRetryConfig(config *RetryConfig) {
	if tm.retryManager != nil {
		tm.retryManager.config = config
	}
}

// AddNodeWithRetry adds a node with retry capabilities
func (tm *DAG) AddNodeWithRetry(nodeType NodeType, name, nodeID string, handler mq.Processor, retryConfig *RetryConfig, startNode ...bool) *DAG {
	if tm.Error != nil {
		return tm
	}

	// Wrap handler with retry logic if config provided
	if retryConfig != nil {
		handler = NewRetryableProcessor(handler, retryConfig, tm.Logger())
	}

	return tm.AddNode(nodeType, name, nodeID, handler, startNode...)
}

// SetAlertThresholds configures monitoring alert thresholds
func (tm *DAG) SetAlertThresholds(thresholds *AlertThresholds) {
	if tm.monitor != nil {
		tm.monitor.SetAlertThresholds(thresholds)
	}
}

// AddAlertHandler adds an alert handler for monitoring
func (tm *DAG) AddAlertHandler(handler AlertHandler) {
	if tm.monitor != nil {
		tm.monitor.AddAlertHandler(handler)
	}
}

// UpdateConfiguration updates the DAG configuration
func (tm *DAG) UpdateConfiguration(config *DAGConfig) error {
	if tm.configManager != nil {
		return tm.configManager.UpdateConfig(config)
	}
	return fmt.Errorf("config manager not initialized")
}

// GetConfiguration returns the current DAG configuration
func (tm *DAG) GetConfiguration() *DAGConfig {
	if tm.configManager != nil {
		return tm.configManager.GetConfig()
	}
	return DefaultDAGConfig()
}
