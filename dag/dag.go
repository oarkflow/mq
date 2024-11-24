package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"golang.org/x/time/rate"

	"github.com/oarkflow/mq/sio"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

type Node struct {
	NodeType  NodeType
	Label     string
	ID        string
	Edges     []Edge
	processor mq.Processor
	isReady   bool
}

type Edge struct {
	From  *Node
	To    *Node
	Type  EdgeType
	Label string
}

type DAG struct {
	server                   *mq.Broker
	consumer                 *mq.Consumer
	nodes                    storage.IMap[string, *Node]
	taskManager              storage.IMap[string, *TaskManager]
	iteratorNodes            storage.IMap[string, []Edge]
	finalResult              func(taskID string, result mq.Result)
	pool                     *mq.Pool
	name                     string
	key                      string
	startNode                string
	opts                     []mq.Option
	conditions               map[string]map[string]string
	consumerTopic            string
	hasPageNode              bool
	reportNodeResultCallback func(mq.Result)
	Error                    error
	Notifier                 *sio.Server
	paused                   bool
	report                   string
}

func NewDAG(name, key string, finalResultCallback func(taskID string, result mq.Result), opts ...mq.Option) *DAG {
	callback := func(ctx context.Context, result mq.Result) error { return nil }
	d := &DAG{
		name:          name,
		key:           key,
		nodes:         memory.New[string, *Node](),
		taskManager:   memory.New[string, *TaskManager](),
		iteratorNodes: memory.New[string, []Edge](),
		conditions:    make(map[string]map[string]string),
		finalResult:   finalResultCallback,
	}
	opts = append(opts,
		mq.WithCallback(d.onTaskCallback),
		mq.WithConsumerOnSubscribe(d.onConsumerJoin),
		mq.WithConsumerOnClose(d.onConsumerClose),
	)
	d.server = mq.NewBroker(opts...)
	d.opts = opts
	options := d.server.Options()
	d.pool = mq.NewPool(
		options.NumOfWorkers(),
		mq.WithTaskQueueSize(options.QueueSize()),
		mq.WithMaxMemoryLoad(options.MaxMemoryLoad()),
		mq.WithHandler(d.ProcessTask),
		mq.WithPoolCallback(callback),
		mq.WithTaskStorage(options.Storage()),
	)
	d.pool.Start(d.server.Options().NumOfWorkers())
	return d
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
	con := mq.NewConsumer(nodeID, nodeID, handler.ProcessTask)
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

func (tm *DAG) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	ctx = context.WithValue(ctx, "task_id", task.ID)
	userContext := UserContext(ctx)
	next := userContext.Get("next")
	manager, ok := tm.taskManager.Get(task.ID)
	resultCh := make(chan mq.Result, 1)
	if !ok {
		manager = NewTaskManager(tm, task.ID, resultCh, tm.iteratorNodes.Clone())
		tm.taskManager.Set(task.ID, manager)
	} else {
		manager.resultCh = resultCh
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
	// Timeout handling
	select {
	case result := <-resultCh:
		return result
	case <-time.After(30 * time.Second): // Set a timeout duration
		return mq.Result{
			Error: fmt.Errorf("timeout waiting for task result"),
			Ctx:   ctx,
		}
	}
}

func (tm *DAG) Process(ctx context.Context, payload []byte) mq.Result {
	var taskID string
	userCtx := UserContext(ctx)
	if val := userCtx.Get("task_id"); val != "" {
		taskID = val
	} else {
		taskID = mq.NewID()
	}
	return tm.ProcessTask(ctx, mq.NewTask(taskID, payload, ""))
}

func (tm *DAG) Validate() error {
	report, hasCycle, err := tm.ClassifyEdges()
	if hasCycle || err != nil {
		tm.Error = err
		return err
	}
	tm.report = report
	return nil
}

func (tm *DAG) GetReport() string {
	return tm.report
}

func (tm *DAG) AddDAGNode(name string, key string, dag *DAG, firstNode ...bool) *DAG {
	dag.AssignTopic(key)
	tm.nodes.Set(key, &Node{
		Label:     name,
		ID:        key,
		processor: dag,
		isReady:   true,
	})
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return tm
}

func (tm *DAG) Start(ctx context.Context, addr string) error {
	// Start the server in a separate goroutine
	go func() {
		defer mq.RecoverPanic(mq.RecoverTitle)
		if err := tm.server.Start(ctx); err != nil {
			panic(err)
		}
	}()

	// Start the node consumers if not in sync mode
	if !tm.server.SyncMode() {
		tm.nodes.ForEach(func(_ string, con *Node) bool {
			go func(con *Node) {
				defer mq.RecoverPanic(mq.RecoverTitle)
				limiter := rate.NewLimiter(rate.Every(1*time.Second), 1) // Retry every second
				for {
					err := con.processor.Consume(ctx)
					if err != nil {
						log.Printf("[ERROR] - Consumer %s failed to start: %v", con.ID, err)
					} else {
						log.Printf("[INFO] - Consumer %s started successfully", con.ID)
						break
					}
					limiter.Wait(ctx) // Wait with rate limiting before retrying
				}
			}(con)
			return true
		})
	}
	app := fiber.New()
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))
	tm.Handlers(app)
	return app.Listen(addr)
}

func (tm *DAG) ScheduleTask(ctx context.Context, payload []byte, opts ...mq.SchedulerOption) mq.Result {
	var taskID string
	userCtx := UserContext(ctx)
	if val := userCtx.Get("task_id"); val != "" {
		taskID = val
	} else {
		taskID = mq.NewID()
	}
	t := mq.NewTask(taskID, payload, "")

	ctx = context.WithValue(ctx, "task_id", taskID)
	userContext := UserContext(ctx)
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

	headers, ok := mq.GetHeaders(ctx)
	ctxx := context.Background()
	if ok {
		ctxx = mq.SetHeaders(ctxx, headers.AsMap())
	}
	tm.pool.Scheduler().AddTask(ctxx, t, opts...)
	return mq.Result{CreatedAt: t.CreatedAt, TaskID: t.ID, Topic: t.Topic, Status: "PENDING"}
}
