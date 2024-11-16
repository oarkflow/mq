package dag

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"

	"github.com/oarkflow/mq/sio"

	"golang.org/x/time/rate"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/metrics"
)

type EdgeType int

func (c EdgeType) IsValid() bool { return c >= Simple && c <= Iterator }

const (
	Simple EdgeType = iota
	Iterator
)

type NodeType int

func (c NodeType) IsValid() bool { return c >= Process && c <= Page }

const (
	Process NodeType = iota
	Page
)

type Node struct {
	processor mq.Processor
	Name      string
	Type      NodeType
	Key       string
	Edges     []Edge
	isReady   bool
}

func (n *Node) ProcessTask(ctx context.Context, msg *mq.Task) mq.Result {
	return n.processor.ProcessTask(ctx, msg)
}

func (n *Node) Close() error {
	return n.processor.Close()
}

type Edge struct {
	Label string
	From  *Node
	To    []*Node
	Type  EdgeType
}

type (
	FromNode string
	When     string
	Then     string
)

type DAG struct {
	server                   *mq.Broker
	consumer                 *mq.Consumer
	taskContext              storage.IMap[string, *TaskManager]
	nodes                    map[string]*Node
	iteratorNodes            storage.IMap[string, []Edge]
	conditions               map[FromNode]map[When]Then
	pool                     *mq.Pool
	taskCleanupCh            chan string
	name                     string
	key                      string
	startNode                string
	consumerTopic            string
	opts                     []mq.Option
	reportNodeResultCallback func(mq.Result)
	Notifier                 *sio.Server
	paused                   bool
	Error                    error
	report                   string
	index                    string
}

func (tm *DAG) SetKey(key string) {
	tm.key = key
}

func (tm *DAG) ReportNodeResult(callback func(mq.Result)) {
	tm.reportNodeResultCallback = callback
}

func (tm *DAG) GetType() string {
	return tm.key
}

func (tm *DAG) listenForTaskCleanup() {
	for taskID := range tm.taskCleanupCh {
		if tm.server.Options().CleanTaskOnComplete() {
			tm.taskCleanup(taskID)
		}
	}
}

func (tm *DAG) taskCleanup(taskID string) {
	tm.taskContext.Del(taskID)
	log.Printf("DAG - Task %s cleaned up", taskID)
}

func (tm *DAG) Consume(ctx context.Context) error {
	if tm.consumer != nil {
		tm.server.Options().SetSyncMode(true)
		return tm.consumer.Consume(ctx)
	}
	return nil
}

func (tm *DAG) Stop(ctx context.Context) error {
	for _, n := range tm.nodes {
		err := n.processor.Stop(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tm *DAG) GetKey() string {
	return tm.key
}

func (tm *DAG) AssignTopic(topic string) {
	tm.consumer = mq.NewConsumer(topic, topic, tm.ProcessTask, mq.WithRespondPendingResult(false), mq.WithBrokerURL(tm.server.URL()))
	tm.consumerTopic = topic
}

func NewDAG(name, key string, opts ...mq.Option) *DAG {
	callback := func(ctx context.Context, result mq.Result) error { return nil }
	d := &DAG{
		name:          name,
		key:           key,
		nodes:         make(map[string]*Node),
		iteratorNodes: memory.New[string, []Edge](),
		taskContext:   memory.New[string, *TaskManager](),
		conditions:    make(map[FromNode]map[When]Then),
		taskCleanupCh: make(chan string),
	}
	opts = append(opts, mq.WithCallback(d.onTaskCallback), mq.WithConsumerOnSubscribe(d.onConsumerJoin), mq.WithConsumerOnClose(d.onConsumerClose))
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
	go d.listenForTaskCleanup()
	return d
}

func (tm *DAG) callbackToConsumer(ctx context.Context, result mq.Result) {
	if tm.consumer != nil {
		result.Topic = tm.consumerTopic
		if tm.consumer.Conn() == nil {
			tm.onTaskCallback(ctx, result)
		} else {
			tm.consumer.OnResponse(ctx, result)
		}
	}
}

func (tm *DAG) onTaskCallback(ctx context.Context, result mq.Result) mq.Result {
	if taskContext, ok := tm.taskContext.Get(result.TaskID); ok && result.Topic != "" {
		return taskContext.handleNextTask(ctx, result)
	}
	return mq.Result{}
}

func (tm *DAG) onConsumerJoin(_ context.Context, topic, _ string) {
	if node, ok := tm.nodes[topic]; ok {
		log.Printf("DAG - CONSUMER ~> ready on %s", topic)
		node.isReady = true
	}
}

func (tm *DAG) onConsumerClose(_ context.Context, topic, _ string) {
	if node, ok := tm.nodes[topic]; ok {
		log.Printf("DAG - CONSUMER ~> down on %s", topic)
		node.isReady = false
	}
}

func (tm *DAG) SetStartNode(node string) {
	tm.startNode = node
}

func (tm *DAG) GetStartNode() string {
	return tm.startNode
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
		for _, con := range tm.nodes {
			go func(con *Node) {
				defer mq.RecoverPanic(mq.RecoverTitle)
				limiter := rate.NewLimiter(rate.Every(1*time.Second), 1) // Retry every second
				for {
					err := con.processor.Consume(ctx)
					if err != nil {
						log.Printf("[ERROR] - Consumer %s failed to start: %v", con.Key, err)
					} else {
						log.Printf("[INFO] - Consumer %s started successfully", con.Key)
						break
					}
					limiter.Wait(ctx) // Wait with rate limiting before retrying
				}
			}(con)
		}
	}
	log.Printf("DAG - HTTP_SERVER ~> started on http://localhost%s", addr)
	tm.Handlers()
	config := tm.server.TLSConfig()
	if config.UseTLS {
		return http.ListenAndServeTLS(addr, config.CertPath, config.KeyPath, nil)
	}
	return http.ListenAndServe(addr, nil)
}

func (tm *DAG) AddDAGNode(name string, key string, dag *DAG, firstNode ...bool) *DAG {
	dag.AssignTopic(key)
	tm.nodes[key] = &Node{
		Name:      name,
		Key:       key,
		processor: dag,
		isReady:   true,
	}
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return tm
}

func (tm *DAG) AddNode(name, key string, handler mq.Processor, firstNode ...bool) *DAG {
	con := mq.NewConsumer(key, key, handler.ProcessTask, tm.opts...)
	n := &Node{
		Name:      name,
		Key:       key,
		processor: con,
	}
	if handler.GetType() == "page" {
		n.Type = Page
	}
	if tm.server.SyncMode() {
		n.isReady = true
	}
	tm.nodes[key] = n
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return tm
}

func (tm *DAG) AddDeferredNode(name, key string, firstNode ...bool) error {
	if tm.server.SyncMode() {
		return fmt.Errorf("DAG cannot have deferred node in Sync Mode")
	}
	tm.nodes[key] = &Node{
		Name: name,
		Key:  key,
	}
	if len(firstNode) > 0 && firstNode[0] {
		tm.startNode = key
	}
	return nil
}

func (tm *DAG) IsReady() bool {
	for _, node := range tm.nodes {
		if !node.isReady {
			return false
		}
	}
	return true
}

func (tm *DAG) AddCondition(fromNode FromNode, conditions map[When]Then) *DAG {
	tm.conditions[fromNode] = conditions
	return tm
}

func (tm *DAG) AddIterator(label, from string, targets ...string) *DAG {
	tm.Error = tm.addEdge(Iterator, label, from, targets...)
	tm.iteratorNodes.Set(from, []Edge{})
	return tm
}

func (tm *DAG) AddEdge(label, from string, targets ...string) *DAG {
	tm.Error = tm.addEdge(Simple, label, from, targets...)
	return tm
}

func (tm *DAG) addEdge(edgeType EdgeType, label, from string, targets ...string) error {
	fromNode, ok := tm.nodes[from]
	if !ok {
		return fmt.Errorf("Error: 'from' node %s does not exist\n", from)
	}
	var nodes []*Node
	for _, target := range targets {
		toNode, ok := tm.nodes[target]
		if !ok {
			return fmt.Errorf("Error: 'from' node %s does not exist\n", target)
		}
		nodes = append(nodes, toNode)
	}
	edge := Edge{From: fromNode, To: nodes, Type: edgeType, Label: label}
	fromNode.Edges = append(fromNode.Edges, edge)
	if edgeType != Iterator {
		if edges, ok := tm.iteratorNodes.Get(fromNode.Key); ok {
			edges = append(edges, edge)
			tm.iteratorNodes.Set(fromNode.Key, edges)
		}
	}
	return nil
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

func (tm *DAG) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	if task.ID == "" {
		task.ID = mq.NewID()
	}
	if index, ok := mq.GetHeader(ctx, "index"); ok {
		tm.index = index
	}
	manager, exists := tm.taskContext.Get(task.ID)
	if !exists {
		manager = NewTaskManager(tm, task.ID, tm.iteratorNodes)
		manager.createdAt = task.CreatedAt
		tm.taskContext.Set(task.ID, manager)
	}

	if tm.consumer != nil {
		initialNode, err := tm.parseInitialNode(ctx)
		if err != nil {
			metrics.TasksErrors.WithLabelValues("unknown").Inc() // Increase error count
			return mq.Result{Error: err}
		}
		task.Topic = initialNode
	}
	if manager.topic != "" {
		task.Topic = manager.topic
		canNext := CanNextNode(ctx)
		if canNext != "" {
			if n, ok := tm.nodes[task.Topic]; ok {
				if len(n.Edges) > 0 {
					task.Topic = n.Edges[0].To[0].Key
				}
			}
		} else {
		}
	}
	result := manager.processTask(ctx, task.Topic, task.Payload)
	if result.Ctx != nil && tm.index != "" {
		result.Ctx = mq.SetHeaders(result.Ctx, map[string]string{"index": tm.index})
	}
	if result.Error != nil {
		metrics.TasksErrors.WithLabelValues(task.Topic).Inc() // Increase error count
	} else {
		metrics.TasksProcessed.WithLabelValues("success").Inc() // Increase processed task count
	}
	return result
}

func (tm *DAG) check(ctx context.Context, payload []byte) (context.Context, *mq.Task, error) {
	if tm.paused {
		return ctx, nil, fmt.Errorf("unable to process task, error: DAG is not accepting any task")
	}
	if !tm.IsReady() {
		return ctx, nil, fmt.Errorf("unable to process task, error: DAG is not ready yet")
	}
	initialNode, err := tm.parseInitialNode(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if tm.server.SyncMode() {
		ctx = mq.SetHeaders(ctx, map[string]string{consts.AwaitResponseKey: "true"})
	}
	taskID := GetTaskID(ctx)
	if taskID != "" {
		if _, exists := tm.taskContext.Get(taskID); !exists {
			return ctx, nil, fmt.Errorf("provided task ID doesn't exist")
		}
	}
	if taskID == "" {
		taskID = mq.NewID()
	}
	return ctx, mq.NewTask(taskID, payload, initialNode), nil
}

func (tm *DAG) Process(ctx context.Context, payload []byte) mq.Result {
	ctx, task, err := tm.check(ctx, payload)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("unable to process task, error: DAG is not accepting any task")}
	}
	awaitResponse, _ := mq.GetAwaitResponse(ctx)
	if awaitResponse != "true" {
		headers, ok := mq.GetHeaders(ctx)
		ctxx := context.Background()
		if ok {
			ctxx = mq.SetHeaders(ctxx, headers.AsMap())
		}
		if err := tm.pool.EnqueueTask(ctxx, task, 0); err != nil {
			return mq.Result{CreatedAt: task.CreatedAt, TaskID: task.ID, Topic: task.Topic, Status: "FAILED", Error: err}
		}
		return mq.Result{CreatedAt: task.CreatedAt, TaskID: task.ID, Topic: task.Topic, Status: "PENDING"}
	}
	return tm.ProcessTask(ctx, task)
}

func (tm *DAG) ScheduleTask(ctx context.Context, payload []byte, opts ...mq.SchedulerOption) mq.Result {
	ctx, task, err := tm.check(ctx, payload)
	if err != nil {
		return mq.Result{Error: fmt.Errorf("unable to process task, error: DAG is not accepting any task")}
	}
	headers, ok := mq.GetHeaders(ctx)
	ctxx := context.Background()
	if ok {
		ctxx = mq.SetHeaders(ctxx, headers.AsMap())
	}
	tm.pool.Scheduler().AddTask(ctxx, task, opts...)
	return mq.Result{CreatedAt: task.CreatedAt, TaskID: task.ID, Topic: task.Topic, Status: "PENDING"}
}

func (tm *DAG) parseInitialNode(ctx context.Context) (string, error) {
	val := ctx.Value("initial_node")
	initialNode, ok := val.(string)
	if ok {
		return initialNode, nil
	}
	if tm.startNode == "" {
		firstNode := tm.findStartNode()
		if firstNode != nil {
			tm.startNode = firstNode.Key
		}
	}

	if tm.startNode == "" {
		return "", fmt.Errorf("initial node not found")
	}
	return tm.startNode, nil
}

func (tm *DAG) findStartNode() *Node {
	incomingEdges := make(map[string]bool)
	connectedNodes := make(map[string]bool)
	for _, node := range tm.nodes {
		for _, edge := range node.Edges {
			if edge.Type.IsValid() {
				connectedNodes[node.Key] = true
				for _, to := range edge.To {
					connectedNodes[to.Key] = true
					incomingEdges[to.Key] = true
				}
			}
		}
		if cond, ok := tm.conditions[FromNode(node.Key)]; ok {
			for _, target := range cond {
				connectedNodes[string(target)] = true
				incomingEdges[string(target)] = true
			}
		}
	}
	for nodeID, node := range tm.nodes {
		if !incomingEdges[nodeID] && connectedNodes[nodeID] {
			return node
		}
	}
	return nil
}

func (tm *DAG) Pause(_ context.Context) error {
	tm.paused = true
	return nil
}

func (tm *DAG) Resume(_ context.Context) error {
	tm.paused = false
	return nil
}

func (tm *DAG) Close() error {
	for _, n := range tm.nodes {
		err := n.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (tm *DAG) PauseConsumer(ctx context.Context, id string) {
	tm.doConsumer(ctx, id, consts.CONSUMER_PAUSE)
}

func (tm *DAG) ResumeConsumer(ctx context.Context, id string) {
	tm.doConsumer(ctx, id, consts.CONSUMER_RESUME)
}

func (tm *DAG) doConsumer(ctx context.Context, id string, action consts.CMD) {
	if node, ok := tm.nodes[id]; ok {
		switch action {
		case consts.CONSUMER_PAUSE:
			err := node.processor.Pause(ctx)
			if err == nil {
				node.isReady = false
				log.Printf("[INFO] - Consumer %s paused successfully", node.Key)
			} else {
				log.Printf("[ERROR] - Failed to pause consumer %s: %v", node.Key, err)
			}
		case consts.CONSUMER_RESUME:
			err := node.processor.Resume(ctx)
			if err == nil {
				node.isReady = true
				log.Printf("[INFO] - Consumer %s resumed successfully", node.Key)
			} else {
				log.Printf("[ERROR] - Failed to resume consumer %s: %v", node.Key, err)
			}
		}
	} else {
		log.Printf("[WARNING] - Consumer %s not found", id)
	}
}

func (tm *DAG) SetNotifyResponse(callback mq.Callback) {
	tm.server.SetNotifyHandler(callback)
}

func (tm *DAG) GetNextNodes(key string) ([]*Node, error) {
	node, exists := tm.nodes[key]
	if !exists {
		return nil, fmt.Errorf("Node with key %s does not exist", key)
	}
	var successors []*Node
	for _, edge := range node.Edges {
		successors = append(successors, edge.To...)
	}
	if conds, exists := tm.conditions[FromNode(key)]; exists {
		for _, targetKey := range conds {
			if targetNode, exists := tm.nodes[string(targetKey)]; exists {
				successors = append(successors, targetNode)
			}
		}
	}
	return successors, nil
}

func (tm *DAG) GetPreviousNodes(key string) ([]*Node, error) {
	var predecessors []*Node
	for _, node := range tm.nodes {
		for _, edge := range node.Edges {
			for _, target := range edge.To {
				if target.Key == key {
					predecessors = append(predecessors, node)
				}
			}
		}
	}
	for fromNode, conds := range tm.conditions {
		for _, targetKey := range conds {
			if string(targetKey) == key {
				node, exists := tm.nodes[string(fromNode)]
				if !exists {
					return nil, fmt.Errorf("Node with key %s does not exist", fromNode)
				}
				predecessors = append(predecessors, node)
			}
		}
	}
	return predecessors, nil
}
