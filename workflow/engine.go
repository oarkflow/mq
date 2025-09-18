package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// WorkflowEngine - Main workflow engine
type WorkflowEngine struct {
	registry         WorkflowRegistry
	stateManager     StateManager
	executor         WorkflowExecutor
	scheduler        WorkflowScheduler
	processorFactory *ProcessorFactory
	config           *Config
	mu               sync.RWMutex
	running          bool
}

// NewWorkflowEngine creates a new workflow engine
func NewWorkflowEngine(config *Config) *WorkflowEngine {
	engine := &WorkflowEngine{
		registry:         NewInMemoryRegistry(),
		stateManager:     NewInMemoryStateManager(),
		processorFactory: NewProcessorFactory(),
		config:           config,
	}

	// Create executor and scheduler
	engine.executor = NewWorkflowExecutor(engine.processorFactory, engine.stateManager, config)
	engine.scheduler = NewWorkflowScheduler(engine.stateManager, engine.executor)

	return engine
}

// Start the workflow engine
func (e *WorkflowEngine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return fmt.Errorf("workflow engine is already running")
	}

	// Start components
	if err := e.executor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start executor: %w", err)
	}

	if err := e.scheduler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}

	e.running = true
	return nil
}

// Stop the workflow engine
func (e *WorkflowEngine) Stop(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return
	}

	e.executor.Stop(ctx)
	e.scheduler.Stop(ctx)
	e.running = false
}

// RegisterWorkflow registers a new workflow definition
func (e *WorkflowEngine) RegisterWorkflow(ctx context.Context, definition *WorkflowDefinition) error {
	// Set timestamps
	now := time.Now()
	if definition.CreatedAt.IsZero() {
		definition.CreatedAt = now
	}
	definition.UpdatedAt = now

	// Validate workflow
	if err := e.validateWorkflow(definition); err != nil {
		return fmt.Errorf("workflow validation failed: %w", err)
	}

	return e.registry.Store(ctx, definition)
}

// GetWorkflow retrieves a workflow definition
func (e *WorkflowEngine) GetWorkflow(ctx context.Context, id string, version string) (*WorkflowDefinition, error) {
	return e.registry.Get(ctx, id, version)
}

// ListWorkflows lists workflow definitions with filtering
func (e *WorkflowEngine) ListWorkflows(ctx context.Context, filter *WorkflowFilter) ([]*WorkflowDefinition, error) {
	return e.registry.List(ctx, filter)
}

// DeleteWorkflow removes a workflow definition
func (e *WorkflowEngine) DeleteWorkflow(ctx context.Context, id string) error {
	return e.registry.Delete(ctx, id)
}

// ExecuteWorkflow starts workflow execution
func (e *WorkflowEngine) ExecuteWorkflow(ctx context.Context, workflowID string, input map[string]interface{}, options *ExecutionOptions) (*Execution, error) {
	// Get workflow definition
	definition, err := e.registry.Get(ctx, workflowID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	// Create execution
	execution := &Execution{
		ID:              uuid.New().String(),
		WorkflowID:      workflowID,
		WorkflowVersion: definition.Version,
		Status:          ExecutionStatusPending,
		Input:           input,
		Context: ExecutionContext{
			Variables:   make(map[string]interface{}),
			Metadata:    make(map[string]interface{}),
			Trace:       []TraceEntry{},
			Checkpoints: []Checkpoint{},
		},
		ExecutedNodes: []ExecutedNode{},
		StartedAt:     time.Now(),
		UpdatedAt:     time.Now(),
		Priority:      PriorityMedium,
	}

	// Apply options
	if options != nil {
		if options.Priority != "" {
			execution.Priority = options.Priority
		}
		if options.Owner != "" {
			execution.Owner = options.Owner
		}
		if options.TriggeredBy != "" {
			execution.TriggeredBy = options.TriggeredBy
		}
		if options.ParentExecution != "" {
			execution.ParentExecution = options.ParentExecution
		}
		if options.Delay > 0 {
			// Schedule for later execution
			if err := e.scheduler.ScheduleExecution(ctx, execution, options.Delay); err != nil {
				return nil, fmt.Errorf("failed to schedule execution: %w", err)
			}
			// Save execution in pending state
			if err := e.stateManager.CreateExecution(ctx, execution); err != nil {
				return nil, fmt.Errorf("failed to create execution: %w", err)
			}
			return execution, nil
		}
	}

	// Save execution
	if err := e.stateManager.CreateExecution(ctx, execution); err != nil {
		return nil, fmt.Errorf("failed to create execution: %w", err)
	}

	// Start execution
	go func() {
		execution.Status = ExecutionStatusRunning
		execution.UpdatedAt = time.Now()

		if err := e.stateManager.UpdateExecution(context.Background(), execution); err != nil {
			// Log error but continue
		}

		if err := e.executor.Execute(context.Background(), definition, execution); err != nil {
			execution.Status = ExecutionStatusFailed
			execution.Error = err.Error()
			now := time.Now()
			execution.CompletedAt = &now
			execution.UpdatedAt = now
			e.stateManager.UpdateExecution(context.Background(), execution)
		}
	}()

	return execution, nil
}

// GetExecution retrieves execution status
func (e *WorkflowEngine) GetExecution(ctx context.Context, executionID string) (*Execution, error) {
	return e.stateManager.GetExecution(ctx, executionID)
}

// ListExecutions lists executions with filtering
func (e *WorkflowEngine) ListExecutions(ctx context.Context, filter *ExecutionFilter) ([]*Execution, error) {
	return e.stateManager.ListExecutions(ctx, filter)
}

// CancelExecution cancels a running execution
func (e *WorkflowEngine) CancelExecution(ctx context.Context, executionID string) error {
	return e.executor.Cancel(ctx, executionID)
}

// SuspendExecution suspends a running execution
func (e *WorkflowEngine) SuspendExecution(ctx context.Context, executionID string) error {
	return e.executor.Suspend(ctx, executionID)
}

// ResumeExecution resumes a suspended execution
func (e *WorkflowEngine) ResumeExecution(ctx context.Context, executionID string) error {
	return e.executor.Resume(ctx, executionID)
}

// validateWorkflow validates a workflow definition
func (e *WorkflowEngine) validateWorkflow(definition *WorkflowDefinition) error {
	if definition.ID == "" {
		return fmt.Errorf("workflow ID cannot be empty")
	}

	if definition.Name == "" {
		return fmt.Errorf("workflow name cannot be empty")
	}

	if definition.Version == "" {
		return fmt.Errorf("workflow version cannot be empty")
	}

	if len(definition.Nodes) == 0 {
		return fmt.Errorf("workflow must have at least one node")
	}

	// Validate nodes
	nodeIDs := make(map[string]bool)
	for _, node := range definition.Nodes {
		if node.ID == "" {
			return fmt.Errorf("node ID cannot be empty")
		}

		if nodeIDs[node.ID] {
			return fmt.Errorf("duplicate node ID: %s", node.ID)
		}
		nodeIDs[node.ID] = true

		if node.Type == "" {
			return fmt.Errorf("node type cannot be empty for node: %s", node.ID)
		}

		// Validate node configuration based on type
		if err := e.validateNodeConfig(node); err != nil {
			return fmt.Errorf("invalid configuration for node %s: %w", node.ID, err)
		}
	}

	// Validate edges
	for _, edge := range definition.Edges {
		if edge.FromNode == "" || edge.ToNode == "" {
			return fmt.Errorf("edge must have both from_node and to_node")
		}

		if !nodeIDs[edge.FromNode] {
			return fmt.Errorf("edge references unknown from_node: %s", edge.FromNode)
		}

		if !nodeIDs[edge.ToNode] {
			return fmt.Errorf("edge references unknown to_node: %s", edge.ToNode)
		}
	}

	return nil
}

func (e *WorkflowEngine) validateNodeConfig(node WorkflowNode) error {
	switch node.Type {
	case NodeTypeAPI:
		if node.Config.URL == "" {
			return fmt.Errorf("API node requires URL")
		}
		if node.Config.Method == "" {
			return fmt.Errorf("API node requires HTTP method")
		}

	case NodeTypeTransform:
		if node.Config.TransformType == "" {
			return fmt.Errorf("Transform node requires transform_type")
		}

	case NodeTypeDecision:
		if node.Config.Condition == "" && len(node.Config.DecisionRules) == 0 {
			return fmt.Errorf("Decision node requires either condition or rules")
		}

	case NodeTypeTimer:
		if node.Config.Duration <= 0 && node.Config.Schedule == "" {
			return fmt.Errorf("Timer node requires either duration or schedule")
		}

	case NodeTypeDatabase:
		if node.Config.Query == "" {
			return fmt.Errorf("Database node requires query")
		}

	case NodeTypeEmail:
		if len(node.Config.EmailTo) == 0 {
			return fmt.Errorf("Email node requires recipients")
		}
	}

	return nil
}

// ExecutionOptions for workflow execution
type ExecutionOptions struct {
	Priority        Priority      `json:"priority"`
	Owner           string        `json:"owner"`
	TriggeredBy     string        `json:"triggered_by"`
	ParentExecution string        `json:"parent_execution"`
	Delay           time.Duration `json:"delay"`
}

// Simple Executor Implementation
type SimpleWorkflowExecutor struct {
	processorFactory *ProcessorFactory
	stateManager     StateManager
	config           *Config
	workers          chan struct{}
	running          bool
	executions       map[string]*ExecutionControl
	mu               sync.RWMutex
}

type ExecutionControl struct {
	cancel    context.CancelFunc
	suspended bool
}

func NewWorkflowExecutor(processorFactory *ProcessorFactory, stateManager StateManager, config *Config) WorkflowExecutor {
	return &SimpleWorkflowExecutor{
		processorFactory: processorFactory,
		stateManager:     stateManager,
		config:           config,
		workers:          make(chan struct{}, config.MaxWorkers),
		executions:       make(map[string]*ExecutionControl),
	}
}

func (e *SimpleWorkflowExecutor) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.running = true

	// Initialize worker pool
	for i := 0; i < e.config.MaxWorkers; i++ {
		e.workers <- struct{}{}
	}

	return nil
}

func (e *SimpleWorkflowExecutor) Stop(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.running = false
	close(e.workers)

	// Cancel all running executions
	for _, control := range e.executions {
		if control.cancel != nil {
			control.cancel()
		}
	}
}

func (e *SimpleWorkflowExecutor) Execute(ctx context.Context, definition *WorkflowDefinition, execution *Execution) error {
	// Get a worker
	<-e.workers
	defer func() {
		if e.running {
			e.workers <- struct{}{}
		}
	}()

	// Create cancellable context
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Track execution
	e.mu.Lock()
	e.executions[execution.ID] = &ExecutionControl{cancel: cancel}
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.executions, execution.ID)
		e.mu.Unlock()
	}()

	// Convert workflow to DAG and execute
	dag, err := e.convertToDAG(definition, execution)
	if err != nil {
		return fmt.Errorf("failed to convert workflow to DAG: %w", err)
	}

	// Execute the DAG
	inputBytes, err := json.Marshal(execution.Input)
	if err != nil {
		return fmt.Errorf("failed to serialize input: %w", err)
	}

	result := dag.Process(execCtx, inputBytes)

	// Update execution state
	execution.Status = ExecutionStatusCompleted
	if result.Error != nil {
		execution.Status = ExecutionStatusFailed
		execution.Error = result.Error.Error()
	} else {
		// Deserialize output
		var output map[string]interface{}
		if err := json.Unmarshal(result.Payload, &output); err == nil {
			execution.Output = output
		}
	}

	now := time.Now()
	execution.CompletedAt = &now
	execution.UpdatedAt = now

	return e.stateManager.UpdateExecution(ctx, execution)
}

func (e *SimpleWorkflowExecutor) Cancel(ctx context.Context, executionID string) error {
	e.mu.RLock()
	control, exists := e.executions[executionID]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("execution not found: %s", executionID)
	}

	if control.cancel != nil {
		control.cancel()
	}

	// Update execution status
	execution, err := e.stateManager.GetExecution(ctx, executionID)
	if err != nil {
		return err
	}

	execution.Status = ExecutionStatusCancelled
	now := time.Now()
	execution.CompletedAt = &now
	execution.UpdatedAt = now

	return e.stateManager.UpdateExecution(ctx, execution)
}

func (e *SimpleWorkflowExecutor) Suspend(ctx context.Context, executionID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	control, exists := e.executions[executionID]
	if !exists {
		return fmt.Errorf("execution not found: %s", executionID)
	}

	control.suspended = true

	// Update execution status
	execution, err := e.stateManager.GetExecution(ctx, executionID)
	if err != nil {
		return err
	}

	execution.Status = ExecutionStatusSuspended
	execution.UpdatedAt = time.Now()

	return e.stateManager.UpdateExecution(ctx, execution)
}

func (e *SimpleWorkflowExecutor) Resume(ctx context.Context, executionID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	control, exists := e.executions[executionID]
	if !exists {
		return fmt.Errorf("execution not found: %s", executionID)
	}

	control.suspended = false

	// Update execution status
	execution, err := e.stateManager.GetExecution(ctx, executionID)
	if err != nil {
		return err
	}

	execution.Status = ExecutionStatusRunning
	execution.UpdatedAt = time.Now()

	return e.stateManager.UpdateExecution(ctx, execution)
}

func (e *SimpleWorkflowExecutor) convertToDAG(definition *WorkflowDefinition, execution *Execution) (*dag.DAG, error) {
	// Create a new DAG
	dagInstance := dag.NewDAG(
		fmt.Sprintf("workflow-%s", definition.ID),
		execution.ID,
		func(taskID string, result mq.Result) {
			// Handle final result
		},
	)

	// Create DAG nodes for each workflow node
	for _, node := range definition.Nodes {
		processor, err := e.processorFactory.CreateProcessor(string(node.Type))
		if err != nil {
			return nil, fmt.Errorf("failed to create processor for node %s: %w", node.ID, err)
		}

		// Wrap processor in a DAG processor adapter
		dagProcessor := &DAGProcessorAdapter{
			processor: processor,
			nodeID:    node.ID,
			execution: execution,
		}

		// Add node to DAG
		dagInstance.AddNode(dag.Function, node.Name, node.ID, dagProcessor, false)
	}

	// Add dependencies based on edges
	for _, edge := range definition.Edges {
		dagInstance.AddEdge(dag.Simple, edge.ID, edge.FromNode, edge.ToNode)
	}

	return dagInstance, nil
}

// DAGProcessorAdapter adapts Processor to DAG Processor interface
type DAGProcessorAdapter struct {
	dag.Operation
	processor Processor
	nodeID    string
	execution *Execution
}

func (a *DAGProcessorAdapter) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Convert task payload to ProcessingContext
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("failed to unmarshal task payload: %v", err)}
	}

	// Create a minimal workflow node for processing (in real implementation, this would be passed in)
	workflowNode := &WorkflowNode{
		ID:     a.nodeID,
		Type:   NodeTypeTask, // Default type, this should be set properly
		Config: NodeConfig{},
	}

	processingContext := ProcessingContext{
		Node:      workflowNode,
		Data:      data,
		Variables: make(map[string]interface{}),
	}

	result, err := a.processor.Process(ctx, processingContext)
	if err != nil {
		return mq.Result{Error: err}
	}

	// Convert ProcessingResult back to mq.Result
	var payload []byte
	if result.Data != nil {
		payload, _ = json.Marshal(result.Data)
	}

	mqResult := mq.Result{
		Payload: payload,
	}

	if !result.Success {
		mqResult.Error = fmt.Errorf(result.Error)
	}

	// Track node execution
	executedNode := ExecutedNode{
		NodeID:    a.nodeID,
		Status:    ExecutionStatusCompleted,
		StartedAt: time.Now(),
		Input:     data,
		Output:    result.Data,
		Logs:      []LogEntry{},
	}

	if !result.Success {
		executedNode.Status = ExecutionStatusFailed
		executedNode.Error = result.Error
	}

	now := time.Now()
	executedNode.CompletedAt = &now
	executedNode.Duration = time.Since(executedNode.StartedAt)

	// Add to execution history (in real implementation, use thread-safe approach)
	if a.execution != nil {
		a.execution.ExecutedNodes = append(a.execution.ExecutedNodes, executedNode)
	}

	return mqResult
}

// Simple Scheduler Implementation
type SimpleWorkflowScheduler struct {
	stateManager StateManager
	executor     WorkflowExecutor
	running      bool
	mu           sync.Mutex
	scheduled    map[string]*time.Timer
}

func NewWorkflowScheduler(stateManager StateManager, executor WorkflowExecutor) WorkflowScheduler {
	return &SimpleWorkflowScheduler{
		stateManager: stateManager,
		executor:     executor,
		scheduled:    make(map[string]*time.Timer),
	}
}

func (s *SimpleWorkflowScheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.running = true
	return nil
}

func (s *SimpleWorkflowScheduler) Stop(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.running = false

	// Cancel all scheduled executions
	for _, timer := range s.scheduled {
		timer.Stop()
	}
	s.scheduled = make(map[string]*time.Timer)
}

func (s *SimpleWorkflowScheduler) ScheduleExecution(ctx context.Context, execution *Execution, delay time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("scheduler is not running")
	}

	// Create timer for delayed execution
	timer := time.AfterFunc(delay, func() {
		// Remove from scheduled map
		s.mu.Lock()
		delete(s.scheduled, execution.ID)
		s.mu.Unlock()

		// Execute workflow (implementation depends on having access to workflow definition)
		// For now, just update status
		execution.Status = ExecutionStatusRunning
		execution.UpdatedAt = time.Now()
		s.stateManager.UpdateExecution(context.Background(), execution)
	})

	s.scheduled[execution.ID] = timer
	return nil
}

func (s *SimpleWorkflowScheduler) CancelScheduledExecution(ctx context.Context, executionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	timer, exists := s.scheduled[executionID]
	if !exists {
		return fmt.Errorf("scheduled execution not found: %s", executionID)
	}

	timer.Stop()
	delete(s.scheduled, executionID)

	return nil
}
