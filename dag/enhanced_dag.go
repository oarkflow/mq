package dag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq"
)

// WorkflowEngine interface to avoid circular dependency
type WorkflowEngine interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
	RegisterWorkflow(ctx context.Context, definition *WorkflowDefinition) error
	ExecuteWorkflow(ctx context.Context, workflowID string, input map[string]any) (*ExecutionResult, error)
	GetExecution(ctx context.Context, executionID string) (*ExecutionResult, error)
}

// Enhanced workflow types to avoid circular dependency
type (
	WorkflowStatus   string
	ExecutionStatus  string
	WorkflowNodeType string
	Priority         string
)

const (
	// Workflow statuses
	WorkflowStatusDraft      WorkflowStatus = "draft"
	WorkflowStatusActive     WorkflowStatus = "active"
	WorkflowStatusInactive   WorkflowStatus = "inactive"
	WorkflowStatusDeprecated WorkflowStatus = "deprecated"

	// Execution statuses
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusCompleted ExecutionStatus = "completed"
	ExecutionStatusFailed    ExecutionStatus = "failed"
	ExecutionStatusCancelled ExecutionStatus = "cancelled"
	ExecutionStatusSuspended ExecutionStatus = "suspended"

	// Enhanced node types
	WorkflowNodeTypeTask      WorkflowNodeType = "task"
	WorkflowNodeTypeAPI       WorkflowNodeType = "api"
	WorkflowNodeTypeTransform WorkflowNodeType = "transform"
	WorkflowNodeTypeDecision  WorkflowNodeType = "decision"
	WorkflowNodeTypeHumanTask WorkflowNodeType = "human_task"
	WorkflowNodeTypeTimer     WorkflowNodeType = "timer"
	WorkflowNodeTypeLoop      WorkflowNodeType = "loop"
	WorkflowNodeTypeParallel  WorkflowNodeType = "parallel"
	WorkflowNodeTypeDatabase  WorkflowNodeType = "database"
	WorkflowNodeTypeEmail     WorkflowNodeType = "email"
	WorkflowNodeTypeWebhook   WorkflowNodeType = "webhook"
	WorkflowNodeTypeSubDAG    WorkflowNodeType = "sub_dag"
	WorkflowNodeTypeHTML      WorkflowNodeType = "html"
	WorkflowNodeTypeSMS       WorkflowNodeType = "sms"
	WorkflowNodeTypeAuth      WorkflowNodeType = "auth"
	WorkflowNodeTypeValidator WorkflowNodeType = "validator"
	WorkflowNodeTypeRouter    WorkflowNodeType = "router"
	WorkflowNodeTypeNotify    WorkflowNodeType = "notify"
	WorkflowNodeTypeStorage   WorkflowNodeType = "storage"
	WorkflowNodeTypeWebhookRx WorkflowNodeType = "webhook_receiver"

	// Priorities
	PriorityLow      Priority = "low"
	PriorityMedium   Priority = "medium"
	PriorityHigh     Priority = "high"
	PriorityCritical Priority = "critical"
)

// WorkflowDefinition represents a complete workflow
type WorkflowDefinition struct {
	ID          string              `json:"id"`
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Version     string              `json:"version"`
	Status      WorkflowStatus      `json:"status"`
	Tags        []string            `json:"tags"`
	Category    string              `json:"category"`
	Owner       string              `json:"owner"`
	Nodes       []WorkflowNode      `json:"nodes"`
	Edges       []WorkflowEdge      `json:"edges"`
	Variables   map[string]Variable `json:"variables"`
	Config      WorkflowConfig      `json:"config"`
	Metadata    map[string]any      `json:"metadata"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
	CreatedBy   string              `json:"created_by"`
	UpdatedBy   string              `json:"updated_by"`
}

// WorkflowNode represents a single node in the workflow
type WorkflowNode struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Type        WorkflowNodeType   `json:"type"`
	Description string             `json:"description"`
	Config      WorkflowNodeConfig `json:"config"`
	Position    Position           `json:"position"`
	Timeout     *time.Duration     `json:"timeout,omitempty"`
	RetryPolicy *RetryPolicy       `json:"retry_policy,omitempty"`
	Metadata    map[string]any     `json:"metadata,omitempty"`
}

// WorkflowNodeConfig holds configuration for different node types
type WorkflowNodeConfig struct {
	// Common fields
	Script    string            `json:"script,omitempty"`
	Command   string            `json:"command,omitempty"`
	Variables map[string]string `json:"variables,omitempty"`

	// API node fields
	URL     string            `json:"url,omitempty"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`

	// Transform node fields
	TransformType string `json:"transform_type,omitempty"`
	Expression    string `json:"expression,omitempty"`

	// Decision node fields
	Condition     string                 `json:"condition,omitempty"`
	DecisionRules []WorkflowDecisionRule `json:"decision_rules,omitempty"`

	// Timer node fields
	Duration time.Duration `json:"duration,omitempty"`
	Schedule string        `json:"schedule,omitempty"`

	// Database node fields
	Query      string `json:"query,omitempty"`
	Connection string `json:"connection,omitempty"`

	// Email node fields
	EmailTo []string `json:"email_to,omitempty"`
	Subject string   `json:"subject,omitempty"`
	Body    string   `json:"body,omitempty"`

	// Sub-DAG node fields
	SubWorkflowID string            `json:"sub_workflow_id,omitempty"`
	InputMapping  map[string]string `json:"input_mapping,omitempty"`
	OutputMapping map[string]string `json:"output_mapping,omitempty"`

	// HTML node fields
	Template     string            `json:"template,omitempty"`
	TemplateData map[string]string `json:"template_data,omitempty"`
	OutputPath   string            `json:"output_path,omitempty"`

	// SMS node fields
	Provider    string   `json:"provider,omitempty"`
	From        string   `json:"from,omitempty"`
	SMSTo       []string `json:"sms_to,omitempty"`
	Message     string   `json:"message,omitempty"`
	MessageType string   `json:"message_type,omitempty"`

	// Auth node fields
	AuthType    string            `json:"auth_type,omitempty"`
	Credentials map[string]string `json:"credentials,omitempty"`
	TokenExpiry time.Duration     `json:"token_expiry,omitempty"`

	// Storage node fields
	StorageType      string            `json:"storage_type,omitempty"`
	StorageOperation string            `json:"storage_operation,omitempty"`
	StorageKey       string            `json:"storage_key,omitempty"`
	StoragePath      string            `json:"storage_path,omitempty"`
	StorageConfig    map[string]string `json:"storage_config,omitempty"`

	// Validator node fields
	ValidationType  string                   `json:"validation_type,omitempty"`
	ValidationRules []WorkflowValidationRule `json:"validation_rules,omitempty"`

	// Router node fields
	RoutingRules []WorkflowRoutingRule `json:"routing_rules,omitempty"`
	DefaultRoute string                `json:"default_route,omitempty"`

	// Notification node fields
	NotifyType             string   `json:"notify_type,omitempty"`
	NotificationType       string   `json:"notification_type,omitempty"`
	NotificationRecipients []string `json:"notification_recipients,omitempty"`
	NotificationMessage    string   `json:"notification_message,omitempty"`
	Recipients             []string `json:"recipients,omitempty"`
	Channel                string   `json:"channel,omitempty"`

	// Webhook receiver fields
	ListenPath        string         `json:"listen_path,omitempty"`
	Secret            string         `json:"secret,omitempty"`
	WebhookSecret     string         `json:"webhook_secret,omitempty"`
	WebhookSignature  string         `json:"webhook_signature,omitempty"`
	WebhookTransforms map[string]any `json:"webhook_transforms,omitempty"`
	Timeout           time.Duration  `json:"timeout,omitempty"`

	// Custom configuration
	Custom map[string]any `json:"custom,omitempty"`
}

// WorkflowDecisionRule for decision nodes
type WorkflowDecisionRule struct {
	Condition string `json:"condition"`
	NextNode  string `json:"next_node"`
}

// WorkflowValidationRule for validator nodes
type WorkflowValidationRule struct {
	Field     string   `json:"field"`
	Type      string   `json:"type"` // "string", "number", "email", "regex", "required"
	Required  bool     `json:"required"`
	MinLength int      `json:"min_length,omitempty"`
	MaxLength int      `json:"max_length,omitempty"`
	Min       *float64 `json:"min,omitempty"`
	Max       *float64 `json:"max,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	Value     any      `json:"value,omitempty"`
	Message   string   `json:"message,omitempty"`
}

// WorkflowRoutingRule for router nodes
type WorkflowRoutingRule struct {
	Condition   string `json:"condition"`
	Destination string `json:"destination"`
}

// WorkflowEdge represents a connection between nodes
type WorkflowEdge struct {
	ID        string         `json:"id"`
	FromNode  string         `json:"from_node"`
	ToNode    string         `json:"to_node"`
	Condition string         `json:"condition,omitempty"`
	Priority  int            `json:"priority"`
	Label     string         `json:"label,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// Variable definition for workflow
type Variable struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	DefaultValue any    `json:"default_value"`
	Required     bool   `json:"required"`
	Description  string `json:"description"`
}

// WorkflowConfig holds configuration for the entire workflow
type WorkflowConfig struct {
	Timeout       *time.Duration `json:"timeout,omitempty"`
	MaxRetries    int            `json:"max_retries"`
	Priority      Priority       `json:"priority"`
	Concurrency   int            `json:"concurrency"`
	EnableAudit   bool           `json:"enable_audit"`
	EnableMetrics bool           `json:"enable_metrics"`
}

// Position represents node position in UI
type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	BackoffMs   int           `json:"backoff_ms"`
	Jitter      bool          `json:"jitter"`
	Timeout     time.Duration `json:"timeout"`
}

// ExecutionResult represents the result of workflow execution
type ExecutionResult struct {
	ID             string          `json:"id"`
	WorkflowID     string          `json:"workflow_id"`
	Status         ExecutionStatus `json:"status"`
	StartTime      time.Time       `json:"start_time"`
	EndTime        *time.Time      `json:"end_time,omitempty"`
	Input          map[string]any  `json:"input"`
	Output         map[string]any  `json:"output"`
	Error          string          `json:"error,omitempty"`
	NodeExecutions map[string]any  `json:"node_executions,omitempty"`
}

// EnhancedDAG represents a DAG that integrates with workflow engine concepts
type EnhancedDAG struct {
	*DAG // Embed the original DAG for backward compatibility

	// Workflow definitions registry
	workflowRegistry map[string]*WorkflowDefinition

	// Enhanced execution capabilities
	executionManager *ExecutionManager
	stateManager     *WorkflowStateManager

	// External workflow engine (optional)
	workflowEngine WorkflowEngine

	// Configuration
	config *EnhancedDAGConfig

	// Thread safety
	mu sync.RWMutex
}

// EnhancedDAGConfig contains configuration for the enhanced DAG
type EnhancedDAGConfig struct {
	// Workflow engine integration
	EnableWorkflowEngine bool
	WorkflowEngine       WorkflowEngine

	// Backward compatibility
	MaintainDAGMode      bool
	AutoMigrateWorkflows bool

	// Enhanced features
	EnablePersistence     bool
	EnableStateManagement bool
	EnableAdvancedRetry   bool
	EnableCircuitBreaker  bool

	// Execution settings
	MaxConcurrentExecutions int
	DefaultTimeout          time.Duration
	EnableMetrics           bool
}

// ExecutionManager manages workflow and DAG executions
type ExecutionManager struct {
	activeExecutions map[string]*WorkflowExecution
	executionHistory map[string]*WorkflowExecution
	mu               sync.RWMutex
}

// WorkflowExecution represents an active or completed workflow execution
type WorkflowExecution struct {
	ID              string
	WorkflowID      string
	WorkflowVersion string
	Status          ExecutionStatus
	StartTime       time.Time
	EndTime         *time.Time
	Context         context.Context
	Input           map[string]any
	Output          map[string]any
	Error           error

	// Node execution tracking
	NodeExecutions map[string]*NodeExecution
}

// NodeExecution tracks individual node execution within a workflow
type NodeExecution struct {
	NodeID     string
	Status     ExecutionStatus
	StartTime  time.Time
	EndTime    *time.Time
	Input      map[string]any
	Output     map[string]any
	Error      error
	RetryCount int
	Duration   time.Duration
}

// WorkflowStateManager manages workflow state and persistence
type WorkflowStateManager struct {
	stateStore map[string]any
	mu         sync.RWMutex
}

// NewEnhancedDAG creates a new enhanced DAG with workflow engine integration
func NewEnhancedDAG(name, key string, config *EnhancedDAGConfig, opts ...mq.Option) (*EnhancedDAG, error) {
	if config == nil {
		config = &EnhancedDAGConfig{
			EnableWorkflowEngine:    false, // Start with false to avoid circular dependency
			MaintainDAGMode:         true,
			AutoMigrateWorkflows:    true,
			MaxConcurrentExecutions: 100,
			DefaultTimeout:          time.Minute * 30,
			EnableMetrics:           true,
		}
	}

	// Create the original DAG
	originalDAG := NewDAG(name, key, nil, opts...)

	// Create enhanced DAG
	enhanced := &EnhancedDAG{
		DAG:              originalDAG,
		workflowRegistry: make(map[string]*WorkflowDefinition),
		config:           config,
		executionManager: &ExecutionManager{
			activeExecutions: make(map[string]*WorkflowExecution),
			executionHistory: make(map[string]*WorkflowExecution),
		},
		stateManager: &WorkflowStateManager{
			stateStore: make(map[string]any),
		},
	}

	// Set external workflow engine if provided
	if config.WorkflowEngine != nil {
		enhanced.workflowEngine = config.WorkflowEngine
	}

	return enhanced, nil
}

// RegisterWorkflow registers a workflow definition with the enhanced DAG
func (e *EnhancedDAG) RegisterWorkflow(ctx context.Context, definition *WorkflowDefinition) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate workflow definition
	if definition.ID == "" {
		return errors.New("workflow ID is required")
	}

	// Register with external workflow engine if enabled
	if e.config.EnableWorkflowEngine && e.workflowEngine != nil {
		if err := e.workflowEngine.RegisterWorkflow(ctx, definition); err != nil {
			return fmt.Errorf("failed to register workflow with engine: %w", err)
		}
	}

	// Store in local registry
	e.workflowRegistry[definition.ID] = definition

	// Convert workflow to DAG nodes if backward compatibility is enabled
	if e.config.MaintainDAGMode {
		if err := e.convertWorkflowToDAGNodes(definition); err != nil {
			return fmt.Errorf("failed to convert workflow to DAG nodes: %w", err)
		}
	}

	return nil
}

// convertWorkflowToDAGNodes converts a workflow definition to DAG nodes
func (e *EnhancedDAG) convertWorkflowToDAGNodes(definition *WorkflowDefinition) error {
	// Create nodes from workflow nodes
	for _, workflowNode := range definition.Nodes {
		node := &Node{
			ID:       workflowNode.ID,
			Label:    workflowNode.Name,
			NodeType: convertWorkflowNodeType(workflowNode.Type),
		}

		// Create a basic processor for the workflow node
		node.processor = e.createBasicProcessor(&workflowNode)

		if workflowNode.Timeout != nil {
			node.Timeout = *workflowNode.Timeout
		}

		e.DAG.nodes.Set(node.ID, node)
	}

	// Create edges from workflow edges
	for _, workflowEdge := range definition.Edges {
		fromNode, fromExists := e.DAG.nodes.Get(workflowEdge.FromNode)
		toNode, toExists := e.DAG.nodes.Get(workflowEdge.ToNode)

		if !fromExists || !toExists {
			continue
		}

		edge := Edge{
			From:  fromNode,
			To:    toNode,
			Label: workflowEdge.Label,
			Type:  Simple, // Default to simple edge type
		}

		fromNode.Edges = append(fromNode.Edges, edge)
	}

	return nil
}

// createBasicProcessor creates a basic processor from a workflow node
func (e *EnhancedDAG) createBasicProcessor(workflowNode *WorkflowNode) mq.Processor {
	// Return a simple processor that implements the mq.Processor interface
	return &workflowNodeProcessor{
		node:        workflowNode,
		enhancedDAG: e,
	}
}

// workflowNodeProcessor implements mq.Processor for workflow nodes
type workflowNodeProcessor struct {
	node        *WorkflowNode
	enhancedDAG *EnhancedDAG
	key         string
}

func (p *workflowNodeProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Execute the workflow node based on its type
	switch p.node.Type {
	case WorkflowNodeTypeAPI:
		return p.processAPINode(ctx, task)
	case WorkflowNodeTypeTransform:
		return p.processTransformNode(ctx, task)
	case WorkflowNodeTypeDecision:
		return p.processDecisionNode(ctx, task)
	case WorkflowNodeTypeEmail:
		return p.processEmailNode(ctx, task)
	case WorkflowNodeTypeDatabase:
		return p.processDatabaseNode(ctx, task)
	case WorkflowNodeTypeTimer:
		return p.processTimerNode(ctx, task)
	default:
		return p.processTaskNode(ctx, task)
	}
}

func (p *workflowNodeProcessor) Consume(ctx context.Context) error {
	// Basic consume implementation
	return nil
}

func (p *workflowNodeProcessor) Pause(ctx context.Context) error {
	return nil
}

func (p *workflowNodeProcessor) Resume(ctx context.Context) error {
	return nil
}

func (p *workflowNodeProcessor) Stop(ctx context.Context) error {
	return nil
}

func (p *workflowNodeProcessor) Close() error {
	// Cleanup resources if needed
	return nil
}

func (p *workflowNodeProcessor) GetKey() string {
	return p.key
}

func (p *workflowNodeProcessor) SetKey(key string) {
	p.key = key
}

func (p *workflowNodeProcessor) GetType() string {
	return string(p.node.Type)
}

// Node type-specific processing methods
func (p *workflowNodeProcessor) processTaskNode(ctx context.Context, task *mq.Task) mq.Result {
	// Basic task processing - execute script or command if provided
	if p.node.Config.Script != "" {
		// Execute script (simplified implementation)
		return mq.Result{
			TaskID:  task.ID,
			Status:  mq.Completed,
			Payload: task.Payload,
		}
	}

	if p.node.Config.Command != "" {
		// Execute command (simplified implementation)
		return mq.Result{
			TaskID:  task.ID,
			Status:  mq.Completed,
			Payload: task.Payload,
		}
	}

	// Default passthrough
	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

func (p *workflowNodeProcessor) processAPINode(ctx context.Context, task *mq.Task) mq.Result {
	// API call processing (simplified implementation)
	// In a real implementation, this would make HTTP requests
	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

func (p *workflowNodeProcessor) processTransformNode(ctx context.Context, task *mq.Task) mq.Result {
	// Data transformation processing (simplified implementation)
	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to unmarshal payload: %w", err),
		}
	}

	// Apply transformation (simplified)
	payload["transformed"] = true
	payload["transform_type"] = p.node.Config.TransformType
	payload["expression"] = p.node.Config.Expression

	transformedPayload, _ := json.Marshal(payload)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: transformedPayload,
	}
}

func (p *workflowNodeProcessor) processDecisionNode(ctx context.Context, task *mq.Task) mq.Result {
	// Decision processing (simplified implementation)
	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

func (p *workflowNodeProcessor) processEmailNode(ctx context.Context, task *mq.Task) mq.Result {
	// Email processing (simplified implementation)
	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

func (p *workflowNodeProcessor) processDatabaseNode(ctx context.Context, task *mq.Task) mq.Result {
	// Database processing (simplified implementation)
	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

func (p *workflowNodeProcessor) processTimerNode(ctx context.Context, task *mq.Task) mq.Result {
	// Timer processing
	if p.node.Config.Duration > 0 {
		time.Sleep(p.node.Config.Duration)
	}

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

// ExecuteWorkflow executes a registered workflow
func (e *EnhancedDAG) ExecuteWorkflow(ctx context.Context, workflowID string, input map[string]any) (*WorkflowExecution, error) {
	e.mu.RLock()
	definition, exists := e.workflowRegistry[workflowID]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("workflow %s not found", workflowID)
	}

	// Create execution
	execution := &WorkflowExecution{
		ID:              generateExecutionID(),
		WorkflowID:      workflowID,
		WorkflowVersion: definition.Version,
		Status:          ExecutionStatusPending,
		StartTime:       time.Now(),
		Context:         ctx,
		Input:           input,
		NodeExecutions:  make(map[string]*NodeExecution),
	}

	// Store execution
	e.executionManager.mu.Lock()
	e.executionManager.activeExecutions[execution.ID] = execution
	e.executionManager.mu.Unlock()

	// Execute using external workflow engine if available
	if e.config.EnableWorkflowEngine && e.workflowEngine != nil {
		go e.executeWithWorkflowEngine(execution, definition)
	} else {
		// Fallback to DAG execution
		go e.executeWithDAG(execution, definition)
	}

	return execution, nil
}

// executeWithWorkflowEngine executes the workflow using the external workflow engine
func (e *EnhancedDAG) executeWithWorkflowEngine(execution *WorkflowExecution, definition *WorkflowDefinition) {
	execution.Status = ExecutionStatusRunning

	defer func() {
		if r := recover(); r != nil {
			execution.Status = ExecutionStatusFailed
			execution.Error = fmt.Errorf("workflow execution panicked: %v", r)
		}

		endTime := time.Now()
		execution.EndTime = &endTime

		// Move to history
		e.executionManager.mu.Lock()
		delete(e.executionManager.activeExecutions, execution.ID)
		e.executionManager.executionHistory[execution.ID] = execution
		e.executionManager.mu.Unlock()
	}()

	// Use external workflow engine to execute
	if e.workflowEngine != nil {
		result, err := e.workflowEngine.ExecuteWorkflow(execution.Context, definition.ID, execution.Input)
		if err != nil {
			execution.Status = ExecutionStatusFailed
			execution.Error = err
			return
		}

		execution.Status = result.Status
		execution.Output = result.Output
		if result.Error != "" {
			execution.Error = errors.New(result.Error)
		}
	}
}

// executeWithDAG executes the workflow using the traditional DAG approach
func (e *EnhancedDAG) executeWithDAG(execution *WorkflowExecution, definition *WorkflowDefinition) {
	execution.Status = ExecutionStatusRunning

	defer func() {
		if r := recover(); r != nil {
			execution.Status = ExecutionStatusFailed
			execution.Error = fmt.Errorf("DAG execution panicked: %v", r)
		}

		endTime := time.Now()
		execution.EndTime = &endTime

		// Move to history
		e.executionManager.mu.Lock()
		delete(e.executionManager.activeExecutions, execution.ID)
		e.executionManager.executionHistory[execution.ID] = execution
		e.executionManager.mu.Unlock()
	}()

	// Convert input to JSON payload
	payload, err := json.Marshal(execution.Input)
	if err != nil {
		execution.Status = ExecutionStatusFailed
		execution.Error = fmt.Errorf("failed to marshal input: %w", err)
		return
	}

	// Execute using DAG
	result := e.DAG.Process(execution.Context, payload)
	if result.Error != nil {
		execution.Status = ExecutionStatusFailed
		execution.Error = result.Error
		return
	}

	// Convert result back to output
	var output map[string]any
	if err := json.Unmarshal(result.Payload, &output); err != nil {
		// If unmarshal fails, create a simple output
		output = map[string]any{"result": string(result.Payload)}
	}

	execution.Status = ExecutionStatusCompleted
	execution.Output = output
}

// GetExecution retrieves a workflow execution by ID
func (e *EnhancedDAG) GetExecution(executionID string) (*WorkflowExecution, error) {
	e.executionManager.mu.RLock()
	defer e.executionManager.mu.RUnlock()

	// Check active executions first
	if execution, exists := e.executionManager.activeExecutions[executionID]; exists {
		return execution, nil
	}

	// Check execution history
	if execution, exists := e.executionManager.executionHistory[executionID]; exists {
		return execution, nil
	}

	return nil, fmt.Errorf("execution %s not found", executionID)
}

// ListActiveExecutions returns all currently active executions
func (e *EnhancedDAG) ListActiveExecutions() []*WorkflowExecution {
	e.executionManager.mu.RLock()
	defer e.executionManager.mu.RUnlock()

	executions := make([]*WorkflowExecution, 0, len(e.executionManager.activeExecutions))
	for _, execution := range e.executionManager.activeExecutions {
		executions = append(executions, execution)
	}

	return executions
}

// CancelExecution cancels a running workflow execution
func (e *EnhancedDAG) CancelExecution(executionID string) error {
	e.executionManager.mu.Lock()
	defer e.executionManager.mu.Unlock()

	execution, exists := e.executionManager.activeExecutions[executionID]
	if !exists {
		return fmt.Errorf("execution %s not found or not active", executionID)
	}

	execution.Status = ExecutionStatusCancelled
	endTime := time.Now()
	execution.EndTime = &endTime

	// Move to history
	delete(e.executionManager.activeExecutions, executionID)
	e.executionManager.executionHistory[executionID] = execution

	return nil
}

// GetWorkflow retrieves a workflow definition by ID
func (e *EnhancedDAG) GetWorkflow(workflowID string) (*WorkflowDefinition, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	definition, exists := e.workflowRegistry[workflowID]
	if !exists {
		return nil, fmt.Errorf("workflow %s not found", workflowID)
	}

	return definition, nil
}

// ListWorkflows returns all registered workflow definitions
func (e *EnhancedDAG) ListWorkflows() []*WorkflowDefinition {
	e.mu.RLock()
	defer e.mu.RUnlock()

	workflows := make([]*WorkflowDefinition, 0, len(e.workflowRegistry))
	for _, workflow := range e.workflowRegistry {
		workflows = append(workflows, workflow)
	}

	return workflows
}

// SetWorkflowEngine sets an external workflow engine
func (e *EnhancedDAG) SetWorkflowEngine(engine WorkflowEngine) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.workflowEngine = engine
	e.config.EnableWorkflowEngine = true
}

// Utility functions
func convertWorkflowNodeType(wt WorkflowNodeType) NodeType {
	// For now, map workflow node types to basic DAG node types
	switch wt {
	case WorkflowNodeTypeHTML:
		return Page
	default:
		return Function
	}
}

func generateExecutionID() string {
	return fmt.Sprintf("exec_%d", time.Now().UnixNano())
}

// Start starts the enhanced DAG and workflow engine
func (e *EnhancedDAG) Start(ctx context.Context, addr string) error {
	// Start the external workflow engine if enabled
	if e.config.EnableWorkflowEngine && e.workflowEngine != nil {
		if err := e.workflowEngine.Start(ctx); err != nil {
			return fmt.Errorf("failed to start workflow engine: %w", err)
		}
	}

	// Start the original DAG
	return e.DAG.Start(ctx, addr)
}

// Stop stops the enhanced DAG and workflow engine
func (e *EnhancedDAG) Stop(ctx context.Context) error {
	// Stop the workflow engine if enabled
	if e.config.EnableWorkflowEngine && e.workflowEngine != nil {
		e.workflowEngine.Stop(ctx)
	}

	// Stop the original DAG
	return e.DAG.Stop(ctx)
}
