package workflow

import (
	"context"
	"time"
)

// Core types
type (
	WorkflowStatus   string
	ExecutionStatus  string
	NodeType         string
	Priority         string
	UserRole         string
	PermissionAction string
	MiddlewareType   string
)

// User and security types
type User struct {
	ID          string            `json:"id"`
	Username    string            `json:"username"`
	Email       string            `json:"email"`
	Role        UserRole          `json:"role"`
	Permissions []string          `json:"permissions"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type AuthContext struct {
	User        *User             `json:"user"`
	SessionID   string            `json:"session_id"`
	Token       string            `json:"token"`
	Permissions []string          `json:"permissions"`
	Metadata    map[string]string `json:"metadata"`
}

type Permission struct {
	ID       string           `json:"id"`
	Resource string           `json:"resource"`
	Action   PermissionAction `json:"action"`
	Scope    string           `json:"scope"`
}

// Middleware types
type Middleware struct {
	ID       string                 `json:"id"`
	Name     string                 `json:"name"`
	Type     MiddlewareType         `json:"type"`
	Priority int                    `json:"priority"`
	Config   map[string]interface{} `json:"config"`
	Enabled  bool                   `json:"enabled"`
}

type MiddlewareResult struct {
	Continue bool                   `json:"continue"`
	Error    error                  `json:"error"`
	Data     map[string]interface{} `json:"data"`
	Headers  map[string]string      `json:"headers"`
}

// Webhook and callback types
type WebhookConfig struct {
	URL         string            `json:"url"`
	Method      string            `json:"method"`
	Headers     map[string]string `json:"headers"`
	Secret      string            `json:"secret"`
	Timeout     time.Duration     `json:"timeout"`
	RetryPolicy *RetryPolicy      `json:"retry_policy"`
}

type WebhookReceiver struct {
	ID          string                 `json:"id"`
	Path        string                 `json:"path"`
	Method      string                 `json:"method"`
	Secret      string                 `json:"secret"`
	Handler     string                 `json:"handler"`
	Config      map[string]interface{} `json:"config"`
	Middlewares []string               `json:"middlewares"`
}

type CallbackData struct {
	ID          string                 `json:"id"`
	WorkflowID  string                 `json:"workflow_id"`
	ExecutionID string                 `json:"execution_id"`
	NodeID      string                 `json:"node_id"`
	Data        map[string]interface{} `json:"data"`
	Headers     map[string]string      `json:"headers"`
	Timestamp   time.Time              `json:"timestamp"`
}

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

	// Node types
	NodeTypeTask      NodeType = "task"
	NodeTypeAPI       NodeType = "api"
	NodeTypeTransform NodeType = "transform"
	NodeTypeDecision  NodeType = "decision"
	NodeTypeHumanTask NodeType = "human_task"
	NodeTypeTimer     NodeType = "timer"
	NodeTypeLoop      NodeType = "loop"
	NodeTypeParallel  NodeType = "parallel"
	NodeTypeDatabase  NodeType = "database"
	NodeTypeEmail     NodeType = "email"
	NodeTypeWebhook   NodeType = "webhook"
	NodeTypeSubDAG    NodeType = "sub_dag"
	NodeTypeHTML      NodeType = "html"
	NodeTypeSMS       NodeType = "sms"
	NodeTypeAuth      NodeType = "auth"
	NodeTypeValidator NodeType = "validator"
	NodeTypeRouter    NodeType = "router"
	NodeTypeNotify    NodeType = "notify"
	NodeTypeStorage   NodeType = "storage"
	NodeTypeWebhookRx NodeType = "webhook_receiver"

	// Priorities
	PriorityLow      Priority = "low"
	PriorityMedium   Priority = "medium"
	PriorityHigh     Priority = "high"
	PriorityCritical Priority = "critical"

	// User roles
	UserRoleAdmin    UserRole = "admin"
	UserRoleManager  UserRole = "manager"
	UserRoleOperator UserRole = "operator"
	UserRoleViewer   UserRole = "viewer"
	UserRoleGuest    UserRole = "guest"

	// Permission actions
	PermissionRead    PermissionAction = "read"
	PermissionWrite   PermissionAction = "write"
	PermissionExecute PermissionAction = "execute"
	PermissionDelete  PermissionAction = "delete"
	PermissionAdmin   PermissionAction = "admin"

	// Middleware types
	MiddlewareAuth      MiddlewareType = "auth"
	MiddlewareLogging   MiddlewareType = "logging"
	MiddlewareRateLimit MiddlewareType = "rate_limit"
	MiddlewareValidate  MiddlewareType = "validate"
	MiddlewareTransform MiddlewareType = "transform"
	MiddlewareCustom    MiddlewareType = "custom"
)

// WorkflowDefinition represents a complete workflow
type WorkflowDefinition struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Version     string                 `json:"version"`
	Status      WorkflowStatus         `json:"status"`
	Tags        []string               `json:"tags"`
	Category    string                 `json:"category"`
	Owner       string                 `json:"owner"`
	Nodes       []WorkflowNode         `json:"nodes"`
	Edges       []WorkflowEdge         `json:"edges"`
	Variables   map[string]Variable    `json:"variables"`
	Config      WorkflowConfig         `json:"config"`
	Metadata    map[string]interface{} `json:"metadata"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	CreatedBy   string                 `json:"created_by"`
	UpdatedBy   string                 `json:"updated_by"`
}

// WorkflowNode represents a single node in the workflow
type WorkflowNode struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Type        NodeType               `json:"type"`
	Description string                 `json:"description"`
	Config      NodeConfig             `json:"config"`
	Position    Position               `json:"position"`
	Timeout     *time.Duration         `json:"timeout,omitempty"`
	RetryPolicy *RetryPolicy           `json:"retry_policy,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// NodeConfig holds configuration for different node types
type NodeConfig struct {
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
	Condition     string `json:"condition,omitempty"`
	DecisionRules []Rule `json:"decision_rules,omitempty"`

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

	// Validator node fields
	ValidationType  string           `json:"validation_type,omitempty"`
	ValidationRules []ValidationRule `json:"validation_rules,omitempty"`

	// Router node fields
	RoutingRules []RoutingRule `json:"routing_rules,omitempty"`
	DefaultRoute string        `json:"default_route,omitempty"`

	// Storage node fields
	StorageType      string            `json:"storage_type,omitempty"`
	StorageOperation string            `json:"storage_operation,omitempty"`
	StorageKey       string            `json:"storage_key,omitempty"`
	StoragePath      string            `json:"storage_path,omitempty"`
	StorageConfig    map[string]string `json:"storage_config,omitempty"`

	// Notification node fields
	NotifyType             string   `json:"notify_type,omitempty"`
	NotificationType       string   `json:"notification_type,omitempty"`
	NotificationRecipients []string `json:"notification_recipients,omitempty"`
	NotificationMessage    string   `json:"notification_message,omitempty"`
	Recipients             []string `json:"recipients,omitempty"`
	Channel                string   `json:"channel,omitempty"`

	// Webhook receiver fields
	ListenPath        string                 `json:"listen_path,omitempty"`
	Secret            string                 `json:"secret,omitempty"`
	WebhookSecret     string                 `json:"webhook_secret,omitempty"`
	WebhookSignature  string                 `json:"webhook_signature,omitempty"`
	WebhookTransforms map[string]interface{} `json:"webhook_transforms,omitempty"`
	Timeout           time.Duration          `json:"timeout,omitempty"`

	// Custom configuration
	Custom map[string]interface{} `json:"custom,omitempty"`
}

// ValidationRule for validator nodes
type ValidationRule struct {
	Field     string      `json:"field"`
	Type      string      `json:"type"` // "string", "number", "email", "regex", "required"
	Required  bool        `json:"required"`
	MinLength int         `json:"min_length,omitempty"`
	MaxLength int         `json:"max_length,omitempty"`
	Min       *float64    `json:"min,omitempty"`
	Max       *float64    `json:"max,omitempty"`
	Pattern   string      `json:"pattern,omitempty"`
	Value     interface{} `json:"value,omitempty"`
	Message   string      `json:"message,omitempty"`
}

// RoutingRule for router nodes
type RoutingRule struct {
	Condition   string `json:"condition"`
	Destination string `json:"destination"`
	Priority    int    `json:"priority"`
	Weight      int    `json:"weight"`
	IsDefault   bool   `json:"is_default"`
}

// Rule for decision nodes
type Rule struct {
	Condition string      `json:"condition"`
	Output    interface{} `json:"output"`
	NextNode  string      `json:"next_node,omitempty"`
}

// WorkflowEdge represents a connection between nodes
type WorkflowEdge struct {
	ID        string                 `json:"id"`
	FromNode  string                 `json:"from_node"`
	ToNode    string                 `json:"to_node"`
	Condition string                 `json:"condition,omitempty"`
	Priority  int                    `json:"priority"`
	Label     string                 `json:"label,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// Variable definition for workflow
type Variable struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	DefaultValue interface{} `json:"default_value"`
	Required     bool        `json:"required"`
	Description  string      `json:"description"`
}

// WorkflowConfig holds configuration for the entire workflow
type WorkflowConfig struct {
	Timeout       *time.Duration `json:"timeout,omitempty"`
	MaxRetries    int            `json:"max_retries"`
	Priority      Priority       `json:"priority"`
	Concurrency   int            `json:"concurrency"`
	EnableAudit   bool           `json:"enable_audit"`
	EnableMetrics bool           `json:"enable_metrics"`
	Notifications []string       `json:"notifications"`
	ErrorHandling ErrorHandling  `json:"error_handling"`
}

// ErrorHandling configuration
type ErrorHandling struct {
	OnFailure string `json:"on_failure"` // "stop", "continue", "retry"
	MaxErrors int    `json:"max_errors"`
	Rollback  bool   `json:"rollback"`
}

// Execution represents a workflow execution instance
type Execution struct {
	ID              string                 `json:"id"`
	WorkflowID      string                 `json:"workflow_id"`
	WorkflowVersion string                 `json:"workflow_version"`
	Status          ExecutionStatus        `json:"status"`
	Input           map[string]interface{} `json:"input"`
	Output          map[string]interface{} `json:"output"`
	Context         ExecutionContext       `json:"context"`
	CurrentNode     string                 `json:"current_node"`
	ExecutedNodes   []ExecutedNode         `json:"executed_nodes"`
	Error           string                 `json:"error,omitempty"`
	StartedAt       time.Time              `json:"started_at"`
	CompletedAt     *time.Time             `json:"completed_at,omitempty"`
	UpdatedAt       time.Time              `json:"updated_at"`
	Priority        Priority               `json:"priority"`
	Owner           string                 `json:"owner"`
	TriggeredBy     string                 `json:"triggered_by"`
	ParentExecution string                 `json:"parent_execution,omitempty"`
}

// ExecutionContext holds runtime context
type ExecutionContext struct {
	Variables   map[string]interface{} `json:"variables"`
	Secrets     map[string]string      `json:"secrets,omitempty"`
	Metadata    map[string]interface{} `json:"metadata"`
	Trace       []TraceEntry           `json:"trace"`
	Checkpoints []Checkpoint           `json:"checkpoints"`
}

// TraceEntry for execution tracing
type TraceEntry struct {
	Timestamp time.Time   `json:"timestamp"`
	NodeID    string      `json:"node_id"`
	Event     string      `json:"event"`
	Data      interface{} `json:"data,omitempty"`
}

// Checkpoint for execution recovery
type Checkpoint struct {
	ID        string                 `json:"id"`
	NodeID    string                 `json:"node_id"`
	Timestamp time.Time              `json:"timestamp"`
	State     map[string]interface{} `json:"state"`
}

// ExecutedNode tracks execution of individual nodes
type ExecutedNode struct {
	NodeID      string                 `json:"node_id"`
	Status      ExecutionStatus        `json:"status"`
	Input       map[string]interface{} `json:"input"`
	Output      map[string]interface{} `json:"output"`
	Error       string                 `json:"error,omitempty"`
	StartedAt   time.Time              `json:"started_at"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
	Duration    time.Duration          `json:"duration"`
	RetryCount  int                    `json:"retry_count"`
	Logs        []LogEntry             `json:"logs"`
}

// LogEntry for node execution logs
type LogEntry struct {
	Timestamp time.Time   `json:"timestamp"`
	Level     string      `json:"level"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
}

// Supporting types
type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	Delay       time.Duration `json:"delay"`
	Backoff     string        `json:"backoff"` // "linear", "exponential", "fixed"
	MaxDelay    time.Duration `json:"max_delay,omitempty"`
}

// Filter types
type WorkflowFilter struct {
	Status      []WorkflowStatus `json:"status"`
	Category    []string         `json:"category"`
	Owner       []string         `json:"owner"`
	Tags        []string         `json:"tags"`
	CreatedFrom *time.Time       `json:"created_from"`
	CreatedTo   *time.Time       `json:"created_to"`
	Search      string           `json:"search"`
	Limit       int              `json:"limit"`
	Offset      int              `json:"offset"`
	SortBy      string           `json:"sort_by"`
	SortOrder   string           `json:"sort_order"`
}

type ExecutionFilter struct {
	WorkflowID  []string          `json:"workflow_id"`
	Status      []ExecutionStatus `json:"status"`
	Owner       []string          `json:"owner"`
	Priority    []Priority        `json:"priority"`
	StartedFrom *time.Time        `json:"started_from"`
	StartedTo   *time.Time        `json:"started_to"`
	Limit       int               `json:"limit"`
	Offset      int               `json:"offset"`
	SortBy      string            `json:"sort_by"`
	SortOrder   string            `json:"sort_order"`
}

type ProcessingContext struct {
	Node       *WorkflowNode
	Data       map[string]interface{}
	Variables  map[string]interface{}
	User       *User
	Middleware *MiddlewareManager
}

type ProcessingResult struct {
	Success bool                   `json:"success"`
	Data    map[string]interface{} `json:"data,omitempty"`
	Error   string                 `json:"error,omitempty"`
	Message string                 `json:"message,omitempty"`
}

// Core interfaces
type Processor interface {
	Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error)
}

type WorkflowRegistry interface {
	Store(ctx context.Context, definition *WorkflowDefinition) error
	Get(ctx context.Context, id string, version string) (*WorkflowDefinition, error)
	List(ctx context.Context, filter *WorkflowFilter) ([]*WorkflowDefinition, error)
	Delete(ctx context.Context, id string) error
	GetVersions(ctx context.Context, id string) ([]string, error)
}

type StateManager interface {
	CreateExecution(ctx context.Context, execution *Execution) error
	UpdateExecution(ctx context.Context, execution *Execution) error
	GetExecution(ctx context.Context, executionID string) (*Execution, error)
	ListExecutions(ctx context.Context, filter *ExecutionFilter) ([]*Execution, error)
	DeleteExecution(ctx context.Context, executionID string) error
	SaveCheckpoint(ctx context.Context, executionID string, checkpoint *Checkpoint) error
	GetCheckpoints(ctx context.Context, executionID string) ([]*Checkpoint, error)
}

type WorkflowExecutor interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
	Execute(ctx context.Context, definition *WorkflowDefinition, execution *Execution) error
	Cancel(ctx context.Context, executionID string) error
	Suspend(ctx context.Context, executionID string) error
	Resume(ctx context.Context, executionID string) error
}

type WorkflowScheduler interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context)
	ScheduleExecution(ctx context.Context, execution *Execution, delay time.Duration) error
	CancelScheduledExecution(ctx context.Context, executionID string) error
}

// Config for the workflow engine
type Config struct {
	MaxWorkers       int            `json:"max_workers"`
	ExecutionTimeout time.Duration  `json:"execution_timeout"`
	EnableMetrics    bool           `json:"enable_metrics"`
	EnableAudit      bool           `json:"enable_audit"`
	EnableTracing    bool           `json:"enable_tracing"`
	LogLevel         string         `json:"log_level"`
	Storage          StorageConfig  `json:"storage"`
	Security         SecurityConfig `json:"security"`
}

type StorageConfig struct {
	Type           string `json:"type"` // "memory", "database"
	ConnectionURL  string `json:"connection_url,omitempty"`
	MaxConnections int    `json:"max_connections"`
}

type SecurityConfig struct {
	EnableAuth     bool     `json:"enable_auth"`
	AllowedOrigins []string `json:"allowed_origins"`
	JWTSecret      string   `json:"jwt_secret,omitempty"`
	RequiredScopes []string `json:"required_scopes"`
}
