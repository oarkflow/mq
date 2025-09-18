package main

import (
	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/mq/dag"
)

// AppConfiguration represents the complete JSON configuration for an application
type AppConfiguration struct {
	App            AppMetadata                `json:"app"`
	WorkflowEngine *WorkflowEngineConfig      `json:"workflow_engine,omitempty"`
	Routes         []RouteConfig              `json:"routes"`
	Middleware     []MiddlewareConfig         `json:"middleware"`
	Templates      map[string]TemplateConfig  `json:"templates"`
	Workflows      []WorkflowConfig           `json:"workflows"`
	Data           map[string]any             `json:"data"`
	Functions      map[string]FunctionConfig  `json:"functions"`
	Validators     map[string]ValidatorConfig `json:"validators"`
}

// AppMetadata contains basic app information
type AppMetadata struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
	Port        string `json:"port"`
	Host        string `json:"host"`
}

// WorkflowEngineConfig contains workflow engine configuration
type WorkflowEngineConfig struct {
	MaxWorkers       int            `json:"max_workers,omitempty"`
	ExecutionTimeout string         `json:"execution_timeout,omitempty"`
	EnableMetrics    bool           `json:"enable_metrics,omitempty"`
	EnableAudit      bool           `json:"enable_audit,omitempty"`
	EnableTracing    bool           `json:"enable_tracing,omitempty"`
	LogLevel         string         `json:"log_level,omitempty"`
	Storage          StorageConfig  `json:"storage,omitempty"`
	Security         SecurityConfig `json:"security,omitempty"`
}

// StorageConfig contains storage configuration
type StorageConfig struct {
	Type           string `json:"type,omitempty"`
	MaxConnections int    `json:"max_connections,omitempty"`
}

// SecurityConfig contains security configuration
type SecurityConfig struct {
	EnableAuth     bool     `json:"enable_auth,omitempty"`
	AllowedOrigins []string `json:"allowed_origins,omitempty"`
}

// RouteConfig defines HTTP routes
type RouteConfig struct {
	Path       string            `json:"path"`
	Method     string            `json:"method"`
	Handler    HandlerConfig     `json:"handler"`
	Middleware []string          `json:"middleware,omitempty"`
	Template   string            `json:"template,omitempty"`
	Variables  map[string]string `json:"variables,omitempty"`
	Auth       *AuthConfig       `json:"auth,omitempty"`
	Response   *ResponseConfig   `json:"response,omitempty"`
}

// ResponseConfig defines response handling
type ResponseConfig struct {
	Type     string `json:"type"` // "json", "html", "text"
	Template string `json:"template,omitempty"`
}

// HandlerConfig defines how to handle a route
type HandlerConfig struct {
	Type           string               `json:"type"` // "workflow", "template", "function", "redirect"
	Target         string               `json:"target"`
	Template       string               `json:"template,omitempty"`
	Input          map[string]any       `json:"input,omitempty"`
	Output         map[string]any       `json:"output,omitempty"`
	ErrorHandling  *ErrorHandlingConfig `json:"error_handling,omitempty"`
	Authentication *AuthConfig          `json:"authentication,omitempty"`
	Validation     []string             `json:"validation,omitempty"`
}

// ErrorHandlingConfig defines error handling behavior
type ErrorHandlingConfig struct {
	Retry      *RetryConfig `json:"retry,omitempty"`
	Fallback   string       `json:"fallback,omitempty"`
	StatusCode int          `json:"status_code,omitempty"`
	Message    string       `json:"message,omitempty"`
}

// RetryConfig defines retry behavior
type RetryConfig struct {
	MaxAttempts int    `json:"max_attempts"`
	Delay       string `json:"delay"`
	Backoff     string `json:"backoff,omitempty"`
}

// AuthConfig defines authentication requirements
type AuthConfig struct {
	Required bool     `json:"required"`
	Type     string   `json:"type,omitempty"`
	Roles    []string `json:"roles,omitempty"`
	Scopes   []string `json:"scopes,omitempty"`
	Redirect string   `json:"redirect,omitempty"`
}

// MiddlewareConfig defines middleware
type MiddlewareConfig struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Type      string         `json:"type"`
	Priority  int            `json:"priority"`
	Config    map[string]any `json:"config,omitempty"`
	Functions []string       `json:"functions,omitempty"`
	Enabled   bool           `json:"enabled"`
}

// TemplateConfig defines templates
type TemplateConfig struct {
	Type         string            `json:"type"` // "html", "text", "json"
	Content      string            `json:"content,omitempty"`
	Template     string            `json:"template,omitempty"` // Alternative field name for content
	File         string            `json:"file,omitempty"`
	Variables    map[string]any    `json:"variables,omitempty"`
	Data         map[string]any    `json:"data,omitempty"`
	Partials     map[string]string `json:"partials,omitempty"`
	Helpers      []string          `json:"helpers,omitempty"`
	CacheEnabled bool              `json:"cache_enabled"`
}

// WorkflowConfig defines workflows
type WorkflowConfig struct {
	ID           string              `json:"id"`
	Name         string              `json:"name"`
	Description  string              `json:"description,omitempty"`
	Version      string              `json:"version,omitempty"`
	Nodes        []NodeConfig        `json:"nodes"`
	Edges        []EdgeConfig        `json:"edges"`
	Variables    map[string]any      `json:"variables,omitempty"`
	Triggers     []TriggerConfig     `json:"triggers,omitempty"`
	SubWorkflows []SubWorkflowConfig `json:"sub_workflows,omitempty"`
	JSONSchema   *JSONSchemaConfig   `json:"json_schema,omitempty"`
}

// NodeConfig defines workflow nodes
type NodeConfig struct {
	ID            string               `json:"id"`
	Type          string               `json:"type"`
	Name          string               `json:"name"`
	Description   string               `json:"description,omitempty"`
	Function      string               `json:"function,omitempty"`
	SubWorkflow   string               `json:"sub_workflow,omitempty"`
	Input         map[string]any       `json:"input,omitempty"`
	Output        map[string]any       `json:"output,omitempty"`
	InputMapping  map[string]any       `json:"input_mapping,omitempty"`
	OutputMapping map[string]any       `json:"output_mapping,omitempty"`
	Config        map[string]any       `json:"config,omitempty"`
	Conditions    []ConditionConfig    `json:"conditions,omitempty"`
	ErrorHandling *ErrorHandlingConfig `json:"error_handling,omitempty"`
	Timeout       string               `json:"timeout,omitempty"`
	Retry         *RetryConfig         `json:"retry,omitempty"`
}

// EdgeConfig defines workflow edges
type EdgeConfig struct {
	ID          string            `json:"id"`
	From        string            `json:"from"`
	To          string            `json:"to"`
	Condition   string            `json:"condition,omitempty"`
	Variables   map[string]string `json:"variables,omitempty"`
	Transform   string            `json:"transform,omitempty"`
	Description string            `json:"description,omitempty"`
}

// ConditionConfig defines conditional logic
type ConditionConfig struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    any    `json:"value"`
	Logic    string `json:"logic,omitempty"` // "AND", "OR"
}

// TriggerConfig defines workflow triggers
type TriggerConfig struct {
	Type       string            `json:"type"` // "http", "cron", "event"
	Config     map[string]any    `json:"config"`
	Enabled    bool              `json:"enabled"`
	Conditions []ConditionConfig `json:"conditions,omitempty"`
}

// SubWorkflowConfig defines sub-workflow mappings
type SubWorkflowConfig struct {
	ID            string         `json:"id"`
	WorkflowID    string         `json:"workflow_id"`
	InputMapping  map[string]any `json:"input_mapping,omitempty"`
	OutputMapping map[string]any `json:"output_mapping,omitempty"`
}

// JSONSchemaConfig defines JSON schema validation
type JSONSchemaConfig struct {
	Input  map[string]any `json:"input,omitempty"`
	Output map[string]any `json:"output,omitempty"`
}

// FunctionConfig defines custom functions with complete flexibility
type FunctionConfig struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Type        string         `json:"type"` // "http", "expression", "template", "js", "builtin"
	Handler     string         `json:"handler,omitempty"`
	Method      string         `json:"method,omitempty"`     // For HTTP functions
	URL         string         `json:"url,omitempty"`        // For HTTP functions
	Headers     map[string]any `json:"headers,omitempty"`    // For HTTP functions
	Body        string         `json:"body,omitempty"`       // For HTTP request body template
	Code        string         `json:"code,omitempty"`       // For custom code functions
	Template    string         `json:"template,omitempty"`   // For template functions
	Expression  string         `json:"expression,omitempty"` // For expression functions
	Parameters  map[string]any `json:"parameters,omitempty"` // Generic parameters
	Returns     map[string]any `json:"returns,omitempty"`    // Generic return definition
	Response    map[string]any `json:"response,omitempty"`   // Response structure
	Config      map[string]any `json:"config,omitempty"`
	Async       bool           `json:"async"`
	Timeout     string         `json:"timeout,omitempty"`
}

// Note: ParameterConfig removed - using generic map[string]any for parameters

// ValidatorConfig defines validation rules with complete flexibility
type ValidatorConfig struct {
	ID         string            `json:"id"`
	Name       string            `json:"name,omitempty"`
	Type       string            `json:"type"` // "jsonschema", "custom", "regex", "builtin"
	Field      string            `json:"field,omitempty"`
	Schema     any               `json:"schema,omitempty"`
	Rules      []ValidationRule  `json:"rules,omitempty"` // Array of validation rules
	Messages   map[string]string `json:"messages,omitempty"`
	Expression string            `json:"expression,omitempty"` // For expression-based validation
	Config     map[string]any    `json:"config,omitempty"`
	StrictMode bool              `json:"strict_mode"`
	AllowEmpty bool              `json:"allow_empty"`
}

// ValidationRule defines individual validation rules with flexibility
type ValidationRule struct {
	Field      string            `json:"field,omitempty"`
	Type       string            `json:"type"`
	Required   bool              `json:"required,omitempty"`
	Value      any               `json:"value,omitempty"` // Generic value field for min/max, patterns, etc.
	Min        any               `json:"min,omitempty"`
	Max        any               `json:"max,omitempty"`
	Pattern    string            `json:"pattern,omitempty"`
	Expression string            `json:"expression,omitempty"` // For custom expressions
	CustomRule string            `json:"custom_rule,omitempty"`
	Message    string            `json:"message,omitempty"`
	Config     map[string]any    `json:"config,omitempty"`
	Conditions []ConditionConfig `json:"conditions,omitempty"`
}

// Generic runtime types for the JSON engine
type JSONEngine struct {
	app                  *fiber.App
	workflowEngine       *dag.WorkflowEngineManager
	workflowEngineConfig *WorkflowEngineConfig
	config               *AppConfiguration
	templates            map[string]*Template
	workflows            map[string]*Workflow
	functions            map[string]*Function
	validators           map[string]*Validator
	middleware           map[string]*Middleware
	data                 map[string]any
	genericData          map[string]any // For any custom data structures
}

type Template struct {
	ID       string
	Config   TemplateConfig
	Compiled any
}

type Workflow struct {
	ID      string
	Config  WorkflowConfig
	Nodes   map[string]*Node
	Edges   []*Edge
	Runtime *WorkflowRuntime
}

type Node struct {
	ID       string
	Config   NodeConfig
	Function *Function
	Inputs   map[string]any
	Outputs  map[string]any
}

type Edge struct {
	ID     string
	Config EdgeConfig
	From   *Node
	To     *Node
}

// Function represents a compiled function with generic handler
type Function struct {
	ID      string
	Config  FunctionConfig
	Handler any            // Can be any type of handler
	Runtime map[string]any // Runtime state/context
}

// Validator represents a compiled validator with generic rules
type Validator struct {
	ID      string
	Config  ValidatorConfig
	Rules   []ValidationRule // Array of validation rules to match ValidatorConfig
	Runtime map[string]any   // Runtime context
}

type Middleware struct {
	ID      string
	Config  MiddlewareConfig
	Handler fiber.Handler
}

type WorkflowRuntime struct {
	Context   map[string]any
	Variables map[string]any
	Status    string
	Error     error
}

// ExecutionContext for runtime with complete flexibility
type ExecutionContext struct {
	Request    *fiber.Ctx
	Data       map[string]any
	Variables  map[string]any
	Session    map[string]any
	User       map[string]any
	Workflow   *Workflow
	Node       *Node
	Functions  map[string]*Function
	Validators map[string]*Validator
	Config     *AppConfiguration // Access to full config
	Runtime    map[string]any    // Runtime state
	Context    map[string]any    // Additional context data
}
