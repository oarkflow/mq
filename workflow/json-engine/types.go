package main

import (
	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/mq/workflow"
)

// AppConfiguration represents the complete JSON configuration for an application
type AppConfiguration struct {
	App        AppMetadata                `json:"app"`
	Routes     []RouteConfig              `json:"routes"`
	Middleware []MiddlewareConfig         `json:"middleware"`
	Templates  map[string]TemplateConfig  `json:"templates"`
	Workflows  []WorkflowConfig           `json:"workflows"`
	Data       map[string]interface{}     `json:"data"`
	Functions  map[string]FunctionConfig  `json:"functions"`
	Validators map[string]ValidatorConfig `json:"validators"`
}

// AppMetadata contains basic app information
type AppMetadata struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
	Port        string `json:"port"`
	Host        string `json:"host"`
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
	Type           string                 `json:"type"` // "workflow", "template", "function", "redirect"
	Target         string                 `json:"target"`
	Input          map[string]interface{} `json:"input,omitempty"`
	Output         map[string]interface{} `json:"output,omitempty"`
	ErrorHandling  *ErrorHandlingConfig   `json:"error_handling,omitempty"`
	Authentication *AuthConfig            `json:"authentication,omitempty"`
	Validation     []string               `json:"validation,omitempty"`
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
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Type      string                 `json:"type"`
	Priority  int                    `json:"priority"`
	Config    map[string]interface{} `json:"config,omitempty"`
	Functions []string               `json:"functions,omitempty"`
	Enabled   bool                   `json:"enabled"`
}

// TemplateConfig defines templates
type TemplateConfig struct {
	Type         string                 `json:"type"` // "html", "text", "json"
	Content      string                 `json:"content,omitempty"`
	Template     string                 `json:"template,omitempty"` // Alternative field name for content
	File         string                 `json:"file,omitempty"`
	Variables    map[string]interface{} `json:"variables,omitempty"`
	Data         map[string]interface{} `json:"data,omitempty"`
	Partials     map[string]string      `json:"partials,omitempty"`
	Helpers      []string               `json:"helpers,omitempty"`
	CacheEnabled bool                   `json:"cache_enabled"`
}

// WorkflowConfig defines workflows
type WorkflowConfig struct {
	ID           string                 `json:"id"`
	Name         string                 `json:"name"`
	Description  string                 `json:"description,omitempty"`
	Version      string                 `json:"version,omitempty"`
	Nodes        []NodeConfig           `json:"nodes"`
	Edges        []EdgeConfig           `json:"edges"`
	Variables    map[string]interface{} `json:"variables,omitempty"`
	Triggers     []TriggerConfig        `json:"triggers,omitempty"`
	SubWorkflows []SubWorkflowConfig    `json:"sub_workflows,omitempty"`
	JSONSchema   *JSONSchemaConfig      `json:"json_schema,omitempty"`
}

// NodeConfig defines workflow nodes
type NodeConfig struct {
	ID            string                 `json:"id"`
	Type          string                 `json:"type"`
	Name          string                 `json:"name"`
	Description   string                 `json:"description,omitempty"`
	Function      string                 `json:"function,omitempty"`
	SubWorkflow   string                 `json:"sub_workflow,omitempty"`
	Input         map[string]interface{} `json:"input,omitempty"`
	Output        map[string]interface{} `json:"output,omitempty"`
	InputMapping  map[string]interface{} `json:"input_mapping,omitempty"`
	OutputMapping map[string]interface{} `json:"output_mapping,omitempty"`
	Config        map[string]interface{} `json:"config,omitempty"`
	Conditions    []ConditionConfig      `json:"conditions,omitempty"`
	ErrorHandling *ErrorHandlingConfig   `json:"error_handling,omitempty"`
	Timeout       string                 `json:"timeout,omitempty"`
	Retry         *RetryConfig           `json:"retry,omitempty"`
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
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
	Logic    string      `json:"logic,omitempty"` // "AND", "OR"
}

// TriggerConfig defines workflow triggers
type TriggerConfig struct {
	Type       string                 `json:"type"` // "http", "cron", "event"
	Config     map[string]interface{} `json:"config"`
	Enabled    bool                   `json:"enabled"`
	Conditions []ConditionConfig      `json:"conditions,omitempty"`
}

// SubWorkflowConfig defines sub-workflow mappings
type SubWorkflowConfig struct {
	ID            string                 `json:"id"`
	WorkflowID    string                 `json:"workflow_id"`
	InputMapping  map[string]interface{} `json:"input_mapping,omitempty"`
	OutputMapping map[string]interface{} `json:"output_mapping,omitempty"`
}

// JSONSchemaConfig defines JSON schema validation
type JSONSchemaConfig struct {
	Input  map[string]interface{} `json:"input,omitempty"`
	Output map[string]interface{} `json:"output,omitempty"`
}

// FunctionConfig defines custom functions
type FunctionConfig struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Type        string                 `json:"type"` // "builtin", "custom", "external", "http"
	Handler     string                 `json:"handler,omitempty"`
	Method      string                 `json:"method,omitempty"`  // For HTTP functions
	URL         string                 `json:"url,omitempty"`     // For HTTP functions
	Headers     map[string]string      `json:"headers,omitempty"` // For HTTP functions
	Code        string                 `json:"code,omitempty"`    // For custom code functions
	Parameters  []ParameterConfig      `json:"parameters,omitempty"`
	Returns     []ParameterConfig      `json:"returns,omitempty"`
	Config      map[string]interface{} `json:"config,omitempty"`
	Async       bool                   `json:"async"`
	Timeout     string                 `json:"timeout,omitempty"`
}

// ParameterConfig defines function parameters
type ParameterConfig struct {
	Name        string      `json:"name"`
	Type        string      `json:"type"`
	Required    bool        `json:"required"`
	Default     interface{} `json:"default,omitempty"`
	Description string      `json:"description,omitempty"`
	Validation  []string    `json:"validation,omitempty"`
}

// ValidatorConfig defines validation rules
type ValidatorConfig struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"` // "jsonschema", "custom", "regex"
	Field      string            `json:"field,omitempty"`
	Schema     interface{}       `json:"schema,omitempty"`
	Rules      []ValidationRule  `json:"rules,omitempty"`
	Messages   map[string]string `json:"messages,omitempty"`
	StrictMode bool              `json:"strict_mode"`
	AllowEmpty bool              `json:"allow_empty"`
}

// ValidationRule defines individual validation rules
type ValidationRule struct {
	Field      string            `json:"field"`
	Type       string            `json:"type"`
	Required   bool              `json:"required"`
	Min        interface{}       `json:"min,omitempty"`
	Max        interface{}       `json:"max,omitempty"`
	Pattern    string            `json:"pattern,omitempty"`
	CustomRule string            `json:"custom_rule,omitempty"`
	Message    string            `json:"message,omitempty"`
	Conditions []ConditionConfig `json:"conditions,omitempty"`
}

// Provider configuration for SMS/communication services
type ProviderConfig struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Type        string                 `json:"type"` // "sms", "email", "push"
	Enabled     bool                   `json:"enabled"`
	Priority    int                    `json:"priority"`
	Config      map[string]interface{} `json:"config"`
	Countries   []string               `json:"countries,omitempty"`
	RateLimit   *RateLimitConfig       `json:"rate_limit,omitempty"`
	Costs       map[string]float64     `json:"costs,omitempty"`
	Features    []string               `json:"features,omitempty"`
	Reliability float64                `json:"reliability"`
}

// RateLimitConfig defines rate limiting
type RateLimitConfig struct {
	RequestsPerSecond int `json:"requests_per_second"`
	BurstSize         int `json:"burst_size"`
	WindowSize        int `json:"window_size"`
}

// Country configuration for routing
type CountryConfig struct {
	Code        string            `json:"code"`
	Name        string            `json:"name"`
	Providers   []string          `json:"providers"`
	DefaultRate float64           `json:"default_rate"`
	Regulations map[string]string `json:"regulations,omitempty"`
}

// Runtime types for the JSON engine
type JSONEngine struct {
	app            *fiber.App
	workflowEngine *workflow.WorkflowEngine
	config         *AppConfiguration
	templates      map[string]*Template
	workflows      map[string]*Workflow
	functions      map[string]*Function
	validators     map[string]*Validator
	middleware     map[string]*Middleware
	data           map[string]interface{}
}

type Template struct {
	ID       string
	Config   TemplateConfig
	Compiled interface{}
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
	Inputs   map[string]interface{}
	Outputs  map[string]interface{}
}

type Edge struct {
	ID     string
	Config EdgeConfig
	From   *Node
	To     *Node
}

type Function struct {
	ID      string
	Config  FunctionConfig
	Handler interface{}
}

type Validator struct {
	ID     string
	Config ValidatorConfig
	Rules  []ValidationRule
}

type Middleware struct {
	ID      string
	Config  MiddlewareConfig
	Handler fiber.Handler
}

type WorkflowRuntime struct {
	Context   map[string]interface{}
	Variables map[string]interface{}
	Status    string
	Error     error
}

// Execution context for runtime
type ExecutionContext struct {
	Request    *fiber.Ctx
	Data       map[string]interface{}
	Variables  map[string]interface{}
	Session    map[string]interface{}
	User       map[string]interface{}
	Workflow   *Workflow
	Node       *Node
	Functions  map[string]*Function
	Validators map[string]*Validator
}
