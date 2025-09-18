package main

import (
	"github.com/gofiber/fiber/v2"
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
	ID          string                 `json:"id"`
	Method      string                 `json:"method"`
	Path        string                 `json:"path"`
	Description string                 `json:"description"`
	Middleware  []string               `json:"middleware"`
	Handler     HandlerConfig          `json:"handler"`
	Auth        *AuthConfig            `json:"auth,omitempty"`
	Response    ResponseConfig         `json:"response"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
}

// HandlerConfig defines route handler behavior
type HandlerConfig struct {
	Type       string                 `json:"type"` // "template", "workflow", "function", "redirect", "static"
	Target     string                 `json:"target"`
	Input      map[string]interface{} `json:"input,omitempty"`
	Transform  []TransformConfig      `json:"transform,omitempty"`
	Conditions []ConditionConfig      `json:"conditions,omitempty"`
}

// MiddlewareConfig defines middleware behavior
type MiddlewareConfig struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"` // "auth", "cors", "logging", "ratelimit", "custom"
	Priority   int                    `json:"priority"`
	Enabled    bool                   `json:"enabled"`
	Config     map[string]interface{} `json:"config"`
	Conditions []ConditionConfig      `json:"conditions,omitempty"`
	Functions  []string               `json:"functions,omitempty"`
}

// TemplateConfig defines HTML templates
type TemplateConfig struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"` // "html", "json", "xml", "text"
	Template   string                 `json:"template"`
	Layout     string                 `json:"layout,omitempty"`
	Partials   []string               `json:"partials,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty"`
	Scripts    []ScriptConfig         `json:"scripts,omitempty"`
	Styles     []StyleConfig          `json:"styles,omitempty"`
	Components []ComponentConfig      `json:"components,omitempty"`
}

// WorkflowConfig defines workflow nodes and execution
type WorkflowConfig struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Version     string                 `json:"version"`
	Nodes       []NodeConfig           `json:"nodes"`
	Edges       []EdgeConfig           `json:"edges"`
	Variables   map[string]interface{} `json:"variables"`
	Triggers    []TriggerConfig        `json:"triggers"`
	Options     ExecutionOptions       `json:"options"`
}

// NodeConfig defines workflow node behavior
type NodeConfig struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Type        string                 `json:"type"` // "function", "http", "template", "validator", "transformer", "condition", "loop", "parallel"
	Description string                 `json:"description"`
	Function    string                 `json:"function,omitempty"`
	Input       map[string]interface{} `json:"input,omitempty"`
	Output      map[string]interface{} `json:"output,omitempty"`
	Config      map[string]interface{} `json:"config,omitempty"`
	Conditions  []ConditionConfig      `json:"conditions,omitempty"`
	Retry       *RetryConfig           `json:"retry,omitempty"`
	Timeout     string                 `json:"timeout,omitempty"`
}

// EdgeConfig defines connections between nodes
type EdgeConfig struct {
	ID         string            `json:"id"`
	From       string            `json:"from"`
	To         string            `json:"to"`
	Conditions []ConditionConfig `json:"conditions,omitempty"`
	Transform  *TransformConfig  `json:"transform,omitempty"`
}

// FunctionConfig defines reusable functions
type FunctionConfig struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"` // "js", "expression", "http", "sql", "template"
	Code       string                 `json:"code,omitempty"`
	URL        string                 `json:"url,omitempty"`
	Method     string                 `json:"method,omitempty"`
	Headers    map[string]string      `json:"headers,omitempty"`
	Body       string                 `json:"body,omitempty"`
	Query      string                 `json:"query,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
	Response   ResponseConfig         `json:"response,omitempty"`
}

// ValidatorConfig defines validation rules
type ValidatorConfig struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"` // "required", "email", "phone", "regex", "length", "range", "custom"
	Field      string                 `json:"field"`
	Rules      []ValidationRule       `json:"rules"`
	Message    string                 `json:"message"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

// Supporting types
type AuthConfig struct {
	Required    bool                   `json:"required"`
	Type        string                 `json:"type"` // "session", "token", "basic", "oauth"
	Provider    string                 `json:"provider,omitempty"`
	Redirect    string                 `json:"redirect,omitempty"`
	Permissions []string               `json:"permissions,omitempty"`
	Config      map[string]interface{} `json:"config,omitempty"`
}

type ResponseConfig struct {
	Type        string                 `json:"type"` // "json", "html", "redirect", "file", "stream"
	Template    string                 `json:"template,omitempty"`
	Data        map[string]interface{} `json:"data,omitempty"`
	Status      int                    `json:"status,omitempty"`
	Headers     map[string]string      `json:"headers,omitempty"`
	ContentType string                 `json:"content_type,omitempty"`
}

type TransformConfig struct {
	Type       string                 `json:"type"` // "map", "filter", "reduce", "expression", "template"
	Expression string                 `json:"expression,omitempty"`
	Template   string                 `json:"template,omitempty"`
	Source     string                 `json:"source,omitempty"`
	Target     string                 `json:"target,omitempty"`
	Function   string                 `json:"function,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

type ConditionConfig struct {
	Type       string      `json:"type"` // "equals", "contains", "exists", "expression", "function"
	Field      string      `json:"field,omitempty"`
	Value      interface{} `json:"value,omitempty"`
	Expression string      `json:"expression,omitempty"`
	Function   string      `json:"function,omitempty"`
	Operator   string      `json:"operator,omitempty"` // "and", "or", "not"
}

type ScriptConfig struct {
	Type    string `json:"type"` // "inline", "file", "url"
	Content string `json:"content"`
	Src     string `json:"src,omitempty"`
}

type StyleConfig struct {
	Type    string `json:"type"` // "inline", "file", "url"
	Content string `json:"content"`
	Href    string `json:"href,omitempty"`
}

type ComponentConfig struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Template string                 `json:"template"`
	Data     map[string]interface{} `json:"data,omitempty"`
	Props    map[string]interface{} `json:"props,omitempty"`
}

type TriggerConfig struct {
	Type       string            `json:"type"` // "http", "schedule", "event", "webhook"
	Schedule   string            `json:"schedule,omitempty"`
	Event      string            `json:"event,omitempty"`
	Path       string            `json:"path,omitempty"`
	Method     string            `json:"method,omitempty"`
	Conditions []ConditionConfig `json:"conditions,omitempty"`
}

type ExecutionOptions struct {
	Async    bool         `json:"async"`
	Timeout  string       `json:"timeout,omitempty"`
	Retry    *RetryConfig `json:"retry,omitempty"`
	Priority string       `json:"priority,omitempty"`
	MaxNodes int          `json:"max_nodes,omitempty"`
}

type RetryConfig struct {
	MaxAttempts int    `json:"max_attempts"`
	Delay       string `json:"delay"`
	BackoffType string `json:"backoff_type"` // "fixed", "exponential", "linear"
}

type ValidationRule struct {
	Type       string                 `json:"type"`
	Value      interface{}            `json:"value,omitempty"`
	Message    string                 `json:"message"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

// Runtime types for the JSON engine
type JSONEngine struct {
	app        *fiber.App
	config     *AppConfiguration
	templates  map[string]*Template
	workflows  map[string]*Workflow
	functions  map[string]*Function
	validators map[string]*Validator
	middleware map[string]*Middleware
	data       map[string]interface{}
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
