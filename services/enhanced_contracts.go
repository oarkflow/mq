package services

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/mq/dag"
)

// Enhanced service interfaces that integrate with workflow engine

// EnhancedValidation extends the base Validation with workflow support
type EnhancedValidation interface {
	Validation
	// Enhanced methods for workflow integration
	ValidateWorkflowInput(ctx context.Context, input map[string]interface{}, rules []*dag.WorkflowValidationRule) (ValidationResult, error)
	CreateValidationProcessor(rules []*dag.WorkflowValidationRule) (*dag.ValidatorProcessor, error)
}

// Enhanced validation result for workflow integration
type ValidationResult struct {
	Valid   bool                   `json:"valid"`
	Errors  map[string]string      `json:"errors,omitempty"`
	Data    map[string]interface{} `json:"data"`
	Message string                 `json:"message,omitempty"`
}

// Enhanced DAG Service for workflow engine integration
type EnhancedDAGService interface {
	// Original DAG methods
	CreateDAG(name, key string, options ...Option) (*dag.DAG, error)
	GetDAG(key string) *dag.DAG
	ListDAGs() map[string]*dag.DAG
	StoreDAG(key string, traditionalDAG *dag.DAG) error

	// Enhanced DAG methods with workflow engine
	CreateEnhancedDAG(name, key string, config *dag.EnhancedDAGConfig, options ...Option) (*dag.EnhancedDAG, error)
	GetEnhancedDAG(key string) *dag.EnhancedDAG
	ListEnhancedDAGs() map[string]*dag.EnhancedDAG
	StoreEnhancedDAG(key string, enhancedDAG *dag.EnhancedDAG) error

	// Workflow engine integration
	GetWorkflowEngine(dagKey string) *dag.WorkflowEngineManager
	CreateWorkflowFromHandler(handler EnhancedHandler) (*dag.WorkflowDefinition, error)
	ExecuteWorkflow(ctx context.Context, workflowID string, input map[string]interface{}) (*dag.ExecutionResult, error)
}

// Enhanced Handler that supports workflow engine features
type EnhancedHandler struct {
	// Original handler fields
	Key        string         `json:"key" yaml:"key"`
	Name       string         `json:"name" yaml:"name"`
	Debug      bool           `json:"debug" yaml:"debug"`
	DisableLog bool           `json:"disable_log" yaml:"disable_log"`
	Nodes      []EnhancedNode `json:"nodes" yaml:"nodes"`
	Edges      []Edge         `json:"edges" yaml:"edges"`
	Loops      []Edge         `json:"loops" yaml:"loops"`

	// Enhanced workflow fields
	WorkflowEnabled    bool                          `json:"workflow_enabled" yaml:"workflow_enabled"`
	WorkflowConfig     *dag.WorkflowEngineConfig     `json:"workflow_config" yaml:"workflow_config"`
	EnhancedConfig     *dag.EnhancedDAGConfig        `json:"enhanced_config" yaml:"enhanced_config"`
	WorkflowProcessors []WorkflowProcessorConfig     `json:"workflow_processors" yaml:"workflow_processors"`
	ValidationRules    []*dag.WorkflowValidationRule `json:"validation_rules" yaml:"validation_rules"`
	RoutingRules       []*dag.WorkflowRoutingRule    `json:"routing_rules" yaml:"routing_rules"`

	// Metadata and lifecycle
	Version     string         `json:"version" yaml:"version"`
	Description string         `json:"description" yaml:"description"`
	Tags        []string       `json:"tags" yaml:"tags"`
	Metadata    map[string]any `json:"metadata" yaml:"metadata"`
}

// Enhanced Node that supports workflow processors
type EnhancedNode struct {
	// Original node fields
	ID        string `json:"id" yaml:"id"`
	Name      string `json:"name" yaml:"name"`
	Node      string `json:"node" yaml:"node"`
	NodeKey   string `json:"node_key" yaml:"node_key"`
	FirstNode bool   `json:"first_node" yaml:"first_node"`

	// Enhanced workflow fields
	Type          dag.WorkflowNodeType   `json:"type" yaml:"type"`
	ProcessorType string                 `json:"processor_type" yaml:"processor_type"`
	Config        dag.WorkflowNodeConfig `json:"config" yaml:"config"`
	Dependencies  []string               `json:"dependencies" yaml:"dependencies"`
	RetryPolicy   *dag.RetryPolicy       `json:"retry_policy" yaml:"retry_policy"`
	Timeout       *string                `json:"timeout" yaml:"timeout"`

	// Conditional execution
	Conditions map[string]string `json:"conditions" yaml:"conditions"`

	// Workflow processor specific configs
	HTMLConfig      *HTMLProcessorConfig      `json:"html_config,omitempty" yaml:"html_config,omitempty"`
	SMSConfig       *SMSProcessorConfig       `json:"sms_config,omitempty" yaml:"sms_config,omitempty"`
	AuthConfig      *AuthProcessorConfig      `json:"auth_config,omitempty" yaml:"auth_config,omitempty"`
	ValidatorConfig *ValidatorProcessorConfig `json:"validator_config,omitempty" yaml:"validator_config,omitempty"`
	RouterConfig    *RouterProcessorConfig    `json:"router_config,omitempty" yaml:"router_config,omitempty"`
	StorageConfig   *StorageProcessorConfig   `json:"storage_config,omitempty" yaml:"storage_config,omitempty"`
	NotifyConfig    *NotifyProcessorConfig    `json:"notify_config,omitempty" yaml:"notify_config,omitempty"`
	WebhookConfig   *WebhookProcessorConfig   `json:"webhook_config,omitempty" yaml:"webhook_config,omitempty"`
}

// EnhancedEdge extends the base Edge with additional workflow features
type EnhancedEdge struct {
	Edge // Embed the original Edge
	// Enhanced workflow fields
	Conditions map[string]string `json:"conditions" yaml:"conditions"`
	Priority   int               `json:"priority" yaml:"priority"`
	Metadata   map[string]any    `json:"metadata" yaml:"metadata"`
}

// Workflow processor configurations
type WorkflowProcessorConfig struct {
	Type   string                 `json:"type" yaml:"type"`
	Config map[string]interface{} `json:"config" yaml:"config"`
}

type HTMLProcessorConfig struct {
	Template     string            `json:"template" yaml:"template"`
	TemplateFile string            `json:"template_file" yaml:"template_file"`
	OutputPath   string            `json:"output_path" yaml:"output_path"`
	Variables    map[string]string `json:"variables" yaml:"variables"`
}

type SMSProcessorConfig struct {
	Provider string   `json:"provider" yaml:"provider"`
	From     string   `json:"from" yaml:"from"`
	To       []string `json:"to" yaml:"to"`
	Message  string   `json:"message" yaml:"message"`
	Template string   `json:"template" yaml:"template"`
}

type AuthProcessorConfig struct {
	AuthType    string            `json:"auth_type" yaml:"auth_type"`
	Credentials map[string]string `json:"credentials" yaml:"credentials"`
	TokenExpiry string            `json:"token_expiry" yaml:"token_expiry"`
	Endpoint    string            `json:"endpoint" yaml:"endpoint"`
}

type ValidatorProcessorConfig struct {
	ValidationRules []*dag.WorkflowValidationRule `json:"validation_rules" yaml:"validation_rules"`
	Schema          map[string]interface{}        `json:"schema" yaml:"schema"`
	StrictMode      bool                          `json:"strict_mode" yaml:"strict_mode"`
}

type RouterProcessorConfig struct {
	RoutingRules []*dag.WorkflowRoutingRule `json:"routing_rules" yaml:"routing_rules"`
	DefaultRoute string                     `json:"default_route" yaml:"default_route"`
	Strategy     string                     `json:"strategy" yaml:"strategy"`
}

type StorageProcessorConfig struct {
	StorageType string            `json:"storage_type" yaml:"storage_type"`
	Operation   string            `json:"operation" yaml:"operation"`
	Key         string            `json:"key" yaml:"key"`
	Path        string            `json:"path" yaml:"path"`
	Config      map[string]string `json:"config" yaml:"config"`
}

type NotifyProcessorConfig struct {
	NotifyType string   `json:"notify_type" yaml:"notify_type"`
	Recipients []string `json:"recipients" yaml:"recipients"`
	Message    string   `json:"message" yaml:"message"`
	Template   string   `json:"template" yaml:"template"`
	Channel    string   `json:"channel" yaml:"channel"`
}

type WebhookProcessorConfig struct {
	ListenPath string                 `json:"listen_path" yaml:"listen_path"`
	Secret     string                 `json:"secret" yaml:"secret"`
	Signature  string                 `json:"signature" yaml:"signature"`
	Transforms map[string]interface{} `json:"transforms" yaml:"transforms"`
	Timeout    string                 `json:"timeout" yaml:"timeout"`
}

// Enhanced service manager
type EnhancedServiceManager interface {
	// Service lifecycle
	Initialize(config *EnhancedServiceConfig) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Health() map[string]interface{}

	// Enhanced DAG management
	RegisterEnhancedHandler(handler EnhancedHandler) error
	GetEnhancedHandler(key string) (EnhancedHandler, error)
	ListEnhancedHandlers() []EnhancedHandler

	// Workflow engine integration
	GetWorkflowEngine() *dag.WorkflowEngineManager
	ExecuteEnhancedWorkflow(ctx context.Context, key string, input map[string]interface{}) (*dag.ExecutionResult, error)

	// HTTP integration
	RegisterHTTPRoutes(app *fiber.App) error
	CreateAPIEndpoints(handlers []EnhancedHandler) error
}

// Enhanced service configuration
type EnhancedServiceConfig struct {
	// Basic config
	BrokerURL string `json:"broker_url" yaml:"broker_url"`
	Debug     bool   `json:"debug" yaml:"debug"`

	// Enhanced DAG config
	EnhancedDAGConfig *dag.EnhancedDAGConfig `json:"enhanced_dag_config" yaml:"enhanced_dag_config"`

	// Workflow engine config
	WorkflowEngineConfig *dag.WorkflowEngineConfig `json:"workflow_engine_config" yaml:"workflow_engine_config"`

	// HTTP config
	HTTPConfig *HTTPServiceConfig `json:"http_config" yaml:"http_config"`

	// Validation config
	ValidationConfig *ValidationServiceConfig `json:"validation_config" yaml:"validation_config"`
}

type HTTPServiceConfig struct {
	Port          string            `json:"port" yaml:"port"`
	Host          string            `json:"host" yaml:"host"`
	CORS          *CORSConfig       `json:"cors" yaml:"cors"`
	RateLimit     *RateLimitConfig  `json:"rate_limit" yaml:"rate_limit"`
	Auth          *AuthConfig       `json:"auth" yaml:"auth"`
	Middleware    []string          `json:"middleware" yaml:"middleware"`
	Headers       map[string]string `json:"headers" yaml:"headers"`
	EnableMetrics bool              `json:"enable_metrics" yaml:"enable_metrics"`
}

type CORSConfig struct {
	AllowOrigins []string `json:"allow_origins" yaml:"allow_origins"`
	AllowMethods []string `json:"allow_methods" yaml:"allow_methods"`
	AllowHeaders []string `json:"allow_headers" yaml:"allow_headers"`
}

type RateLimitConfig struct {
	Max        int    `json:"max" yaml:"max"`
	Expiration string `json:"expiration" yaml:"expiration"`
}

type AuthConfig struct {
	Type    string            `json:"type" yaml:"type"`
	Users   map[string]string `json:"users" yaml:"users"`
	Realm   string            `json:"realm" yaml:"realm"`
	Enabled bool              `json:"enabled" yaml:"enabled"`
}

type ValidationServiceConfig struct {
	StrictMode      bool     `json:"strict_mode" yaml:"strict_mode"`
	CustomRules     []string `json:"custom_rules" yaml:"custom_rules"`
	EnableCaching   bool     `json:"enable_caching" yaml:"enable_caching"`
	DefaultMessages bool     `json:"default_messages" yaml:"default_messages"`
}
