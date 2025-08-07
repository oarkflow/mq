package services

import (
	"github.com/oarkflow/json"
	v2 "github.com/oarkflow/jsonschema"

	"github.com/oarkflow/filters"

	"github.com/oarkflow/metadata"
)

type Storage struct {
	Name       string `json:"name" yaml:"name"`
	Key        string `json:"key" yaml:"key"`
	Driver     string `json:"driver" yaml:"driver"`
	Database   string `json:"database" yaml:"database"`
	Host       string `json:"host" yaml:"host"`
	Username   string `json:"username" yaml:"username"`
	Password   string `json:"password" yaml:"password"`
	Port       int    `json:"port" yaml:"port"`
	IndexCache int    `json:"index_cache" yaml:"index_cache"`
	BlockCache int    `json:"block_cache" yaml:"block_cache"`
	InMemory   bool   `json:"in_memory" yaml:"in_memory"`
}

type Cache struct {
	Name     string `json:"name" yaml:"name"`
	Key      string `json:"key" yaml:"key"`
	Driver   string `json:"driver" yaml:"driver"`
	Database string `json:"database" yaml:"database"`
	Host     string `json:"host" yaml:"host"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
	Prefix   string `json:"prefix" yaml:"prefix"`
	Port     int    `json:"port" yaml:"port"`
}

type Credentials struct {
	Databases []metadata.Config `json:"databases" yaml:"databases"`
	Storages  []Storage         `json:"storages" yaml:"storages"`
	Caches    []Cache           `json:"caches" yaml:"caches"`
}

type Core struct {
	Consts      map[string]any            `json:"consts" yaml:"consts"`
	Enums       map[string]map[string]any `json:"enums" yaml:"enums"`
	Credentials Credentials               `json:"credentials" yaml:"credentials"`
}

type Operation struct {
	Role    string   `json:"role" yaml:"role"`
	Actions []string `json:"actions" yaml:"actions"`
}

type Constraint struct {
	Indices     []metadata.Indices    `json:"indices" yaml:"indices"`
	ForeignKeys []metadata.ForeignKey `json:"foreign" yaml:"foreign"`
}

type Query struct {
	File   string `json:"file" yaml:"file"`
	String string `json:"string" yaml:"string"`
}

type Model struct {
	Name        string           `json:"name" yaml:"name"`
	OldName     string           `json:"old_name" yaml:"old_name"`
	Key         string           `json:"key" yaml:"key"`
	Title       string           `json:"title" yaml:"title"`
	Database    string           `json:"database" yaml:"database"`
	ModelType   string           `json:"model_type" yaml:"model_type"`
	StoreFields []string         `json:"store_fields" yaml:"store_fields"`
	IndexFields []string         `json:"index_fields" yaml:"index_fields"`
	Query       Query            `json:"query" yaml:"query"`
	Constraints Constraint       `json:"constraints" yaml:"constraints"`
	Fields      []metadata.Field `json:"fields" yaml:"fields"`
	Operations  []Operation      `json:"operations" yaml:"operations"`
	Fulltext    bool             `json:"fulltext" yaml:"fulltext"`
	Files       map[string]bool  `json:"files" yaml:"files"`
	SortField   string           `json:"sort_field" yaml:"sort_field"`
	SortOrder   string           `json:"sort_order" yaml:"sort_order"`
	RestApi     bool             `json:"rest_api" yaml:"rest_api"`
	Update      bool             `json:"update" yaml:"update"`
}

type Property struct {
	Properties           map[string]Property `json:"properties,omitempty" yaml:"properties,omitempty"`
	Items                *RequestSchema      `json:"items,omitempty" yaml:"items,omitempty"`
	Type                 any                 `json:"type,omitempty" yaml:"type,omitempty"`
	Default              any                 `json:"default" yaml:"default"`
	In                   string              `json:"in,omitempty" yaml:"in,omitempty"`
	AdditionalProperties bool                `json:"additionalProperties,omitempty" yaml:"additionalProperties,omitempty"`
}

func (p *Property) UnmarshalJSON(data []byte) error {
	type T Property
	var pr T
	err := json.Unmarshal(data, &pr)
	if err != nil {
		return err
	}
	p.Properties = pr.Properties
	p.Items = pr.Items
	p.In = pr.In
	p.AdditionalProperties = pr.AdditionalProperties
	switch pr.Type.(type) {
	case string:
		p.Type = pr.Type.(string)
	case []any:
		var v []string
		for _, i := range pr.Type.([]any) {
			v = append(v, i.(string))
		}
		p.Type = v
	}
	return nil
}

type Provider struct {
	Mapping       map[string]any `json:"mapping,omitempty" yaml:"mapping,omitempty"`
	UpdateMapping map[string]any `json:"update_mapping,omitempty" yaml:"update_mapping,omitempty"`
	InsertMapping map[string]any `json:"insert_mapping,omitempty" yaml:"insert_mapping,omitempty"`
	Defaults      map[string]any `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	ProviderType  string         `json:"provider_type,omitempty" yaml:"provider_type,omitempty"`
	Database      string         `json:"database,omitempty" yaml:"database,omitempty"`
	Source        string         `json:"source,omitempty" yaml:"source,omitempty"`
}

type Data struct {
	Mapping         map[string]string `json:"mapping,omitempty" yaml:"mapping,omitempty"`
	AdditionalData  map[string]any    `json:"additional_data,omitempty" yaml:"additional_data,omitempty"`
	GeneratedFields []string          `json:"generated_fields,omitempty" yaml:"generated_fields,omitempty"`
	Providers       []Provider        `json:"providers,omitempty" yaml:"providers,omitempty"`
}

type Node struct {
	Name      string `json:"name,omitempty" yaml:"name,omitempty"`
	ID        string `json:"id" yaml:"id"`
	Node      string `json:"node" yaml:"node"`
	Data      Data   `json:"data" yaml:"data"`
	FirstNode bool   `json:"first_node" yaml:"first_node"`
}

type Loop struct {
	Label  string   `json:"label" yaml:"label"`
	Source string   `json:"source" yaml:"source"`
	Target []string `json:"target" yaml:"target"`
}

type Edge struct {
	Label  string   `json:"label" yaml:"label"`
	Source string   `json:"source" yaml:"source"`
	Target []string `json:"target" yaml:"target"`
}

type Branch struct {
	ConditionalNodes map[string]string `json:"conditional_nodes" yaml:"conditional_nodes"`
	Key              string            `json:"key" yaml:"key"`
}

type Handler struct {
	Name     string   `json:"name" yaml:"name"`
	Key      string   `json:"key" yaml:"key"`
	Nodes    []Node   `json:"nodes,omitempty" yaml:"nodes,omitempty"`
	Edges    []Edge   `json:"edges,omitempty" yaml:"edges,omitempty"`
	Branches []Branch `json:"branches,omitempty" yaml:"branches,omitempty"`
	Loops    []Loop   `json:"loops,omitempty" yaml:"loops,omitempty"`
}

type RequestSchema struct {
	Properties           map[string]Property `json:"properties,omitempty" yaml:"properties,omitempty"`
	Items                *RequestSchema      `json:"items,omitempty" yaml:"items,omitempty"`
	Type                 string              `json:"type,omitempty" yaml:"type,omitempty"`
	MaxLength            int                 `json:"maxLength,omitempty" yaml:"maxLength,omitempty"`
	Required             []string            `json:"required,omitempty" yaml:"required,omitempty"`
	PrimaryKeys          []string            `json:"primaryKeys,omitempty" yaml:"primaryKeys,omitempty"`
	Description          string              `json:"description,omitempty" yaml:"description,omitempty"`
	AdditionalProperties bool                `json:"additionalProperties,omitempty" yaml:"additionalProperties,omitempty"`
}

type RestrictedFields struct {
	Fields []string `json:"fields" yaml:"fields"`
	Roles  []string `json:"roles" yaml:"roles"`
}

type Route struct {
	Name             string `json:"name" yaml:"name"`
	Description      string `json:"description" yaml:"description"`
	Uri              string `json:"route_uri" yaml:"route_uri"`
	HandlerKey       string `json:"handler_key" yaml:"handler_key"`
	Method           string `json:"route_method" yaml:"route_method"`
	Schema           []byte `json:"schema" yaml:"schema"`
	schema           *v2.Schema
	Rules            map[string]string `json:"rules" yaml:"rules"`
	CustomRules      []string          `json:"custom_rules" yaml:"custom_rules"`
	Model            string            `json:"model" yaml:"model"`
	Handler          Handler           `json:"handler" yaml:"handler"`
	Middlewares      []Middleware      `json:"middlewares" yaml:"middlewares"`
	Operation        string            `json:"operation" yaml:"operation"`
	RestrictedFields RestrictedFields  `json:"restricted_fields" yaml:"restricted_fields"`
}

func (r *Route) GetSchema() *v2.Schema {
	if r.schema != nil {
		return r.schema
	}
	return nil
}

func (r *Route) SetSchema(schema *v2.Schema) {
	r.schema = schema
}

type Schedule struct {
	Enable   bool   `json:"enable" yaml:"enable"`
	Interval string `json:"interval" yaml:"interval"`
}

type BackgroundHandler struct {
	Queue      string          `json:"queue" yaml:"queue"`
	HandlerKey string          `json:"handler_key" yaml:"handler_key"`
	Payload    json.RawMessage `json:"payload" yaml:"payload"`
	Handler    Handler         `json:"handler" yaml:"handler"`
	Schedule   *Schedule       `json:"schedule" yaml:"schedule"`
}

type Api struct {
	Prefix      string       `json:"prefix" yaml:"prefix"`
	Middlewares []Middleware `json:"middlewares" yaml:"middlewares"`
	Routes      []*Route     `json:"routes" yaml:"routes"`
}

type AllowedStatus struct {
	Source string   `json:"source" yaml:"source"`
	Target []string `yaml:"target"`
}

type Transition struct {
	Source     string           `json:"source" yaml:"source"`
	Target     string           `json:"target" yaml:"target"`
	Filters    map[string]any   `json:"filters" yaml:"filters"`
	Validators []map[string]any `json:"validators" yaml:"validators"`
	Triggers   []map[string]any `json:"triggers" yaml:"triggers"`
	Actions    []map[string]any `json:"actions" yaml:"actions"`
}

type FlowPipeline struct {
	ID       string   `json:"id" yaml:"id"`
	Statuses []string `json:"statuses" yaml:"statuses"`
}

type Flow struct {
	ID              string          `json:"id" yaml:"id"`
	Service         string          `json:"service" yaml:"service"`
	Entity          string          `json:"entity" yaml:"entity"`
	Model           string          `json:"model" yaml:"model"`
	StatusField     string          `json:"status_field" yaml:"status_field"`
	AggregateBy     []string        `json:"aggregate_by" yaml:"aggregate_by"`
	Statuses        []string        `json:"statuses" yaml:"statuses"`
	AllowedStatuses []AllowedStatus `json:"allowed_statuses" yaml:"allowed_statuses"`
	Transitions     []Transition    `json:"transitions" yaml:"transitions"`
	Pipelines       []FlowPipeline  `json:"pipelines" yaml:"pipelines"`
}

func (f *Flow) GetPipeline(key string) *FlowPipeline {
	for _, pipeline := range f.Pipelines {
		if pipeline.ID == key {
			return &pipeline
		}
	}
	return nil
}

type Middleware struct {
	Name    string          `json:"name" yaml:"name"`
	Options json.RawMessage `json:"options" yaml:"options"`
}

type Static struct {
	Dir     string `json:"dir" yaml:"dir"`
	Prefix  string `json:"prefix" yaml:"prefix"`
	Options struct {
		ByteRange bool   `json:"byte_range" yaml:"byte_range"`
		Compress  bool   `json:"compress" yaml:"compress"`
		Browse    bool   `json:"browse" yaml:"browse"`
		IndexFile string `json:"index_file" yaml:"index_file"`
	} `json:"options" yaml:"options"`
}

type RenderConfig struct {
	ID        string `json:"id" yaml:"id"`
	Prefix    string `json:"prefix" yaml:"prefix"`
	Root      string `json:"root" yaml:"root"`
	Index     string `json:"index" yaml:"index"`
	UseIndex  bool   `json:"use_index" yaml:"use_index"`
	Compress  bool   `json:"compress" yaml:"compress"`
	Extension string `json:"extension" yaml:"extension"`
}

type Web struct {
	Prefix      string          `json:"prefix" yaml:"prefix"`
	Static      *Static         `json:"static" yaml:"static"`
	Render      []*RenderConfig `json:"render" yaml:"render"`
	Middlewares []Middleware    `json:"middlewares" yaml:"middlewares"`
	Apis        []Api           `json:"apis" yaml:"apis"`
}

type Policy struct {
	Web                Web                        `json:"web" yaml:"web"`
	Models             []Model                    `json:"models" yaml:"models"`
	BackgroundHandlers []*BackgroundHandler       `json:"background_handlers" yaml:"background_handlers"`
	Commands           []*GenericCommand          `json:"commands"`
	Conditions         []*filters.Filter          `json:"conditions" yaml:"conditions"`
	ApplicationRules   []*filters.ApplicationRule `json:"application_rules" yaml:"application_rules"`
	Handlers           []Handler                  `json:"handlers" yaml:"handlers"`
	Flows              []Flow                     `json:"flows" yaml:"flows"`
}

type UserConfig struct {
	Core   Core   `json:"core" yaml:"core"`
	Policy Policy `json:"policies" yaml:"policies"`
}

func (c *UserConfig) GetModel(source string) *Model {
	for _, model := range c.Policy.Models {
		if model.Name == source {
			return &model
		}
	}
	return nil
}

func (c *UserConfig) GetRenderConfig(source string) *RenderConfig {
	for _, model := range c.Policy.Web.Render {
		if model.ID == source {
			return model
		}
	}
	return nil
}

func (c *UserConfig) GetEntities() (entities []string) {
	for _, model := range c.Policy.Models {
		entities = append(entities, model.Name)
	}
	return
}

func (c *UserConfig) GetDatabase(db string) *metadata.Config {
	for _, database := range c.Core.Credentials.Databases {
		if database.Key == db {
			return &database
		}
	}
	return nil
}

func (c *UserConfig) GetHandler(handlerName string) *Handler {
	for _, handler := range c.Policy.Handlers {
		if handler.Key == handlerName {
			return &handler
		}
	}
	return nil
}

func (c *UserConfig) GetRoute(name string) *Route {
	for _, routes := range c.Policy.Web.Apis {
		for _, route := range routes.Routes {
			if route.Name == name {
				return route
			}
		}
	}
	return nil
}

func (c *UserConfig) GetHandlerList() (handlers []string) {
	for _, handler := range c.Policy.Handlers {
		handlers = append(handlers, handler.Key)
	}
	return
}

func (c *UserConfig) GetSourceDatabase(source string, db ...string) *metadata.Config {
	if len(db) > 0 && db[0] != "" {
		for _, database := range c.Core.Credentials.Databases {
			if database.Key == db[0] {
				return &database
			}
		}
	}
	model := c.GetModel(source)
	if model == nil {
		return nil
	}
	for _, database := range c.Core.Credentials.Databases {
		if database.Key == model.Database {
			return &database
		}
	}
	return nil
}

func (c *UserConfig) GetCondition(key string) *filters.Filter {
	for _, condition := range c.Policy.Conditions {
		if condition.Key == key {
			return condition
		}
	}
	return nil
}

func (c *UserConfig) GetApplicationRule(key string) *filters.ApplicationRule {
	for _, applicationRule := range c.Policy.ApplicationRules {
		if applicationRule.Key == key {
			return applicationRule
		}
	}
	return nil
}

func (c *UserConfig) GetFlow(key string) *Flow {
	for _, flow := range c.Policy.Flows {
		if flow.ID == key {
			return &flow
		}
	}
	return nil
}
