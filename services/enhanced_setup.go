package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// EnhancedServiceManager implementation
type enhancedServiceManager struct {
	config         *EnhancedServiceConfig
	workflowEngine *dag.WorkflowEngineManager
	dagService     EnhancedDAGService
	validation     EnhancedValidation
	handlers       map[string]EnhancedHandler
	running        bool
}

// NewEnhancedServiceManager creates a new enhanced service manager
func NewEnhancedServiceManager(config *EnhancedServiceConfig) EnhancedServiceManager {
	return &enhancedServiceManager{
		config:   config,
		handlers: make(map[string]EnhancedHandler),
	}
}

// Initialize sets up the enhanced service manager
func (sm *enhancedServiceManager) Initialize(config *EnhancedServiceConfig) error {
	sm.config = config

	// Initialize workflow engine
	if config.WorkflowEngineConfig != nil {
		engine := dag.NewWorkflowEngineManager(config.WorkflowEngineConfig)
		sm.workflowEngine = engine
	}

	// Initialize enhanced DAG service
	sm.dagService = NewEnhancedDAGService(config)

	// Initialize enhanced validation
	if config.ValidationConfig != nil {
		validation, err := NewEnhancedValidation(config.ValidationConfig)
		if err != nil {
			return fmt.Errorf("failed to initialize enhanced validation: %w", err)
		}
		sm.validation = validation
	}

	return nil
}

// Start starts all services
func (sm *enhancedServiceManager) Start(ctx context.Context) error {
	if sm.running {
		return errors.New("service manager already running")
	}

	// Start workflow engine
	if sm.workflowEngine != nil {
		if err := sm.workflowEngine.Start(ctx); err != nil {
			return fmt.Errorf("failed to start workflow engine: %w", err)
		}
	}

	sm.running = true
	return nil
}

// Stop stops all services
func (sm *enhancedServiceManager) Stop(ctx context.Context) error {
	if !sm.running {
		return nil
	}

	// Stop workflow engine
	if sm.workflowEngine != nil {
		sm.workflowEngine.Stop(ctx)
	}

	sm.running = false
	return nil
}

// Health returns the health status of all services
func (sm *enhancedServiceManager) Health() map[string]interface{} {
	health := make(map[string]interface{})

	health["running"] = sm.running
	health["workflow_engine"] = sm.workflowEngine != nil
	health["dag_service"] = sm.dagService != nil
	health["validation"] = sm.validation != nil
	health["handlers_count"] = len(sm.handlers)

	return health
}

// RegisterEnhancedHandler registers an enhanced handler
func (sm *enhancedServiceManager) RegisterEnhancedHandler(handler EnhancedHandler) error {
	if handler.Key == "" {
		return errors.New("handler key is required")
	}

	// Create enhanced DAG if workflow is enabled
	if handler.WorkflowEnabled {
		enhancedDAG, err := sm.createEnhancedDAGFromHandler(handler)
		if err != nil {
			return fmt.Errorf("failed to create enhanced DAG for handler %s: %w", handler.Key, err)
		}

		// Register with workflow engine if available
		if sm.workflowEngine != nil {
			workflow, err := sm.convertHandlerToWorkflow(handler)
			if err != nil {
				return fmt.Errorf("failed to convert handler to workflow: %w", err)
			}

			if err := sm.workflowEngine.RegisterWorkflow(context.Background(), workflow); err != nil {
				return fmt.Errorf("failed to register workflow: %w", err)
			}
		}

		// Store enhanced DAG
		if sm.dagService != nil {
			if err := sm.dagService.StoreEnhancedDAG(handler.Key, enhancedDAG); err != nil {
				return fmt.Errorf("failed to store enhanced DAG: %w", err)
			}
		}
	} else {
		// Create traditional DAG
		traditionalDAG, err := sm.createTraditionalDAGFromHandler(handler)
		if err != nil {
			return fmt.Errorf("failed to create traditional DAG for handler %s: %w", handler.Key, err)
		}

		// Store traditional DAG
		if sm.dagService != nil {
			if err := sm.dagService.StoreDAG(handler.Key, traditionalDAG); err != nil {
				return fmt.Errorf("failed to store DAG: %w", err)
			}
		}
	}

	sm.handlers[handler.Key] = handler
	return nil
}

// GetEnhancedHandler retrieves an enhanced handler
func (sm *enhancedServiceManager) GetEnhancedHandler(key string) (EnhancedHandler, error) {
	handler, exists := sm.handlers[key]
	if !exists {
		return EnhancedHandler{}, fmt.Errorf("handler with key %s not found", key)
	}
	return handler, nil
}

// ListEnhancedHandlers returns all registered handlers
func (sm *enhancedServiceManager) ListEnhancedHandlers() []EnhancedHandler {
	handlers := make([]EnhancedHandler, 0, len(sm.handlers))
	for _, handler := range sm.handlers {
		handlers = append(handlers, handler)
	}
	return handlers
}

// GetWorkflowEngine returns the workflow engine
func (sm *enhancedServiceManager) GetWorkflowEngine() *dag.WorkflowEngineManager {
	return sm.workflowEngine
}

// ExecuteEnhancedWorkflow executes a workflow with enhanced features
func (sm *enhancedServiceManager) ExecuteEnhancedWorkflow(ctx context.Context, key string, input map[string]interface{}) (*dag.ExecutionResult, error) {
	handler, err := sm.GetEnhancedHandler(key)
	if err != nil {
		return nil, err
	}

	if handler.WorkflowEnabled && sm.workflowEngine != nil {
		// Execute using workflow engine
		return sm.workflowEngine.ExecuteWorkflow(ctx, handler.Key, input)
	} else {
		// Execute using traditional DAG
		traditionalDAG := sm.dagService.GetDAG(key)
		if traditionalDAG == nil {
			return nil, fmt.Errorf("DAG not found for key: %s", key)
		}

		// Convert input to byte format for traditional DAG
		inputBytes, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("failed to convert input: %w", err)
		}

		result := traditionalDAG.Process(ctx, inputBytes)

		// Convert output
		var output map[string]interface{}
		if err := json.Unmarshal(result.Payload, &output); err != nil {
			output = map[string]interface{}{"raw": string(result.Payload)}
		}

		// Convert result to ExecutionResult format
		now := time.Now()
		executionResult := &dag.ExecutionResult{
			ID:        fmt.Sprintf("%s-%d", key, now.Unix()),
			Status:    dag.ExecutionStatusCompleted,
			Output:    output,
			StartTime: now,
			EndTime:   &now,
		}

		if result.Error != nil {
			executionResult.Error = result.Error.Error()
			executionResult.Status = dag.ExecutionStatusFailed
		}

		return executionResult, nil
	}
}

// RegisterHTTPRoutes registers HTTP routes for enhanced handlers
func (sm *enhancedServiceManager) RegisterHTTPRoutes(app *fiber.App) error {
	// Create API group
	api := app.Group("/api/v1")

	// Health endpoint
	api.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(sm.Health())
	})

	// List handlers endpoint
	api.Get("/handlers", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"handlers": sm.ListEnhancedHandlers(),
		})
	})

	// Execute workflow endpoint
	api.Post("/execute/:key", func(c *fiber.Ctx) error {
		key := c.Params("key")

		var input map[string]interface{}
		if err := c.BodyParser(&input); err != nil {
			return c.Status(400).JSON(fiber.Map{
				"error": "Invalid input format",
			})
		}

		result, err := sm.ExecuteEnhancedWorkflow(c.Context(), key, input)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(result)
	})

	// Workflow engine specific endpoints
	if sm.workflowEngine != nil {
		sm.registerWorkflowEngineRoutes(api)
	}

	return nil
}

// CreateAPIEndpoints creates API endpoints for handlers
func (sm *enhancedServiceManager) CreateAPIEndpoints(handlers []EnhancedHandler) error {
	for _, handler := range handlers {
		if err := sm.RegisterEnhancedHandler(handler); err != nil {
			return fmt.Errorf("failed to register handler %s: %w", handler.Key, err)
		}
	}
	return nil
}

// Helper methods

func (sm *enhancedServiceManager) createEnhancedDAGFromHandler(handler EnhancedHandler) (*dag.EnhancedDAG, error) {
	// Create enhanced DAG configuration
	config := handler.EnhancedConfig
	if config == nil {
		config = &dag.EnhancedDAGConfig{
			EnableWorkflowEngine:  true,
			EnableStateManagement: true,
			EnableAdvancedRetry:   true,
			EnableMetrics:         true,
		}
	}

	// Create enhanced DAG
	enhancedDAG, err := dag.NewEnhancedDAG(handler.Name, handler.Key, config)
	if err != nil {
		return nil, err
	}

	// Add enhanced nodes
	for _, node := range handler.Nodes {
		if err := sm.addEnhancedNodeToDAG(enhancedDAG, node); err != nil {
			return nil, fmt.Errorf("failed to add node %s: %w", node.ID, err)
		}
	}

	return enhancedDAG, nil
}

func (sm *enhancedServiceManager) createTraditionalDAGFromHandler(handler EnhancedHandler) (*dag.DAG, error) {
	// Create traditional DAG (backward compatibility)
	opts := []mq.Option{
		mq.WithSyncMode(true),
	}

	if sm.config.BrokerURL != "" {
		opts = append(opts, mq.WithBrokerURL(sm.config.BrokerURL))
	}

	traditionalDAG := dag.NewDAG(handler.Name, handler.Key, nil, opts...)
	traditionalDAG.SetDebug(handler.Debug)

	// Add traditional nodes (convert enhanced nodes to traditional)
	for _, node := range handler.Nodes {
		if err := sm.addTraditionalNodeToDAG(traditionalDAG, node); err != nil {
			return nil, fmt.Errorf("failed to add traditional node %s: %w", node.ID, err)
		}
	}

	// Add edges
	for _, edge := range handler.Edges {
		if edge.Label == "" {
			edge.Label = fmt.Sprintf("edge-%s", edge.Source)
		}
		traditionalDAG.AddEdge(dag.Simple, edge.Label, edge.Source, edge.Target...)
	}

	// Add loops
	for _, loop := range handler.Loops {
		if loop.Label == "" {
			loop.Label = fmt.Sprintf("loop-%s", loop.Source)
		}
		traditionalDAG.AddEdge(dag.Iterator, loop.Label, loop.Source, loop.Target...)
	}

	return traditionalDAG, traditionalDAG.Validate()
}

func (sm *enhancedServiceManager) addEnhancedNodeToDAG(enhancedDAG *dag.EnhancedDAG, node EnhancedNode) error {
	// This would need to be implemented based on the actual EnhancedDAG API
	// For now, we'll return nil as a placeholder
	return nil
}

func (sm *enhancedServiceManager) addTraditionalNodeToDAG(traditionalDAG *dag.DAG, node EnhancedNode) error {
	// Convert enhanced node to traditional node
	// This is a simplified conversion - in practice, you'd need more sophisticated mapping
	if node.Node != "" {
		// Traditional node with processor
		processor, err := sm.createProcessorFromNode(node)
		if err != nil {
			return err
		}
		traditionalDAG.AddNode(dag.Function, node.Name, node.ID, processor, node.FirstNode)
	} else if node.NodeKey != "" {
		// Reference to another DAG
		referencedDAG := sm.dagService.GetDAG(node.NodeKey)
		if referencedDAG == nil {
			return fmt.Errorf("referenced DAG not found: %s", node.NodeKey)
		}
		traditionalDAG.AddDAGNode(dag.Function, node.Name, node.ID, referencedDAG, node.FirstNode)
	}

	return nil
}

func (sm *enhancedServiceManager) createProcessorFromNode(node EnhancedNode) (mq.Processor, error) {
	// This would create appropriate processors based on node type
	// For now, return a basic processor
	return &basicProcessor{id: node.ID, name: node.Name}, nil
}

func (sm *enhancedServiceManager) convertHandlerToWorkflow(handler EnhancedHandler) (*dag.WorkflowDefinition, error) {
	// Convert enhanced handler to workflow definition
	nodes := make([]dag.WorkflowNode, len(handler.Nodes))
	for i, node := range handler.Nodes {
		nodes[i] = dag.WorkflowNode{
			ID:     node.ID,
			Name:   node.Name,
			Type:   node.Type,
			Config: node.Config,
		}
	}

	workflow := &dag.WorkflowDefinition{
		ID:          handler.Key,
		Name:        handler.Name,
		Description: handler.Description,
		Version:     handler.Version,
		Nodes:       nodes,
	}

	return workflow, nil
}

func (sm *enhancedServiceManager) registerWorkflowEngineRoutes(api fiber.Router) {
	// Workflow management endpoints
	workflows := api.Group("/workflows")

	// List workflows
	workflows.Get("/", func(c *fiber.Ctx) error {
		registry := sm.workflowEngine.GetRegistry()
		workflowList, err := registry.List(c.Context())
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(workflowList)
	})

	// Get workflow by ID
	workflows.Get("/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		registry := sm.workflowEngine.GetRegistry()
		workflow, err := registry.Get(c.Context(), id, "") // Empty version means get latest
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Workflow not found"})
		}
		return c.JSON(workflow)
	})

	// Execute workflow
	workflows.Post("/:id/execute", func(c *fiber.Ctx) error {
		id := c.Params("id")

		var input map[string]interface{}
		if err := c.BodyParser(&input); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
		}

		result, err := sm.workflowEngine.ExecuteWorkflow(c.Context(), id, input)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(result)
	})
}

// Basic processor implementation for backward compatibility
type basicProcessor struct {
	id   string
	name string
	key  string
}

func (p *basicProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{
		Ctx:     ctx,
		Payload: task.Payload,
	}
}

func (p *basicProcessor) Consume(ctx context.Context) error {
	// Basic consume implementation - just return nil for now
	return nil
}

func (p *basicProcessor) Pause(ctx context.Context) error {
	return nil
}

func (p *basicProcessor) Resume(ctx context.Context) error {
	return nil
}

func (p *basicProcessor) Stop(ctx context.Context) error {
	return nil
}

func (p *basicProcessor) Close() error {
	return nil
}

func (p *basicProcessor) GetKey() string {
	return p.key
}

func (p *basicProcessor) SetKey(key string) {
	p.key = key
}

func (p *basicProcessor) GetType() string {
	return "basic"
}
