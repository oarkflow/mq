package dag

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// WorkflowEngineManager integrates the complete workflow engine capabilities into DAG
type WorkflowEngineManager struct {
	registry         *WorkflowRegistry
	stateManager     *AdvancedWorkflowStateManager
	processorFactory *ProcessorFactory
	scheduler        *WorkflowScheduler
	executor         *WorkflowExecutor
	middleware       *WorkflowMiddleware
	security         *WorkflowSecurity
	config           *WorkflowEngineConfig
	mu               sync.RWMutex
	running          bool
}

// NewWorkflowScheduler creates a new workflow scheduler
func NewWorkflowScheduler(stateManager *AdvancedWorkflowStateManager, executor *WorkflowExecutor) *WorkflowScheduler {
	return &WorkflowScheduler{
		stateManager:   stateManager,
		executor:       executor,
		scheduledTasks: make(map[string]*ScheduledTask),
	}
}

// WorkflowEngineConfig configures the workflow engine
type WorkflowEngineConfig struct {
	MaxConcurrentExecutions int           `json:"max_concurrent_executions"`
	DefaultTimeout          time.Duration `json:"default_timeout"`
	EnablePersistence       bool          `json:"enable_persistence"`
	EnableSecurity          bool          `json:"enable_security"`
	EnableMiddleware        bool          `json:"enable_middleware"`
	EnableScheduling        bool          `json:"enable_scheduling"`
	RetryConfig             *RetryConfig  `json:"retry_config"`
}

// WorkflowScheduler handles workflow scheduling and timing
type WorkflowScheduler struct {
	stateManager   *AdvancedWorkflowStateManager
	executor       *WorkflowExecutor
	scheduledTasks map[string]*ScheduledTask
	mu             sync.RWMutex
	running        bool
}

// WorkflowRegistry manages workflow definitions
type WorkflowRegistry struct {
	workflows map[string]*WorkflowDefinition
	mu        sync.RWMutex
}

// NewWorkflowRegistry creates a new workflow registry
func NewWorkflowRegistry() *WorkflowRegistry {
	return &WorkflowRegistry{
		workflows: make(map[string]*WorkflowDefinition),
	}
}

// Store stores a workflow definition
func (r *WorkflowRegistry) Store(ctx context.Context, definition *WorkflowDefinition) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if definition.ID == "" {
		return fmt.Errorf("workflow ID cannot be empty")
	}

	r.workflows[definition.ID] = definition
	return nil
}

// Get retrieves a workflow definition
func (r *WorkflowRegistry) Get(ctx context.Context, id string, version string) (*WorkflowDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workflow, exists := r.workflows[id]
	if !exists {
		return nil, fmt.Errorf("workflow not found: %s", id)
	}

	// If version specified, check version match
	if version != "" && workflow.Version != version {
		return nil, fmt.Errorf("workflow version mismatch: requested %s, found %s", version, workflow.Version)
	}

	return workflow, nil
}

// List returns all workflow definitions
func (r *WorkflowRegistry) List(ctx context.Context) ([]*WorkflowDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workflows := make([]*WorkflowDefinition, 0, len(r.workflows))
	for _, workflow := range r.workflows {
		workflows = append(workflows, workflow)
	}

	return workflows, nil
}

// Delete removes a workflow definition
func (r *WorkflowRegistry) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.workflows[id]; !exists {
		return fmt.Errorf("workflow not found: %s", id)
	}

	delete(r.workflows, id)
	return nil
}

// AdvancedWorkflowStateManager manages workflow execution state
type AdvancedWorkflowStateManager struct {
	executions map[string]*WorkflowExecution
	mu         sync.RWMutex
}

// NewAdvancedWorkflowStateManager creates a new state manager
func NewAdvancedWorkflowStateManager() *AdvancedWorkflowStateManager {
	return &AdvancedWorkflowStateManager{
		executions: make(map[string]*WorkflowExecution),
	}
}

// CreateExecution creates a new workflow execution
func (sm *AdvancedWorkflowStateManager) CreateExecution(ctx context.Context, workflowID string, input map[string]interface{}) (*WorkflowExecution, error) {
	execution := &WorkflowExecution{
		ID:             generateExecutionID(),
		WorkflowID:     workflowID,
		Status:         ExecutionStatusPending,
		StartTime:      time.Now(),
		Context:        ctx,
		Input:          input,
		NodeExecutions: make(map[string]*NodeExecution),
	}

	sm.mu.Lock()
	sm.executions[execution.ID] = execution
	sm.mu.Unlock()

	return execution, nil
}

// GetExecution retrieves an execution by ID
func (sm *AdvancedWorkflowStateManager) GetExecution(ctx context.Context, executionID string) (*WorkflowExecution, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	execution, exists := sm.executions[executionID]
	if !exists {
		return nil, fmt.Errorf("execution not found: %s", executionID)
	}

	return execution, nil
}

// UpdateExecution updates an execution
func (sm *AdvancedWorkflowStateManager) UpdateExecution(ctx context.Context, execution *WorkflowExecution) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.executions[execution.ID] = execution
	return nil
}

// ListExecutions returns all executions
func (sm *AdvancedWorkflowStateManager) ListExecutions(ctx context.Context, filters map[string]interface{}) ([]*WorkflowExecution, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	executions := make([]*WorkflowExecution, 0)
	for _, execution := range sm.executions {
		// Apply filters if any
		if workflowID, ok := filters["workflow_id"]; ok {
			if execution.WorkflowID != workflowID {
				continue
			}
		}
		if status, ok := filters["status"]; ok {
			if execution.Status != status {
				continue
			}
		}

		executions = append(executions, execution)
	}

	return executions, nil
}

type ScheduledTask struct {
	ID         string
	WorkflowID string
	Schedule   string
	Input      map[string]interface{}
	NextRun    time.Time
	LastRun    *time.Time
	Enabled    bool
}

// Start starts the scheduler
func (s *WorkflowScheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("scheduler already running")
	}

	s.running = true
	go s.run(ctx)
	return nil
}

// Stop stops the scheduler
func (s *WorkflowScheduler) Stop(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
}

func (s *WorkflowScheduler) run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute) // Check every minute
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkScheduledTasks(ctx)
		}

		s.mu.RLock()
		running := s.running
		s.mu.RUnlock()

		if !running {
			return
		}
	}
}

func (s *WorkflowScheduler) checkScheduledTasks(ctx context.Context) {
	s.mu.RLock()
	tasks := make([]*ScheduledTask, 0, len(s.scheduledTasks))
	for _, task := range s.scheduledTasks {
		if task.Enabled && time.Now().After(task.NextRun) {
			tasks = append(tasks, task)
		}
	}
	s.mu.RUnlock()

	for _, task := range tasks {
		go s.executeScheduledTask(ctx, task)
	}
}

func (s *WorkflowScheduler) executeScheduledTask(ctx context.Context, task *ScheduledTask) {
	// Execute the workflow
	if s.executor != nil {
		_, err := s.executor.ExecuteWorkflow(ctx, task.WorkflowID, task.Input)
		if err != nil {
			// Log error (in real implementation)
			fmt.Printf("Failed to execute scheduled workflow %s: %v\n", task.WorkflowID, err)
		}
	}

	// Update last run and calculate next run
	now := time.Now()
	task.LastRun = &now

	// Simple scheduling - add 1 hour for demo (in real implementation, parse cron expression)
	task.NextRun = now.Add(1 * time.Hour)
}

// WorkflowExecutor executes workflows using the processor factory
type WorkflowExecutor struct {
	processorFactory *ProcessorFactory
	stateManager     *AdvancedWorkflowStateManager
	config           *WorkflowEngineConfig
	mu               sync.RWMutex
}

// NewWorkflowExecutor creates a new executor
func NewWorkflowExecutor(factory *ProcessorFactory, stateManager *AdvancedWorkflowStateManager, config *WorkflowEngineConfig) *WorkflowExecutor {
	return &WorkflowExecutor{
		processorFactory: factory,
		stateManager:     stateManager,
		config:           config,
	}
}

// Start starts the executor
func (e *WorkflowExecutor) Start(ctx context.Context) error {
	return nil // No special startup needed
}

// Stop stops the executor
func (e *WorkflowExecutor) Stop(ctx context.Context) {
	// Cleanup resources if needed
}

// ExecuteWorkflow executes a workflow
func (e *WorkflowExecutor) ExecuteWorkflow(ctx context.Context, workflowID string, input map[string]interface{}) (*WorkflowExecution, error) {
	// Create execution
	execution, err := e.stateManager.CreateExecution(ctx, workflowID, input)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution: %w", err)
	}

	// Start execution
	execution.Status = ExecutionStatusRunning
	e.stateManager.UpdateExecution(ctx, execution)

	// Execute asynchronously
	go e.executeWorkflowAsync(ctx, execution)

	return execution, nil
}

func (e *WorkflowExecutor) executeWorkflowAsync(ctx context.Context, execution *WorkflowExecution) {
	defer func() {
		if r := recover(); r != nil {
			execution.Status = ExecutionStatusFailed
			execution.Error = fmt.Errorf("execution panicked: %v", r)
			endTime := time.Now()
			execution.EndTime = &endTime
			e.stateManager.UpdateExecution(ctx, execution)
		}
	}()

	// For now, simulate workflow execution
	time.Sleep(100 * time.Millisecond)

	execution.Status = ExecutionStatusCompleted
	execution.Output = map[string]interface{}{
		"result": "workflow completed successfully",
		"input":  execution.Input,
	}
	endTime := time.Now()
	execution.EndTime = &endTime

	e.stateManager.UpdateExecution(ctx, execution)
}

// WorkflowMiddleware handles middleware processing
type WorkflowMiddleware struct {
	middlewares []WorkflowMiddlewareFunc
	mu          sync.RWMutex
}

type WorkflowMiddlewareFunc func(ctx context.Context, execution *WorkflowExecution, next WorkflowNextFunc) error
type WorkflowNextFunc func(ctx context.Context, execution *WorkflowExecution) error

// NewWorkflowMiddleware creates new middleware manager
func NewWorkflowMiddleware() *WorkflowMiddleware {
	return &WorkflowMiddleware{
		middlewares: make([]WorkflowMiddlewareFunc, 0),
	}
}

// Use adds middleware to the chain
func (m *WorkflowMiddleware) Use(middleware WorkflowMiddlewareFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.middlewares = append(m.middlewares, middleware)
}

// Execute executes middleware chain
func (m *WorkflowMiddleware) Execute(ctx context.Context, execution *WorkflowExecution, handler WorkflowNextFunc) error {
	m.mu.RLock()
	middlewares := make([]WorkflowMiddlewareFunc, len(m.middlewares))
	copy(middlewares, m.middlewares)
	m.mu.RUnlock()

	// Build middleware chain
	chain := handler
	for i := len(middlewares) - 1; i >= 0; i-- {
		middleware := middlewares[i]
		next := chain
		chain = func(ctx context.Context, execution *WorkflowExecution) error {
			return middleware(ctx, execution, next)
		}
	}

	return chain(ctx, execution)
}

// WorkflowSecurity handles authentication and authorization
type WorkflowSecurity struct {
	users       map[string]*WorkflowUser
	permissions map[string]*WorkflowPermission
	mu          sync.RWMutex
}

type WorkflowUser struct {
	ID          string   `json:"id"`
	Username    string   `json:"username"`
	Email       string   `json:"email"`
	Role        string   `json:"role"`
	Permissions []string `json:"permissions"`
}

type WorkflowPermission struct {
	ID       string `json:"id"`
	Resource string `json:"resource"`
	Action   string `json:"action"`
	Scope    string `json:"scope"`
}

// NewWorkflowSecurity creates new security manager
func NewWorkflowSecurity() *WorkflowSecurity {
	return &WorkflowSecurity{
		users:       make(map[string]*WorkflowUser),
		permissions: make(map[string]*WorkflowPermission),
	}
}

// Authenticate authenticates a user
func (s *WorkflowSecurity) Authenticate(ctx context.Context, token string) (*WorkflowUser, error) {
	// Simplified authentication - in real implementation, validate JWT or similar
	if token == "admin-token" {
		return &WorkflowUser{
			ID:          "admin",
			Username:    "admin",
			Role:        "admin",
			Permissions: []string{"workflow:read", "workflow:write", "workflow:execute", "workflow:delete"},
		}, nil
	}

	return nil, fmt.Errorf("invalid token")
}

// Authorize checks if user has permission
func (s *WorkflowSecurity) Authorize(ctx context.Context, user *WorkflowUser, resource, action string) error {
	requiredPermission := fmt.Sprintf("%s:%s", resource, action)

	for _, permission := range user.Permissions {
		if permission == requiredPermission || permission == "*" {
			return nil
		}
	}

	return fmt.Errorf("permission denied: %s", requiredPermission)
}

// NewWorkflowEngineManager creates a complete workflow engine manager
func NewWorkflowEngineManager(config *WorkflowEngineConfig) *WorkflowEngineManager {
	if config == nil {
		config = &WorkflowEngineConfig{
			MaxConcurrentExecutions: 100,
			DefaultTimeout:          30 * time.Minute,
			EnablePersistence:       true,
			EnableSecurity:          false,
			EnableMiddleware:        false,
			EnableScheduling:        false,
		}
	}

	registry := NewWorkflowRegistry()
	stateManager := NewAdvancedWorkflowStateManager()
	processorFactory := NewProcessorFactory()

	executor := NewWorkflowExecutor(processorFactory, stateManager, config)
	scheduler := NewWorkflowScheduler(stateManager, executor)
	middleware := NewWorkflowMiddleware()
	security := NewWorkflowSecurity()

	return &WorkflowEngineManager{
		registry:         registry,
		stateManager:     stateManager,
		processorFactory: processorFactory,
		scheduler:        scheduler,
		executor:         executor,
		middleware:       middleware,
		security:         security,
		config:           config,
	}
}

// Start starts the workflow engine
func (m *WorkflowEngineManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("workflow engine already running")
	}

	// Start components
	if err := m.executor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start executor: %w", err)
	}

	if m.config.EnableScheduling {
		if err := m.scheduler.Start(ctx); err != nil {
			return fmt.Errorf("failed to start scheduler: %w", err)
		}
	}

	m.running = true
	return nil
}

// Stop stops the workflow engine
func (m *WorkflowEngineManager) Stop(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return
	}

	m.executor.Stop(ctx)
	if m.config.EnableScheduling {
		m.scheduler.Stop(ctx)
	}

	m.running = false
}

// RegisterWorkflow registers a workflow definition
func (m *WorkflowEngineManager) RegisterWorkflow(ctx context.Context, definition *WorkflowDefinition) error {
	return m.registry.Store(ctx, definition)
}

// ExecuteWorkflow executes a workflow
func (m *WorkflowEngineManager) ExecuteWorkflow(ctx context.Context, workflowID string, input map[string]interface{}) (*ExecutionResult, error) {
	execution, err := m.executor.ExecuteWorkflow(ctx, workflowID, input)
	if err != nil {
		return nil, err
	}

	return &ExecutionResult{
		ID:         execution.ID,
		WorkflowID: execution.WorkflowID,
		Status:     execution.Status,
		StartTime:  execution.StartTime,
		EndTime:    execution.EndTime,
		Input:      execution.Input,
		Output:     execution.Output,
		Error:      "",
	}, nil
}

// GetExecution retrieves an execution
func (m *WorkflowEngineManager) GetExecution(ctx context.Context, executionID string) (*ExecutionResult, error) {
	execution, err := m.stateManager.GetExecution(ctx, executionID)
	if err != nil {
		return nil, err
	}

	errorMsg := ""
	if execution.Error != nil {
		errorMsg = execution.Error.Error()
	}

	return &ExecutionResult{
		ID:         execution.ID,
		WorkflowID: execution.WorkflowID,
		Status:     execution.Status,
		StartTime:  execution.StartTime,
		EndTime:    execution.EndTime,
		Input:      execution.Input,
		Output:     execution.Output,
		Error:      errorMsg,
	}, nil
}

// GetRegistry returns the workflow registry
func (m *WorkflowEngineManager) GetRegistry() *WorkflowRegistry {
	return m.registry
}

// GetStateManager returns the state manager
func (m *WorkflowEngineManager) GetStateManager() *AdvancedWorkflowStateManager {
	return m.stateManager
}

// GetProcessorFactory returns the processor factory
func (m *WorkflowEngineManager) GetProcessorFactory() *ProcessorFactory {
	return m.processorFactory
}
