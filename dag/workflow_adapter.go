package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// WorkflowEngineAdapter implements the WorkflowEngine interface
// This adapter bridges between the DAG system and the external workflow engine
type WorkflowEngineAdapter struct {
	// External workflow engine import (when available)
	// workflowEngine *workflow.WorkflowEngine

	// Configuration
	config             *WorkflowEngineAdapterConfig
	stateManager       *WorkflowStateManager
	persistenceManager *PersistenceManager

	// In-memory state for when external engine is not available
	definitions map[string]*WorkflowDefinition
	executions  map[string]*ExecutionResult

	// Thread safety
	mu sync.RWMutex

	// Status
	running bool
}

// WorkflowEngineAdapterConfig contains configuration for the adapter
type WorkflowEngineAdapterConfig struct {
	UseExternalEngine   bool
	EnablePersistence   bool
	PersistenceType     string // "memory", "file", "database"
	PersistencePath     string
	EnableStateRecovery bool
	MaxExecutions       int
}

// PersistenceManager handles workflow and execution persistence
type PersistenceManager struct {
	config  *WorkflowEngineAdapterConfig
	storage PersistenceStorage
	mu      sync.RWMutex
}

// PersistenceStorage interface for different storage backends
type PersistenceStorage interface {
	SaveWorkflow(definition *WorkflowDefinition) error
	LoadWorkflow(id string) (*WorkflowDefinition, error)
	ListWorkflows() ([]*WorkflowDefinition, error)
	DeleteWorkflow(id string) error

	SaveExecution(execution *ExecutionResult) error
	LoadExecution(id string) (*ExecutionResult, error)
	ListExecutions(workflowID string) ([]*ExecutionResult, error)
	DeleteExecution(id string) error
}

// MemoryPersistenceStorage implements in-memory persistence
type MemoryPersistenceStorage struct {
	workflows  map[string]*WorkflowDefinition
	executions map[string]*ExecutionResult
	mu         sync.RWMutex
}

// NewWorkflowEngineAdapter creates a new workflow engine adapter
func NewWorkflowEngineAdapter(config *WorkflowEngineAdapterConfig) *WorkflowEngineAdapter {
	if config == nil {
		config = &WorkflowEngineAdapterConfig{
			UseExternalEngine:   false,
			EnablePersistence:   true,
			PersistenceType:     "memory",
			EnableStateRecovery: true,
			MaxExecutions:       1000,
		}
	}

	adapter := &WorkflowEngineAdapter{
		config:      config,
		definitions: make(map[string]*WorkflowDefinition),
		executions:  make(map[string]*ExecutionResult),
		stateManager: &WorkflowStateManager{
			stateStore: make(map[string]interface{}),
		},
	}

	// Initialize persistence manager if enabled
	if config.EnablePersistence {
		adapter.persistenceManager = NewPersistenceManager(config)
	}

	return adapter
}

// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager(config *WorkflowEngineAdapterConfig) *PersistenceManager {
	pm := &PersistenceManager{
		config: config,
	}

	// Initialize storage backend based on configuration
	switch config.PersistenceType {
	case "memory":
		pm.storage = NewMemoryPersistenceStorage()
	case "file":
		// TODO: Implement file-based storage
		pm.storage = NewMemoryPersistenceStorage()
	case "database":
		// TODO: Implement database storage
		pm.storage = NewMemoryPersistenceStorage()
	default:
		pm.storage = NewMemoryPersistenceStorage()
	}

	return pm
}

// NewMemoryPersistenceStorage creates a new memory-based persistence storage
func NewMemoryPersistenceStorage() *MemoryPersistenceStorage {
	return &MemoryPersistenceStorage{
		workflows:  make(map[string]*WorkflowDefinition),
		executions: make(map[string]*ExecutionResult),
	}
}

// WorkflowEngine interface implementation
func (a *WorkflowEngineAdapter) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		return fmt.Errorf("workflow engine adapter is already running")
	}

	// Load persisted workflows if enabled
	if a.config.EnablePersistence && a.config.EnableStateRecovery {
		if err := a.recoverState(); err != nil {
			return fmt.Errorf("failed to recover state: %w", err)
		}
	}

	a.running = true
	return nil
}

func (a *WorkflowEngineAdapter) Stop(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.running {
		return
	}

	// Save state before stopping
	if a.config.EnablePersistence {
		a.saveState()
	}

	a.running = false
}

func (a *WorkflowEngineAdapter) RegisterWorkflow(ctx context.Context, definition *WorkflowDefinition) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if definition.ID == "" {
		return fmt.Errorf("workflow ID is required")
	}

	// Store in memory
	a.definitions[definition.ID] = definition

	// Persist if enabled
	if a.config.EnablePersistence && a.persistenceManager != nil {
		if err := a.persistenceManager.SaveWorkflow(definition); err != nil {
			return fmt.Errorf("failed to persist workflow: %w", err)
		}
	}

	return nil
}

func (a *WorkflowEngineAdapter) ExecuteWorkflow(ctx context.Context, workflowID string, input map[string]interface{}) (*ExecutionResult, error) {
	a.mu.RLock()
	definition, exists := a.definitions[workflowID]
	a.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("workflow %s not found", workflowID)
	}

	// Create execution result
	execution := &ExecutionResult{
		ID:         generateExecutionID(),
		WorkflowID: workflowID,
		Status:     ExecutionStatusRunning,
		StartTime:  time.Now(),
		Input:      input,
		Output:     make(map[string]interface{}),
	}

	// Store execution
	a.mu.Lock()
	a.executions[execution.ID] = execution
	a.mu.Unlock()

	// Execute asynchronously
	go a.executeWorkflowAsync(ctx, execution, definition)

	return execution, nil
}

func (a *WorkflowEngineAdapter) GetExecution(ctx context.Context, executionID string) (*ExecutionResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	execution, exists := a.executions[executionID]
	if !exists {
		return nil, fmt.Errorf("execution %s not found", executionID)
	}

	return execution, nil
}

// executeWorkflowAsync executes a workflow asynchronously
func (a *WorkflowEngineAdapter) executeWorkflowAsync(ctx context.Context, execution *ExecutionResult, definition *WorkflowDefinition) {
	defer func() {
		if r := recover(); r != nil {
			execution.Status = ExecutionStatusFailed
			execution.Error = fmt.Sprintf("workflow execution panicked: %v", r)
		}

		endTime := time.Now()
		execution.EndTime = &endTime

		// Persist final execution state
		if a.config.EnablePersistence && a.persistenceManager != nil {
			a.persistenceManager.SaveExecution(execution)
		}
	}()

	// Simple execution simulation
	// In a real implementation, this would execute the workflow nodes
	for i, node := range definition.Nodes {
		// Simulate node execution
		time.Sleep(time.Millisecond * 100) // Simulate processing time

		// Update execution with node results
		if execution.NodeExecutions == nil {
			execution.NodeExecutions = make(map[string]interface{})
		}

		execution.NodeExecutions[node.ID] = map[string]interface{}{
			"status":     "completed",
			"started_at": time.Now().Add(-time.Millisecond * 100),
			"ended_at":   time.Now(),
			"output":     fmt.Sprintf("Node %s executed successfully", node.Name),
		}

		// Check for cancellation
		select {
		case <-ctx.Done():
			execution.Status = ExecutionStatusCancelled
			execution.Error = "execution was cancelled"
			return
		default:
		}

		// Simulate processing
		if i == len(definition.Nodes)-1 {
			// Last node - complete execution
			execution.Status = ExecutionStatusCompleted
			execution.Output = map[string]interface{}{
				"result":         "workflow completed successfully",
				"nodes_executed": len(definition.Nodes),
			}
		}
	}
}

// recoverState recovers persisted state
func (a *WorkflowEngineAdapter) recoverState() error {
	if a.persistenceManager == nil {
		return nil
	}

	// Load workflows
	workflows, err := a.persistenceManager.ListWorkflows()
	if err != nil {
		return fmt.Errorf("failed to load workflows: %w", err)
	}

	for _, workflow := range workflows {
		a.definitions[workflow.ID] = workflow
	}

	return nil
}

// saveState saves current state
func (a *WorkflowEngineAdapter) saveState() {
	if a.persistenceManager == nil {
		return
	}

	// Save all workflows
	for _, workflow := range a.definitions {
		a.persistenceManager.SaveWorkflow(workflow)
	}

	// Save all executions
	for _, execution := range a.executions {
		a.persistenceManager.SaveExecution(execution)
	}
}

// PersistenceManager methods
func (pm *PersistenceManager) SaveWorkflow(definition *WorkflowDefinition) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.storage.SaveWorkflow(definition)
}

func (pm *PersistenceManager) LoadWorkflow(id string) (*WorkflowDefinition, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.storage.LoadWorkflow(id)
}

func (pm *PersistenceManager) ListWorkflows() ([]*WorkflowDefinition, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.storage.ListWorkflows()
}

func (pm *PersistenceManager) SaveExecution(execution *ExecutionResult) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.storage.SaveExecution(execution)
}

func (pm *PersistenceManager) LoadExecution(id string) (*ExecutionResult, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.storage.LoadExecution(id)
}

// MemoryPersistenceStorage implementation
func (m *MemoryPersistenceStorage) SaveWorkflow(definition *WorkflowDefinition) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy to avoid reference issues
	data, err := json.Marshal(definition)
	if err != nil {
		return err
	}

	var copy WorkflowDefinition
	if err := json.Unmarshal(data, &copy); err != nil {
		return err
	}

	m.workflows[definition.ID] = &copy
	return nil
}

func (m *MemoryPersistenceStorage) LoadWorkflow(id string) (*WorkflowDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	workflow, exists := m.workflows[id]
	if !exists {
		return nil, fmt.Errorf("workflow %s not found", id)
	}

	return workflow, nil
}

func (m *MemoryPersistenceStorage) ListWorkflows() ([]*WorkflowDefinition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	workflows := make([]*WorkflowDefinition, 0, len(m.workflows))
	for _, workflow := range m.workflows {
		workflows = append(workflows, workflow)
	}

	return workflows, nil
}

func (m *MemoryPersistenceStorage) DeleteWorkflow(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.workflows, id)
	return nil
}

func (m *MemoryPersistenceStorage) SaveExecution(execution *ExecutionResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep copy to avoid reference issues
	data, err := json.Marshal(execution)
	if err != nil {
		return err
	}

	var copy ExecutionResult
	if err := json.Unmarshal(data, &copy); err != nil {
		return err
	}

	m.executions[execution.ID] = &copy
	return nil
}

func (m *MemoryPersistenceStorage) LoadExecution(id string) (*ExecutionResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	execution, exists := m.executions[id]
	if !exists {
		return nil, fmt.Errorf("execution %s not found", id)
	}

	return execution, nil
}

func (m *MemoryPersistenceStorage) ListExecutions(workflowID string) ([]*ExecutionResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	executions := make([]*ExecutionResult, 0)
	for _, execution := range m.executions {
		if workflowID == "" || execution.WorkflowID == workflowID {
			executions = append(executions, execution)
		}
	}

	return executions, nil
}

func (m *MemoryPersistenceStorage) DeleteExecution(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.executions, id)
	return nil
}
