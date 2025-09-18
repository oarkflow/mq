package services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// EnhancedDAGService implementation
type enhancedDAGService struct {
	config          *EnhancedServiceConfig
	enhancedDAGs    map[string]*dag.EnhancedDAG
	traditionalDAGs map[string]*dag.DAG
}

// NewEnhancedDAGService creates a new enhanced DAG service
func NewEnhancedDAGService(config *EnhancedServiceConfig) EnhancedDAGService {
	return &enhancedDAGService{
		config:          config,
		enhancedDAGs:    make(map[string]*dag.EnhancedDAG),
		traditionalDAGs: make(map[string]*dag.DAG),
	}
}

// CreateDAG creates a traditional DAG
func (eds *enhancedDAGService) CreateDAG(name, key string, options ...Option) (*dag.DAG, error) {
	opts := []mq.Option{
		mq.WithSyncMode(true),
	}

	if eds.config.BrokerURL != "" {
		opts = append(opts, mq.WithBrokerURL(eds.config.BrokerURL))
	}

	dagInstance := dag.NewDAG(name, key, nil, opts...)
	eds.traditionalDAGs[key] = dagInstance

	return dagInstance, nil
}

// GetDAG retrieves a traditional DAG
func (eds *enhancedDAGService) GetDAG(key string) *dag.DAG {
	return eds.traditionalDAGs[key]
}

// ListDAGs lists all traditional DAGs
func (eds *enhancedDAGService) ListDAGs() map[string]*dag.DAG {
	return eds.traditionalDAGs
}

// CreateEnhancedDAG creates an enhanced DAG
func (eds *enhancedDAGService) CreateEnhancedDAG(name, key string, config *dag.EnhancedDAGConfig, options ...Option) (*dag.EnhancedDAG, error) {
	enhancedDAG, err := dag.NewEnhancedDAG(name, key, config)
	if err != nil {
		return nil, err
	}

	eds.enhancedDAGs[key] = enhancedDAG
	return enhancedDAG, nil
}

// GetEnhancedDAG retrieves an enhanced DAG
func (eds *enhancedDAGService) GetEnhancedDAG(key string) *dag.EnhancedDAG {
	return eds.enhancedDAGs[key]
}

// ListEnhancedDAGs lists all enhanced DAGs
func (eds *enhancedDAGService) ListEnhancedDAGs() map[string]*dag.EnhancedDAG {
	return eds.enhancedDAGs
}

// GetWorkflowEngine retrieves workflow engine for a DAG
func (eds *enhancedDAGService) GetWorkflowEngine(dagKey string) *dag.WorkflowEngineManager {
	enhancedDAG := eds.GetEnhancedDAG(dagKey)
	if enhancedDAG == nil {
		return nil
	}

	// This would need to be implemented based on the actual EnhancedDAG API
	// For now, return nil as a placeholder
	return nil
}

// CreateWorkflowFromHandler creates a workflow definition from handler
func (eds *enhancedDAGService) CreateWorkflowFromHandler(handler EnhancedHandler) (*dag.WorkflowDefinition, error) {
	nodes := make([]dag.WorkflowNode, len(handler.Nodes))
	for i, node := range handler.Nodes {
		nodes[i] = dag.WorkflowNode{
			ID:          node.ID,
			Name:        node.Name,
			Type:        node.Type,
			Description: fmt.Sprintf("Node: %s", node.Name),
			Config:      node.Config,
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

// ExecuteWorkflow executes a workflow
func (eds *enhancedDAGService) ExecuteWorkflow(ctx context.Context, workflowID string, input map[string]any) (*dag.ExecutionResult, error) {
	enhancedDAG := eds.GetEnhancedDAG(workflowID)
	if enhancedDAG != nil {
		// Execute enhanced DAG workflow
		return eds.executeEnhancedDAGWorkflow(ctx, enhancedDAG, input)
	}

	traditionalDAG := eds.GetDAG(workflowID)
	if traditionalDAG != nil {
		// Execute traditional DAG
		return eds.executeTraditionalDAGWorkflow(ctx, traditionalDAG, input)
	}

	return nil, fmt.Errorf("workflow not found: %s", workflowID)
}

// StoreEnhancedDAG stores an enhanced DAG
func (eds *enhancedDAGService) StoreEnhancedDAG(key string, enhancedDAG *dag.EnhancedDAG) error {
	eds.enhancedDAGs[key] = enhancedDAG
	return nil
}

// StoreDAG stores a traditional DAG
func (eds *enhancedDAGService) StoreDAG(key string, traditionalDAG *dag.DAG) error {
	eds.traditionalDAGs[key] = traditionalDAG
	return nil
}

// Helper methods

func (eds *enhancedDAGService) executeEnhancedDAGWorkflow(ctx context.Context, enhancedDAG *dag.EnhancedDAG, input map[string]any) (*dag.ExecutionResult, error) {
	// This would need to be implemented based on the actual EnhancedDAG API
	// For now, create a mock result
	result := &dag.ExecutionResult{
		ID:     fmt.Sprintf("exec_%s", enhancedDAG.GetKey()),
		Status: dag.ExecutionStatusCompleted,
		Output: input,
	}

	return result, nil
}

func (eds *enhancedDAGService) executeTraditionalDAGWorkflow(ctx context.Context, traditionalDAG *dag.DAG, input map[string]any) (*dag.ExecutionResult, error) {
	// Convert input to bytes
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input: %w", err)
	}

	// Execute traditional DAG
	result := traditionalDAG.Process(ctx, inputBytes)

	// Convert result to ExecutionResult format
	var output map[string]any
	if err := json.Unmarshal(result.Payload, &output); err != nil {
		// If unmarshal fails, use the raw payload
		output = map[string]any{
			"raw_payload": string(result.Payload),
		}
	}

	executionResult := &dag.ExecutionResult{
		ID:     fmt.Sprintf("exec_%s", traditionalDAG.GetKey()),
		Status: dag.ExecutionStatusCompleted,
		Output: output,
	}

	if result.Error != nil {
		executionResult.Status = dag.ExecutionStatusFailed
		executionResult.Error = result.Error.Error()
	}

	return executionResult, nil
}
