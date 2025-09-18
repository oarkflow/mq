package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/oarkflow/mq"
)

// WorkflowProcessor interface for workflow-aware processors
type WorkflowProcessor interface {
	mq.Processor
	SetConfig(config *WorkflowNodeConfig)
	GetConfig() *WorkflowNodeConfig
}

// ProcessorFactory creates and manages workflow processors
type ProcessorFactory struct {
	processors map[string]func() WorkflowProcessor
	mu         sync.RWMutex
}

// NewProcessorFactory creates a new processor factory with all workflow processors
func NewProcessorFactory() *ProcessorFactory {
	factory := &ProcessorFactory{
		processors: make(map[string]func() WorkflowProcessor),
	}

	// Register all workflow processors
	factory.registerBuiltinProcessors()

	return factory
}

// registerBuiltinProcessors registers all built-in workflow processors
func (f *ProcessorFactory) registerBuiltinProcessors() {
	// Basic workflow processors
	f.RegisterProcessor("task", func() WorkflowProcessor { return &TaskWorkflowProcessor{} })
	f.RegisterProcessor("api", func() WorkflowProcessor { return &APIWorkflowProcessor{} })
	f.RegisterProcessor("transform", func() WorkflowProcessor { return &TransformWorkflowProcessor{} })
	f.RegisterProcessor("decision", func() WorkflowProcessor { return &DecisionWorkflowProcessor{} })
	f.RegisterProcessor("timer", func() WorkflowProcessor { return &TimerWorkflowProcessor{} })
	f.RegisterProcessor("database", func() WorkflowProcessor { return &DatabaseWorkflowProcessor{} })
	f.RegisterProcessor("email", func() WorkflowProcessor { return &EmailWorkflowProcessor{} })

	// Advanced workflow processors
	f.RegisterProcessor("html", func() WorkflowProcessor { return &HTMLProcessor{} })
	f.RegisterProcessor("sms", func() WorkflowProcessor { return &SMSProcessor{} })
	f.RegisterProcessor("auth", func() WorkflowProcessor { return &AuthProcessor{} })
	f.RegisterProcessor("validator", func() WorkflowProcessor { return &ValidatorProcessor{} })
	f.RegisterProcessor("router", func() WorkflowProcessor { return &RouterProcessor{} })
	f.RegisterProcessor("storage", func() WorkflowProcessor { return &StorageProcessor{} })
	f.RegisterProcessor("notify", func() WorkflowProcessor { return &NotifyProcessor{} })
	f.RegisterProcessor("webhook_receiver", func() WorkflowProcessor { return &WebhookReceiverProcessor{} })
	f.RegisterProcessor("webhook", func() WorkflowProcessor { return &WebhookProcessor{} })
	f.RegisterProcessor("sub_dag", func() WorkflowProcessor { return &SubDAGWorkflowProcessor{} })
	f.RegisterProcessor("parallel", func() WorkflowProcessor { return &ParallelWorkflowProcessor{} })
	f.RegisterProcessor("loop", func() WorkflowProcessor { return &LoopWorkflowProcessor{} })
}

// RegisterProcessor registers a custom processor
func (f *ProcessorFactory) RegisterProcessor(nodeType string, creator func() WorkflowProcessor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.processors[nodeType] = creator
}

// CreateProcessor creates a processor instance for the given node type
func (f *ProcessorFactory) CreateProcessor(nodeType string, config *WorkflowNodeConfig) (WorkflowProcessor, error) {
	f.mu.RLock()
	creator, exists := f.processors[nodeType]
	f.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown processor type: %s", nodeType)
	}

	processor := creator()
	processor.SetConfig(config)
	return processor, nil
}

// GetRegisteredTypes returns all registered processor types
func (f *ProcessorFactory) GetRegisteredTypes() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	types := make([]string, 0, len(f.processors))
	for nodeType := range f.processors {
		types = append(types, nodeType)
	}
	return types
}

// Basic workflow processors that wrap existing DAG processors

// TaskWorkflowProcessor wraps task processing with workflow config
type TaskWorkflowProcessor struct {
	BaseProcessor
}

func (p *TaskWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	// Execute script or command if provided
	if config.Script != "" {
		// In real implementation, execute script
		return mq.Result{
			TaskID:  task.ID,
			Status:  mq.Completed,
			Payload: task.Payload,
		}
	}

	if config.Command != "" {
		// In real implementation, execute command
		return mq.Result{
			TaskID:  task.ID,
			Status:  mq.Completed,
			Payload: task.Payload,
		}
	}

	// Default passthrough
	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

// APIWorkflowProcessor handles API calls with workflow config
type APIWorkflowProcessor struct {
	BaseProcessor
}

func (p *APIWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	if config.URL == "" {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("API URL not specified"),
		}
	}

	// In real implementation, make HTTP request
	// For now, simulate API call
	result := map[string]any{
		"api_called": true,
		"url":        config.URL,
		"method":     config.Method,
		"headers":    config.Headers,
		"called_at":  "simulated",
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// TransformWorkflowProcessor handles data transformations
type TransformWorkflowProcessor struct {
	BaseProcessor
}

func (p *TransformWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to unmarshal payload: %w", err),
		}
	}

	// Apply transformation
	payload["transformed"] = true
	payload["transform_type"] = config.TransformType
	payload["expression"] = config.Expression

	transformedPayload, _ := json.Marshal(payload)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: transformedPayload,
	}
}

// DecisionWorkflowProcessor handles decision logic
type DecisionWorkflowProcessor struct {
	BaseProcessor
}

func (p *DecisionWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to parse input data: %w", err),
		}
	}

	// Apply decision rules
	selectedPath := "default"
	for _, rule := range config.DecisionRules {
		if p.evaluateCondition(rule.Condition, inputData) {
			selectedPath = rule.NextNode
			break
		}
	}

	// Add decision result to data
	inputData["decision_path"] = selectedPath
	inputData["condition_evaluated"] = config.Condition

	resultPayload, _ := json.Marshal(inputData)

	return mq.Result{
		TaskID:          task.ID,
		Status:          mq.Completed,
		Payload:         resultPayload,
		ConditionStatus: selectedPath,
	}
}

// TimerWorkflowProcessor handles timer/delay operations
type TimerWorkflowProcessor struct {
	BaseProcessor
}

func (p *TimerWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	if config.Duration > 0 {
		// In real implementation, this might use a scheduler
		// For demo, we just add the delay info to the result
		result := map[string]any{
			"timer_delay":  config.Duration.String(),
			"schedule":     config.Schedule,
			"timer_set_at": "simulated",
		}

		var inputData map[string]any
		if err := json.Unmarshal(task.Payload, &inputData); err == nil {
			for key, value := range inputData {
				result[key] = value
			}
		}

		resultPayload, _ := json.Marshal(result)

		return mq.Result{
			TaskID:  task.ID,
			Status:  mq.Completed,
			Payload: resultPayload,
		}
	}

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: task.Payload,
	}
}

// DatabaseWorkflowProcessor handles database operations
type DatabaseWorkflowProcessor struct {
	BaseProcessor
}

func (p *DatabaseWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	if config.Query == "" {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("database query not specified"),
		}
	}

	// Simulate database operation
	result := map[string]any{
		"db_query_executed": true,
		"query":             config.Query,
		"connection":        config.Connection,
		"executed_at":       "simulated",
	}

	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err == nil {
		for key, value := range inputData {
			result[key] = value
		}
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// EmailWorkflowProcessor handles email sending
type EmailWorkflowProcessor struct {
	BaseProcessor
}

func (p *EmailWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	if len(config.EmailTo) == 0 {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("email recipients not specified"),
		}
	}

	// Simulate email sending
	result := map[string]any{
		"email_sent": true,
		"to":         config.EmailTo,
		"subject":    config.Subject,
		"body":       config.Body,
		"sent_at":    "simulated",
	}

	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err == nil {
		for key, value := range inputData {
			result[key] = value
		}
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// WebhookProcessor handles webhook sending
type WebhookProcessor struct {
	BaseProcessor
}

func (p *WebhookProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	if config.URL == "" {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("webhook URL not specified"),
		}
	}

	// Simulate webhook sending
	result := map[string]any{
		"webhook_sent": true,
		"url":          config.URL,
		"method":       config.Method,
		"sent_at":      "simulated",
	}

	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err == nil {
		for key, value := range inputData {
			result[key] = value
		}
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// SubDAGWorkflowProcessor handles sub-DAG execution
type SubDAGWorkflowProcessor struct {
	BaseProcessor
}

func (p *SubDAGWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	if config.SubWorkflowID == "" {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("sub-workflow ID not specified"),
		}
	}

	// Simulate sub-DAG execution
	result := map[string]any{
		"sub_dag_executed": true,
		"sub_workflow_id":  config.SubWorkflowID,
		"input_mapping":    config.InputMapping,
		"output_mapping":   config.OutputMapping,
		"executed_at":      "simulated",
	}

	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err == nil {
		for key, value := range inputData {
			result[key] = value
		}
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// ParallelWorkflowProcessor handles parallel execution
type ParallelWorkflowProcessor struct {
	BaseProcessor
}

func (p *ParallelWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Simulate parallel processing
	result := map[string]any{
		"parallel_executed": true,
		"executed_at":       "simulated",
	}

	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err == nil {
		for key, value := range inputData {
			result[key] = value
		}
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// LoopWorkflowProcessor handles loop execution
type LoopWorkflowProcessor struct {
	BaseProcessor
}

func (p *LoopWorkflowProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Simulate loop processing
	result := map[string]any{
		"loop_executed": true,
		"executed_at":   "simulated",
	}

	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err == nil {
		for key, value := range inputData {
			result[key] = value
		}
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}
