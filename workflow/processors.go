package workflow

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

// ProcessorFactory creates processor instances for different node types
type ProcessorFactory struct {
	processors map[string]func() Processor
}

// NewProcessorFactory creates a new processor factory with all registered processors
func NewProcessorFactory() *ProcessorFactory {
	factory := &ProcessorFactory{
		processors: make(map[string]func() Processor),
	}

	// Register basic processors
	factory.RegisterProcessor("task", func() Processor { return &TaskProcessor{} })
	factory.RegisterProcessor("api", func() Processor { return &APIProcessor{} })
	factory.RegisterProcessor("transform", func() Processor { return &TransformProcessor{} })
	factory.RegisterProcessor("decision", func() Processor { return &DecisionProcessor{} })
	factory.RegisterProcessor("timer", func() Processor { return &TimerProcessor{} })
	factory.RegisterProcessor("parallel", func() Processor { return &ParallelProcessor{} })
	factory.RegisterProcessor("sequence", func() Processor { return &SequenceProcessor{} })
	factory.RegisterProcessor("loop", func() Processor { return &LoopProcessor{} })
	factory.RegisterProcessor("filter", func() Processor { return &FilterProcessor{} })
	factory.RegisterProcessor("aggregator", func() Processor { return &AggregatorProcessor{} })
	factory.RegisterProcessor("error", func() Processor { return &ErrorProcessor{} })

	// Register advanced processors
	factory.RegisterProcessor("subdag", func() Processor { return &SubDAGProcessor{} })
	factory.RegisterProcessor("html", func() Processor { return &HTMLProcessor{} })
	factory.RegisterProcessor("sms", func() Processor { return &SMSProcessor{} })
	factory.RegisterProcessor("auth", func() Processor { return &AuthProcessor{} })
	factory.RegisterProcessor("validator", func() Processor { return &ValidatorProcessor{} })
	factory.RegisterProcessor("router", func() Processor { return &RouterProcessor{} })
	factory.RegisterProcessor("storage", func() Processor { return &StorageProcessor{} })
	factory.RegisterProcessor("notify", func() Processor { return &NotifyProcessor{} })
	factory.RegisterProcessor("webhook_receiver", func() Processor { return &WebhookReceiverProcessor{} })

	return factory
}

// RegisterProcessor registers a new processor type
func (f *ProcessorFactory) RegisterProcessor(nodeType string, creator func() Processor) {
	f.processors[nodeType] = creator
}

// CreateProcessor creates a processor instance for the given node type
func (f *ProcessorFactory) CreateProcessor(nodeType string) (Processor, error) {
	creator, exists := f.processors[nodeType]
	if !exists {
		return nil, fmt.Errorf("unknown processor type: %s", nodeType)
	}
	return creator(), nil
}

// Basic Processors

// TaskProcessor handles task execution
type TaskProcessor struct{}

func (p *TaskProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	log.Printf("Executing task: %s", input.Node.Name)

	// Execute the task based on configuration
	config := input.Node.Config

	// Simulate task execution based on script or command
	if config.Script != "" {
		log.Printf("Executing script: %s", config.Script)
	} else if config.Command != "" {
		log.Printf("Executing command: %s", config.Command)
	}

	time.Sleep(100 * time.Millisecond)

	result := &ProcessingResult{
		Success: true,
		Data:    map[string]interface{}{"task_completed": true, "task_name": input.Node.Name},
		Message: fmt.Sprintf("Task %s completed successfully", input.Node.Name),
	}

	return result, nil
}

// APIProcessor handles API calls
type APIProcessor struct{}

func (p *APIProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	url := config.URL
	if url == "" {
		return &ProcessingResult{
			Success: false,
			Error:   "URL not specified in API configuration",
		}, nil
	}

	method := "GET"
	if config.Method != "" {
		method = strings.ToUpper(config.Method)
	}

	log.Printf("Making %s request to %s", method, url)

	// Simulate API call
	time.Sleep(200 * time.Millisecond)

	// Mock response
	response := map[string]interface{}{
		"status": "success",
		"url":    url,
		"method": method,
		"data":   "mock response data",
	}

	return &ProcessingResult{
		Success: true,
		Data:    response,
		Message: fmt.Sprintf("API call to %s completed", url),
	}, nil
}

// TransformProcessor handles data transformation
type TransformProcessor struct{}

func (p *TransformProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	// Get transformation rules from Custom config
	transforms, ok := config.Custom["transforms"].(map[string]interface{})
	if !ok {
		return &ProcessingResult{
			Success: false,
			Error:   "No transformation rules specified",
		}, nil
	}

	// Apply transformations to input data
	result := make(map[string]interface{})
	for key, rule := range transforms {
		// Simple field mapping for now
		if sourceField, ok := rule.(string); ok {
			if value, exists := input.Data[sourceField]; exists {
				result[key] = value
			}
		}
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: "Data transformation completed",
	}, nil
}

// DecisionProcessor handles conditional logic
type DecisionProcessor struct{}

func (p *DecisionProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	condition := config.Condition
	if condition == "" {
		return &ProcessingResult{
			Success: false,
			Error:   "No condition specified",
		}, nil
	}

	// Simple condition evaluation
	decision := p.evaluateCondition(condition, input.Data)

	result := &ProcessingResult{
		Success: true,
		Data: map[string]interface{}{
			"decision":  decision,
			"condition": condition,
		},
		Message: fmt.Sprintf("Decision made: %t", decision),
	}

	return result, nil
}

func (p *DecisionProcessor) evaluateCondition(condition string, data map[string]interface{}) bool {
	// Simple condition evaluation - in real implementation, use expression parser
	if strings.Contains(condition, "==") {
		parts := strings.Split(condition, "==")
		if len(parts) == 2 {
			field := strings.TrimSpace(parts[0])
			expectedValue := strings.TrimSpace(strings.Trim(parts[1], "\"'"))

			if value, exists := data[field]; exists {
				return fmt.Sprintf("%v", value) == expectedValue
			}
		}
	}

	// Default to true for simplicity
	return true
}

// TimerProcessor handles time-based operations
type TimerProcessor struct{}

func (p *TimerProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	duration := 1 * time.Second
	if config.Duration > 0 {
		duration = config.Duration
	} else if config.Schedule != "" {
		// Simple schedule parsing - just use 1 second for demo
		duration = 1 * time.Second
	}

	log.Printf("Timer waiting for %v", duration)

	select {
	case <-ctx.Done():
		return &ProcessingResult{
			Success: false,
			Error:   "Timer cancelled",
		}, ctx.Err()
	case <-time.After(duration):
		return &ProcessingResult{
			Success: true,
			Data:    map[string]interface{}{"waited": duration.String()},
			Message: fmt.Sprintf("Timer completed after %v", duration),
		}, nil
	}
}

// ParallelProcessor handles parallel execution
type ParallelProcessor struct{}

func (p *ParallelProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	// This would typically trigger parallel execution of child nodes
	// For now, just return success
	return &ProcessingResult{
		Success: true,
		Data:    map[string]interface{}{"parallel_execution": "started"},
		Message: "Parallel execution initiated",
	}, nil
}

// SequenceProcessor handles sequential execution
type SequenceProcessor struct{}

func (p *SequenceProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	// This would typically ensure sequential execution of child nodes
	// For now, just return success
	return &ProcessingResult{
		Success: true,
		Data:    map[string]interface{}{"sequence_execution": "started"},
		Message: "Sequential execution initiated",
	}, nil
}

// LoopProcessor handles loop operations
type LoopProcessor struct{}

func (p *LoopProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	iterations := 1
	if iterValue, ok := config.Custom["iterations"].(float64); ok {
		iterations = int(iterValue)
	}

	results := make([]interface{}, 0, iterations)

	for i := 0; i < iterations; i++ {
		// In real implementation, this would execute child nodes
		results = append(results, map[string]interface{}{
			"iteration": i + 1,
			"data":      input.Data,
		})
	}

	return &ProcessingResult{
		Success: true,
		Data:    map[string]interface{}{"loop_results": results},
		Message: fmt.Sprintf("Loop completed %d iterations", iterations),
	}, nil
}

// FilterProcessor handles data filtering
type FilterProcessor struct{}

func (p *FilterProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	filterField, ok := config.Custom["field"].(string)
	if !ok {
		return &ProcessingResult{
			Success: false,
			Error:   "No filter field specified",
		}, nil
	}

	filterValue := config.Custom["value"]

	// Simple filtering logic
	if value, exists := input.Data[filterField]; exists {
		if fmt.Sprintf("%v", value) == fmt.Sprintf("%v", filterValue) {
			return &ProcessingResult{
				Success: true,
				Data:    input.Data,
				Message: "Filter passed",
			}, nil
		}
	}

	return &ProcessingResult{
		Success: false,
		Data:    nil,
		Message: "Filter failed",
	}, nil
}

// AggregatorProcessor handles data aggregation
type AggregatorProcessor struct{}

func (p *AggregatorProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	operation := "sum"
	if op, ok := config.Custom["operation"].(string); ok {
		operation = op
	}

	field, ok := config.Custom["field"].(string)
	if !ok {
		return &ProcessingResult{
			Success: false,
			Error:   "No aggregation field specified",
		}, nil
	}

	// Simple aggregation - in real implementation, collect data from multiple sources
	value := input.Data[field]

	result := map[string]interface{}{
		"operation": operation,
		"field":     field,
		"result":    value,
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("Aggregation completed: %s on %s", operation, field),
	}, nil
}

// ErrorProcessor handles error scenarios
type ErrorProcessor struct{}

func (p *ErrorProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	errorMessage := "Simulated error"
	if msg, ok := config.Custom["message"].(string); ok {
		errorMessage = msg
	}

	shouldFail := true
	if fail, ok := config.Custom["fail"].(bool); ok {
		shouldFail = fail
	}

	if shouldFail {
		return &ProcessingResult{
			Success: false,
			Error:   errorMessage,
		}, nil
	}

	return &ProcessingResult{
		Success: true,
		Data:    map[string]interface{}{"error_handled": true},
		Message: "Error processor completed without error",
	}, nil
}
