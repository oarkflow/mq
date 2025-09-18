package services

import (
	"context"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/mq/dag"
)

// Enhanced validation implementation
type enhancedValidation struct {
	config *ValidationServiceConfig
	base   Validation
}

// NewEnhancedValidation creates a new enhanced validation service
func NewEnhancedValidation(config *ValidationServiceConfig) (EnhancedValidation, error) {
	// Create base validation (assuming ValidationInstance is available)
	if ValidationInstance == nil {
		return nil, fmt.Errorf("base validation instance not available")
	}

	return &enhancedValidation{
		config: config,
		base:   ValidationInstance,
	}, nil
}

// Make implements the base Validation interface
func (ev *enhancedValidation) Make(ctx *fiber.Ctx, data any, rules map[string]string, options ...Option) (Validator, error) {
	return ev.base.Make(ctx, data, rules, options...)
}

// AddRules implements the base Validation interface
func (ev *enhancedValidation) AddRules(rules []Rule) error {
	return ev.base.AddRules(rules)
}

// Rules implements the base Validation interface
func (ev *enhancedValidation) Rules() []Rule {
	return ev.base.Rules()
}

// ValidateWorkflowInput validates input using workflow validation rules
func (ev *enhancedValidation) ValidateWorkflowInput(ctx context.Context, input map[string]interface{}, rules []*dag.WorkflowValidationRule) (ValidationResult, error) {
	result := ValidationResult{
		Valid:  true,
		Errors: make(map[string]string),
		Data:   input,
	}

	for _, rule := range rules {
		if err := ev.validateField(input, rule, &result); err != nil {
			return result, err
		}
	}

	return result, nil
}

// CreateValidationProcessor creates a validator processor from rules
func (ev *enhancedValidation) CreateValidationProcessor(rules []*dag.WorkflowValidationRule) (*dag.ValidatorProcessor, error) {
	config := &dag.WorkflowNodeConfig{
		ValidationType:  "custom",
		ValidationRules: make([]dag.WorkflowValidationRule, len(rules)),
	}

	// Convert pointer slice to value slice
	for i, rule := range rules {
		config.ValidationRules[i] = *rule
	}

	// Create processor factory and get validator processor
	factory := dag.NewProcessorFactory()
	processor, err := factory.CreateProcessor("validator", config)
	if err != nil {
		return nil, fmt.Errorf("failed to create validator processor: %w", err)
	}

	// Type assert to ValidatorProcessor
	validatorProcessor, ok := processor.(*dag.ValidatorProcessor)
	if !ok {
		return nil, fmt.Errorf("processor is not a ValidatorProcessor")
	}

	return validatorProcessor, nil
}

// Helper method to validate individual fields
func (ev *enhancedValidation) validateField(input map[string]interface{}, rule *dag.WorkflowValidationRule, result *ValidationResult) error {
	value, exists := input[rule.Field]

	// Check required fields
	if rule.Required && (!exists || value == nil || value == "") {
		result.Valid = false
		result.Errors[rule.Field] = rule.Message
		if result.Errors[rule.Field] == "" {
			result.Errors[rule.Field] = fmt.Sprintf("Field %s is required", rule.Field)
		}
		return nil
	}

	// Skip validation if field doesn't exist and is not required
	if !exists {
		return nil
	}

	// Validate based on type
	switch rule.Type {
	case "string":
		if err := ev.validateString(value, rule, result); err != nil {
			return err
		}
	case "number":
		if err := ev.validateNumber(value, rule, result); err != nil {
			return err
		}
	case "email":
		if err := ev.validateEmail(value, rule, result); err != nil {
			return err
		}
	case "bool":
		if err := ev.validateBool(value, rule, result); err != nil {
			return err
		}
	default:
		// Custom validation type
		if err := ev.validateCustom(value, rule, result); err != nil {
			return err
		}
	}

	return nil
}

func (ev *enhancedValidation) validateString(value interface{}, rule *dag.WorkflowValidationRule, result *ValidationResult) error {
	str, ok := value.(string)
	if !ok {
		result.Valid = false
		result.Errors[rule.Field] = fmt.Sprintf("Field %s must be a string", rule.Field)
		return nil
	}

	// Check length constraints
	if rule.MinLength > 0 && len(str) < rule.MinLength {
		result.Valid = false
		result.Errors[rule.Field] = fmt.Sprintf("Field %s must be at least %d characters", rule.Field, rule.MinLength)
		return nil
	}

	if rule.MaxLength > 0 && len(str) > rule.MaxLength {
		result.Valid = false
		result.Errors[rule.Field] = fmt.Sprintf("Field %s must be at most %d characters", rule.Field, rule.MaxLength)
		return nil
	}

	// Check pattern
	if rule.Pattern != "" {
		// Simple pattern matching - in practice, you'd use regex
		// This is a placeholder implementation
		if rule.Pattern == "^[a-zA-Z\\s]+$" && !isAlphaSpace(str) {
			result.Valid = false
			result.Errors[rule.Field] = rule.Message
			if result.Errors[rule.Field] == "" {
				result.Errors[rule.Field] = fmt.Sprintf("Field %s contains invalid characters", rule.Field)
			}
			return nil
		}
	}

	return nil
}

func (ev *enhancedValidation) validateNumber(value interface{}, rule *dag.WorkflowValidationRule, result *ValidationResult) error {
	var num float64
	var ok bool

	switch v := value.(type) {
	case float64:
		num = v
		ok = true
	case int:
		num = float64(v)
		ok = true
	case int64:
		num = float64(v)
		ok = true
	default:
		ok = false
	}

	if !ok {
		result.Valid = false
		result.Errors[rule.Field] = fmt.Sprintf("Field %s must be a number", rule.Field)
		return nil
	}

	// Check range constraints
	if rule.Min != nil && num < *rule.Min {
		result.Valid = false
		result.Errors[rule.Field] = fmt.Sprintf("Field %s must be at least %f", rule.Field, *rule.Min)
		return nil
	}

	if rule.Max != nil && num > *rule.Max {
		result.Valid = false
		result.Errors[rule.Field] = fmt.Sprintf("Field %s must be at most %f", rule.Field, *rule.Max)
		return nil
	}

	return nil
}

func (ev *enhancedValidation) validateEmail(value interface{}, rule *dag.WorkflowValidationRule, result *ValidationResult) error {
	email, ok := value.(string)
	if !ok {
		result.Valid = false
		result.Errors[rule.Field] = fmt.Sprintf("Field %s must be a string", rule.Field)
		return nil
	}

	// Simple email validation - in practice, you'd use a proper email validator
	if !isValidEmail(email) {
		result.Valid = false
		result.Errors[rule.Field] = rule.Message
		if result.Errors[rule.Field] == "" {
			result.Errors[rule.Field] = fmt.Sprintf("Field %s must be a valid email", rule.Field)
		}
		return nil
	}

	return nil
}

func (ev *enhancedValidation) validateBool(value interface{}, rule *dag.WorkflowValidationRule, result *ValidationResult) error {
	_, ok := value.(bool)
	if !ok {
		result.Valid = false
		result.Errors[rule.Field] = fmt.Sprintf("Field %s must be a boolean", rule.Field)
		return nil
	}

	return nil
}

func (ev *enhancedValidation) validateCustom(value interface{}, rule *dag.WorkflowValidationRule, result *ValidationResult) error {
	// Custom validation logic - implement based on your needs
	// For now, just accept any value for custom types
	return nil
}

// Helper functions for validation

func isAlphaSpace(s string) bool {
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == ' ') {
			return false
		}
	}
	return true
}

func isValidEmail(email string) bool {
	// Very basic email validation - in practice, use a proper email validator
	return len(email) > 3 &&
		len(email) < 255 &&
		contains(email, "@") &&
		contains(email, ".") &&
		email[0] != '@' &&
		email[len(email)-1] != '@'
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
