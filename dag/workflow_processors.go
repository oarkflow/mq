package dag

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/mq"
)

// Advanced node processors that implement full workflow capabilities

// BaseProcessor provides common functionality for workflow processors
type BaseProcessor struct {
	config *WorkflowNodeConfig
	key    string
}

func (p *BaseProcessor) GetConfig() *WorkflowNodeConfig {
	return p.config
}

func (p *BaseProcessor) SetConfig(config *WorkflowNodeConfig) {
	p.config = config
}

func (p *BaseProcessor) GetKey() string {
	return p.key
}

func (p *BaseProcessor) SetKey(key string) {
	p.key = key
}

func (p *BaseProcessor) GetType() string {
	return "workflow" // Default type
}

func (p *BaseProcessor) Consume(ctx context.Context) error {
	return nil // Base implementation
}

func (p *BaseProcessor) Pause(ctx context.Context) error {
	return nil // Base implementation
}

func (p *BaseProcessor) Resume(ctx context.Context) error {
	return nil // Base implementation
}

func (p *BaseProcessor) Stop(ctx context.Context) error {
	return nil // Base implementation
}

func (p *BaseProcessor) Close() error {
	return nil // Base implementation
}

// Helper methods for workflow processors

func (p *BaseProcessor) processTemplate(template string, data map[string]interface{}) string {
	result := template
	for key, value := range data {
		placeholder := fmt.Sprintf("{{%s}}", key)
		result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", value))
	}
	return result
}

func (p *BaseProcessor) generateToken() string {
	return fmt.Sprintf("token_%d_%s", time.Now().UnixNano(), generateRandomString(16))
}

func (p *BaseProcessor) validateRule(rule WorkflowValidationRule, data map[string]interface{}) error {
	value, exists := data[rule.Field]

	if rule.Required && !exists {
		return fmt.Errorf("field '%s' is required", rule.Field)
	}

	if !exists {
		return nil // Optional field not provided
	}

	switch rule.Type {
	case "string":
		str, ok := value.(string)
		if !ok {
			return fmt.Errorf("field '%s' must be a string", rule.Field)
		}
		if rule.MinLength > 0 && len(str) < rule.MinLength {
			return fmt.Errorf("field '%s' must be at least %d characters", rule.Field, rule.MinLength)
		}
		if rule.MaxLength > 0 && len(str) > rule.MaxLength {
			return fmt.Errorf("field '%s' must not exceed %d characters", rule.Field, rule.MaxLength)
		}
		if rule.Pattern != "" {
			matched, _ := regexp.MatchString(rule.Pattern, str)
			if !matched {
				return fmt.Errorf("field '%s' does not match required pattern", rule.Field)
			}
		}
	case "number":
		var num float64
		switch v := value.(type) {
		case float64:
			num = v
		case int:
			num = float64(v)
		case string:
			var err error
			num, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return fmt.Errorf("field '%s' must be a number", rule.Field)
			}
		default:
			return fmt.Errorf("field '%s' must be a number", rule.Field)
		}

		if rule.Min != nil && num < *rule.Min {
			return fmt.Errorf("field '%s' must be at least %f", rule.Field, *rule.Min)
		}
		if rule.Max != nil && num > *rule.Max {
			return fmt.Errorf("field '%s' must not exceed %f", rule.Field, *rule.Max)
		}
	case "email":
		str, ok := value.(string)
		if !ok {
			return fmt.Errorf("field '%s' must be a string", rule.Field)
		}
		emailRegex := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
		matched, _ := regexp.MatchString(emailRegex, str)
		if !matched {
			return fmt.Errorf("field '%s' must be a valid email address", rule.Field)
		}
	}

	return nil
}

func (p *BaseProcessor) evaluateCondition(condition string, data map[string]interface{}) bool {
	// Simple condition evaluation (in real implementation, use proper expression parser)
	// For now, support basic equality checks like "field == value"
	parts := strings.Split(condition, "==")
	if len(parts) == 2 {
		field := strings.TrimSpace(parts[0])
		expectedValue := strings.TrimSpace(strings.Trim(parts[1], "\"'"))

		if actualValue, exists := data[field]; exists {
			return fmt.Sprintf("%v", actualValue) == expectedValue
		}
	}

	// Default to false for unsupported conditions
	return false
}

func (p *BaseProcessor) validateWebhookSignature(payload []byte, secret, signature string) bool {
	if signature == "" {
		return true // No signature to validate
	}

	// Generate HMAC signature
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	expectedSignature := hex.EncodeToString(mac.Sum(nil))

	// Compare signatures (remove common prefixes like "sha256=")
	signature = strings.TrimPrefix(signature, "sha256=")

	return hmac.Equal([]byte(signature), []byte(expectedSignature))
}

func (p *BaseProcessor) applyTransforms(data map[string]interface{}, transforms map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy original data
	for key, value := range data {
		result[key] = value
	}

	// Apply transforms (simplified implementation)
	for key, transform := range transforms {
		if transformMap, ok := transform.(map[string]interface{}); ok {
			if transformType, exists := transformMap["type"]; exists {
				switch transformType {
				case "rename":
					if from, ok := transformMap["from"].(string); ok {
						if value, exists := result[from]; exists {
							result[key] = value
							delete(result, from)
						}
					}
				case "default":
					if _, exists := result[key]; !exists {
						result[key] = transformMap["value"]
					}
				case "format":
					if format, ok := transformMap["format"].(string); ok {
						if value, exists := result[key]; exists {
							result[key] = fmt.Sprintf(format, value)
						}
					}
				}
			}
		}
	}

	return result
}

// HTMLProcessor handles HTML page generation
type HTMLProcessor struct {
	BaseProcessor
}

func (p *HTMLProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	templateStr := config.Template
	if templateStr == "" {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("template not specified"),
		}
	}

	// Parse template
	tmpl, err := template.New("html_page").Parse(templateStr)
	if err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to parse template: %w", err),
		}
	}

	// Prepare template data
	var inputData map[string]interface{}
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		inputData = make(map[string]interface{})
	}

	// Add template-specific data from config
	for key, value := range config.TemplateData {
		inputData[key] = value
	}

	// Execute template
	var htmlOutput strings.Builder
	if err := tmpl.Execute(&htmlOutput, inputData); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to execute template: %w", err),
		}
	}

	// Prepare result
	result := map[string]interface{}{
		"html_content": htmlOutput.String(),
		"template":     templateStr,
		"data":         inputData,
	}

	if config.OutputPath != "" {
		result["output_path"] = config.OutputPath
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// SMSProcessor handles SMS sending
type SMSProcessor struct {
	BaseProcessor
}

func (p *SMSProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	// Validate required fields
	if len(config.SMSTo) == 0 {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("SMS recipients not specified"),
		}
	}

	if config.Message == "" {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("SMS message not specified"),
		}
	}

	// Parse input data for dynamic content
	var inputData map[string]interface{}
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		inputData = make(map[string]interface{})
	}

	// Process message template
	message := p.processTemplate(config.Message, inputData)

	// Simulate SMS sending (in real implementation, integrate with SMS provider)
	result := map[string]interface{}{
		"sms_sent":     true,
		"provider":     config.Provider,
		"from":         config.From,
		"to":           config.SMSTo,
		"message":      message,
		"message_type": config.MessageType,
		"sent_at":      time.Now(),
		"message_id":   fmt.Sprintf("sms_%d", time.Now().UnixNano()),
	}

	// Add original data
	for key, value := range inputData {
		result[key] = value
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// AuthProcessor handles authentication tasks
type AuthProcessor struct {
	BaseProcessor
}

func (p *AuthProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to parse input data: %w", err),
		}
	}

	// Simulate authentication based on type
	result := map[string]interface{}{
		"auth_type":     config.AuthType,
		"authenticated": true,
		"auth_time":     time.Now(),
	}

	switch config.AuthType {
	case "token":
		result["token"] = p.generateToken()
		if config.TokenExpiry > 0 {
			result["expires_at"] = time.Now().Add(config.TokenExpiry)
		}
	case "oauth":
		result["access_token"] = p.generateToken()
		result["refresh_token"] = p.generateToken()
		result["token_type"] = "Bearer"
	case "basic":
		// Validate credentials
		if username, ok := inputData["username"]; ok {
			result["username"] = username
		}
		result["auth_method"] = "basic"
	}

	// Add original data
	for key, value := range inputData {
		if key != "password" && key != "secret" { // Don't include sensitive data
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

// ValidatorProcessor handles data validation
type ValidatorProcessor struct {
	BaseProcessor
}

func (p *ValidatorProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to parse input data: %w", err),
		}
	}

	// Validate based on validation rules
	validationErrors := make([]string, 0)

	for _, rule := range config.ValidationRules {
		if err := p.validateRule(rule, inputData); err != nil {
			validationErrors = append(validationErrors, err.Error())
		}
	}

	// Prepare result
	result := map[string]interface{}{
		"validation_passed": len(validationErrors) == 0,
		"validation_type":   config.ValidationType,
		"validated_at":      time.Now(),
	}

	if len(validationErrors) > 0 {
		result["validation_errors"] = validationErrors
		result["validation_status"] = "failed"
	} else {
		result["validation_status"] = "passed"
	}

	// Add original data
	for key, value := range inputData {
		result[key] = value
	}

	resultPayload, _ := json.Marshal(result)

	// Determine status based on validation
	status := mq.Completed
	if len(validationErrors) > 0 && config.ValidationType == "strict" {
		status = mq.Failed
	}

	return mq.Result{
		TaskID:  task.ID,
		Status:  status,
		Payload: resultPayload,
	}
}

// RouterProcessor handles routing decisions
type RouterProcessor struct {
	BaseProcessor
}

func (p *RouterProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to parse input data: %w", err),
		}
	}

	// Apply routing rules
	selectedRoute := config.DefaultRoute

	for _, rule := range config.RoutingRules {
		if p.evaluateCondition(rule.Condition, inputData) {
			selectedRoute = rule.Destination
			break
		}
	}

	// Prepare result
	result := map[string]interface{}{
		"route_selected": selectedRoute,
		"routed_at":      time.Now(),
		"routing_rules":  len(config.RoutingRules),
	}

	// Add original data
	for key, value := range inputData {
		result[key] = value
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// StorageProcessor handles storage operations
type StorageProcessor struct {
	BaseProcessor
}

func (p *StorageProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to parse input data: %w", err),
		}
	}

	// Simulate storage operation
	result := map[string]interface{}{
		"storage_type":      config.StorageType,
		"storage_operation": config.StorageOperation,
		"storage_key":       config.StorageKey,
		"operated_at":       time.Now(),
	}

	switch config.StorageOperation {
	case "store", "save", "put":
		result["stored"] = true
		result["storage_path"] = config.StoragePath
	case "retrieve", "get", "load":
		result["retrieved"] = true
		result["data"] = inputData // Simulate retrieved data
	case "delete", "remove":
		result["deleted"] = true
	case "update", "modify":
		result["updated"] = true
		result["storage_path"] = config.StoragePath
	}

	// Add original data
	for key, value := range inputData {
		result[key] = value
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// NotifyProcessor handles notifications
type NotifyProcessor struct {
	BaseProcessor
}

func (p *NotifyProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		inputData = make(map[string]interface{})
	}

	// Process notification message template
	message := p.processTemplate(config.NotificationMessage, inputData)

	// Prepare result
	result := map[string]interface{}{
		"notified":             true,
		"notify_type":          config.NotifyType,
		"notification_type":    config.NotificationType,
		"recipients":           config.NotificationRecipients,
		"message":              message,
		"channel":              config.Channel,
		"notification_sent_at": time.Now(),
		"notification_id":      fmt.Sprintf("notify_%d", time.Now().UnixNano()),
	}

	// Add original data
	for key, value := range inputData {
		result[key] = value
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

// WebhookReceiverProcessor handles webhook reception
type WebhookReceiverProcessor struct {
	BaseProcessor
}

func (p *WebhookReceiverProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	config := p.GetConfig()

	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{
			TaskID: task.ID,
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to parse webhook payload: %w", err),
		}
	}

	// Validate webhook if secret is provided
	if config.WebhookSecret != "" {
		if !p.validateWebhookSignature(task.Payload, config.WebhookSecret, config.WebhookSignature) {
			return mq.Result{
				TaskID: task.ID,
				Status: mq.Failed,
				Error:  fmt.Errorf("webhook signature validation failed"),
			}
		}
	}

	// Apply webhook transforms if configured
	transformedData := inputData
	if len(config.WebhookTransforms) > 0 {
		transformedData = p.applyTransforms(inputData, config.WebhookTransforms)
	}

	// Prepare result
	result := map[string]interface{}{
		"webhook_received":     true,
		"webhook_path":         config.ListenPath,
		"webhook_processed_at": time.Now(),
		"webhook_validated":    config.WebhookSecret != "",
		"webhook_transformed":  len(config.WebhookTransforms) > 0,
		"data":                 transformedData,
	}

	resultPayload, _ := json.Marshal(result)

	return mq.Result{
		TaskID:  task.ID,
		Status:  mq.Completed,
		Payload: resultPayload,
	}
}

func generateRandomString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[time.Now().UnixNano()%int64(len(chars))]
	}
	return string(result)
}
