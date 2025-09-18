package workflow

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
)

// SubDAGProcessor handles sub-workflow execution
type SubDAGProcessor struct{}

func (p *SubDAGProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	subWorkflowID := config.SubWorkflowID
	if subWorkflowID == "" {
		return &ProcessingResult{
			Success: false,
			Error:   "sub_workflow_id not specified",
		}, nil
	}

	// Apply input mapping
	subInput := make(map[string]interface{})
	for subKey, sourceKey := range config.InputMapping {
		if value, exists := input.Data[sourceKey]; exists {
			subInput[subKey] = value
		}
	}

	// Simulate sub-workflow execution (in real implementation, this would trigger actual sub-workflow)
	time.Sleep(100 * time.Millisecond)

	// Mock sub-workflow output
	subOutput := map[string]interface{}{
		"sub_workflow_result": "completed",
		"sub_workflow_id":     subWorkflowID,
		"processed_data":      subInput,
	}

	// Apply output mapping
	result := make(map[string]interface{})
	for targetKey, subKey := range config.OutputMapping {
		if value, exists := subOutput[subKey]; exists {
			result[targetKey] = value
		}
	}

	// If no output mapping specified, return all sub-workflow output
	if len(config.OutputMapping) == 0 {
		result = subOutput
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("Sub-workflow %s completed successfully", subWorkflowID),
	}, nil
}

// HTMLProcessor handles HTML page generation
type HTMLProcessor struct{}

func (p *HTMLProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	templateStr := config.Template
	if templateStr == "" {
		return &ProcessingResult{
			Success: false,
			Error:   "template not specified",
		}, nil
	}

	// Parse template
	tmpl, err := template.New("html_page").Parse(templateStr)
	if err != nil {
		return &ProcessingResult{
			Success: false,
			Error:   fmt.Sprintf("failed to parse template: %v", err),
		}, nil
	}

	// Prepare template data
	templateData := make(map[string]interface{})

	// Add data from input
	for key, value := range input.Data {
		templateData[key] = value
	}

	// Add template-specific data from config
	for key, value := range config.TemplateData {
		templateData[key] = value
	}

	// Add current timestamp
	templateData["timestamp"] = time.Now().Format("2006-01-02 15:04:05")

	// Execute template
	var htmlBuffer strings.Builder
	if err := tmpl.Execute(&htmlBuffer, templateData); err != nil {
		return &ProcessingResult{
			Success: false,
			Error:   fmt.Sprintf("failed to execute template: %v", err),
		}, nil
	}

	html := htmlBuffer.String()

	result := map[string]interface{}{
		"html_content": html,
		"template":     templateStr,
		"data_used":    templateData,
	}

	// If output path is specified, simulate file writing
	if config.OutputPath != "" {
		result["output_path"] = config.OutputPath
		result["file_written"] = true
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: "HTML page generated successfully",
	}, nil
}

// SMSProcessor handles SMS operations
type SMSProcessor struct{}

func (p *SMSProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	provider := config.Provider
	if provider == "" {
		provider = "default"
	}

	from := config.From
	if from == "" {
		return &ProcessingResult{
			Success: false,
			Error:   "from number not specified",
		}, nil
	}

	if len(config.SMSTo) == 0 {
		return &ProcessingResult{
			Success: false,
			Error:   "recipient numbers not specified",
		}, nil
	}

	message := config.Message
	if message == "" {
		return &ProcessingResult{
			Success: false,
			Error:   "message not specified",
		}, nil
	}

	// Process message template with input data
	processedMessage := p.processMessageTemplate(message, input.Data)

	// Validate phone numbers
	validRecipients := []string{}
	invalidRecipients := []string{}

	for _, recipient := range config.SMSTo {
		if p.isValidPhoneNumber(recipient) {
			validRecipients = append(validRecipients, recipient)
		} else {
			invalidRecipients = append(invalidRecipients, recipient)
		}
	}

	if len(validRecipients) == 0 {
		return &ProcessingResult{
			Success: false,
			Error:   "no valid recipient numbers",
		}, nil
	}

	// Simulate SMS sending
	time.Sleep(50 * time.Millisecond)

	// Mock SMS sending results
	results := []map[string]interface{}{}
	for _, recipient := range validRecipients {
		results = append(results, map[string]interface{}{
			"recipient":  recipient,
			"status":     "sent",
			"message_id": fmt.Sprintf("msg_%d", time.Now().UnixNano()),
			"provider":   provider,
		})
	}

	result := map[string]interface{}{
		"provider":           provider,
		"from":               from,
		"message":            processedMessage,
		"valid_recipients":   validRecipients,
		"invalid_recipients": invalidRecipients,
		"sent_count":         len(validRecipients),
		"failed_count":       len(invalidRecipients),
		"results":            results,
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("SMS sent to %d recipients via %s", len(validRecipients), provider),
	}, nil
}

func (p *SMSProcessor) processMessageTemplate(message string, data map[string]interface{}) string {
	result := message
	for key, value := range data {
		placeholder := fmt.Sprintf("{{%s}}", key)
		result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", value))
	}
	return result
}

func (p *SMSProcessor) isValidPhoneNumber(phone string) bool {
	// Simple phone number validation (E.164 format)
	phoneRegex := regexp.MustCompile(`^\+[1-9]\d{1,14}$`)
	return phoneRegex.MatchString(phone)
}

// AuthProcessor handles authentication operations
type AuthProcessor struct{}

func (p *AuthProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	authType := config.AuthType
	if authType == "" {
		authType = "jwt"
	}

	credentials := config.Credentials
	if credentials == nil {
		return &ProcessingResult{
			Success: false,
			Error:   "credentials not provided",
		}, nil
	}

	switch authType {
	case "jwt":
		return p.processJWTAuth(input, credentials, config.TokenExpiry)
	case "basic":
		return p.processBasicAuth(input, credentials)
	case "api_key":
		return p.processAPIKeyAuth(input, credentials)
	default:
		return &ProcessingResult{
			Success: false,
			Error:   fmt.Sprintf("unsupported auth type: %s", authType),
		}, nil
	}
}

func (p *AuthProcessor) processJWTAuth(input ProcessingContext, credentials map[string]string, expiry time.Duration) (*ProcessingResult, error) {
	username, hasUsername := credentials["username"]
	password, hasPassword := credentials["password"]

	if !hasUsername || !hasPassword {
		return &ProcessingResult{
			Success: false,
			Error:   "username and password required for JWT auth",
		}, nil
	}

	// Simulate authentication (in real implementation, verify against user store)
	if username == "admin" && password == "password" {
		// Generate mock JWT token
		token := fmt.Sprintf("jwt.token.%d", time.Now().Unix())
		expiresAt := time.Now().Add(expiry)
		if expiry == 0 {
			expiresAt = time.Now().Add(24 * time.Hour)
		}

		result := map[string]interface{}{
			"auth_type":   "jwt",
			"token":       token,
			"expires_at":  expiresAt,
			"username":    username,
			"permissions": []string{"read", "write", "admin"},
		}

		return &ProcessingResult{
			Success: true,
			Data:    result,
			Message: "JWT authentication successful",
		}, nil
	}

	return &ProcessingResult{
		Success: false,
		Error:   "invalid credentials",
	}, nil
}

func (p *AuthProcessor) processBasicAuth(input ProcessingContext, credentials map[string]string) (*ProcessingResult, error) {
	username, hasUsername := credentials["username"]
	password, hasPassword := credentials["password"]

	if !hasUsername || !hasPassword {
		return &ProcessingResult{
			Success: false,
			Error:   "username and password required for basic auth",
		}, nil
	}

	// Simulate basic auth
	if username != "" && password != "" {
		result := map[string]interface{}{
			"auth_type": "basic",
			"username":  username,
			"status":    "authenticated",
		}

		return &ProcessingResult{
			Success: true,
			Data:    result,
			Message: "Basic authentication successful",
		}, nil
	}

	return &ProcessingResult{
		Success: false,
		Error:   "invalid credentials",
	}, nil
}

func (p *AuthProcessor) processAPIKeyAuth(input ProcessingContext, credentials map[string]string) (*ProcessingResult, error) {
	apiKey, hasAPIKey := credentials["api_key"]

	if !hasAPIKey {
		return &ProcessingResult{
			Success: false,
			Error:   "api_key required for API key auth",
		}, nil
	}

	// Simulate API key validation
	if apiKey != "" && len(apiKey) >= 10 {
		result := map[string]interface{}{
			"auth_type": "api_key",
			"api_key":   apiKey[:6] + "...", // Partially masked
			"status":    "authenticated",
		}

		return &ProcessingResult{
			Success: true,
			Data:    result,
			Message: "API key authentication successful",
		}, nil
	}

	return &ProcessingResult{
		Success: false,
		Error:   "invalid API key",
	}, nil
}

// ValidatorProcessor handles data validation
type ValidatorProcessor struct{}

func (p *ValidatorProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	validationType := config.ValidationType
	if validationType == "" {
		validationType = "rules"
	}

	validationRules := config.ValidationRules
	if len(validationRules) == 0 {
		return &ProcessingResult{
			Success: false,
			Error:   "no validation rules specified",
		}, nil
	}

	errors := []string{}
	warnings := []string{}
	validatedFields := []string{}

	for _, rule := range validationRules {
		fieldValue, exists := input.Data[rule.Field]
		if !exists {
			if rule.Required {
				errors = append(errors, fmt.Sprintf("required field '%s' is missing", rule.Field))
			}
			continue
		}

		// Validate based on rule type
		switch rule.Type {
		case "string":
			if err := p.validateString(fieldValue, rule); err != nil {
				errors = append(errors, fmt.Sprintf("field '%s': %s", rule.Field, err.Error()))
			} else {
				validatedFields = append(validatedFields, rule.Field)
			}
		case "number":
			if err := p.validateNumber(fieldValue, rule); err != nil {
				errors = append(errors, fmt.Sprintf("field '%s': %s", rule.Field, err.Error()))
			} else {
				validatedFields = append(validatedFields, rule.Field)
			}
		case "email":
			if err := p.validateEmail(fieldValue); err != nil {
				errors = append(errors, fmt.Sprintf("field '%s': %s", rule.Field, err.Error()))
			} else {
				validatedFields = append(validatedFields, rule.Field)
			}
		case "regex":
			if err := p.validateRegex(fieldValue, rule.Pattern); err != nil {
				errors = append(errors, fmt.Sprintf("field '%s': %s", rule.Field, err.Error()))
			} else {
				validatedFields = append(validatedFields, rule.Field)
			}
		default:
			warnings = append(warnings, fmt.Sprintf("unknown validation type '%s' for field '%s'", rule.Type, rule.Field))
		}
	}

	success := len(errors) == 0
	result := map[string]interface{}{
		"validation_type":  validationType,
		"validated_fields": validatedFields,
		"errors":           errors,
		"warnings":         warnings,
		"error_count":      len(errors),
		"warning_count":    len(warnings),
		"is_valid":         success,
	}

	message := fmt.Sprintf("Validation completed: %d fields validated, %d errors, %d warnings",
		len(validatedFields), len(errors), len(warnings))

	return &ProcessingResult{
		Success: success,
		Data:    result,
		Message: message,
	}, nil
}

func (p *ValidatorProcessor) validateString(value interface{}, rule ValidationRule) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string, got %T", value)
	}

	if rule.MinLength > 0 && len(str) < int(rule.MinLength) {
		return fmt.Errorf("minimum length is %d, got %d", rule.MinLength, len(str))
	}

	if rule.MaxLength > 0 && len(str) > int(rule.MaxLength) {
		return fmt.Errorf("maximum length is %d, got %d", rule.MaxLength, len(str))
	}

	return nil
}

func (p *ValidatorProcessor) validateNumber(value interface{}, rule ValidationRule) error {
	var num float64
	switch v := value.(type) {
	case int:
		num = float64(v)
	case int64:
		num = float64(v)
	case float64:
		num = v
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("cannot parse as number: %s", v)
		}
		num = parsed
	default:
		return fmt.Errorf("expected number, got %T", value)
	}

	if rule.Min != nil && num < *rule.Min {
		return fmt.Errorf("minimum value is %f, got %f", *rule.Min, num)
	}

	if rule.Max != nil && num > *rule.Max {
		return fmt.Errorf("maximum value is %f, got %f", *rule.Max, num)
	}

	return nil
}

func (p *ValidatorProcessor) validateEmail(value interface{}) error {
	email, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string, got %T", value)
	}

	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(email) {
		return fmt.Errorf("invalid email format")
	}

	return nil
}

func (p *ValidatorProcessor) validateRegex(value interface{}, pattern string) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string, got %T", value)
	}

	regex, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %s", err.Error())
	}

	if !regex.MatchString(str) {
		return fmt.Errorf("does not match pattern %s", pattern)
	}

	return nil
}

// RouterProcessor handles conditional routing
type RouterProcessor struct{}

func (p *RouterProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	routingRules := config.RoutingRules
	if len(routingRules) == 0 {
		return &ProcessingResult{
			Success: false,
			Error:   "no routing rules specified",
		}, nil
	}

	selectedRoutes := []RoutingRule{}

	for _, rule := range routingRules {
		if p.evaluateRoutingCondition(rule.Condition, input.Data) {
			selectedRoutes = append(selectedRoutes, rule)
		}
	}

	if len(selectedRoutes) == 0 {
		// Check if there's a default route
		for _, rule := range routingRules {
			if rule.IsDefault {
				selectedRoutes = append(selectedRoutes, rule)
				break
			}
		}
	}

	result := map[string]interface{}{
		"selected_routes": selectedRoutes,
		"route_count":     len(selectedRoutes),
		"routing_data":    input.Data,
	}

	if len(selectedRoutes) == 0 {
		return &ProcessingResult{
			Success: false,
			Data:    result,
			Error:   "no matching routes found",
		}, nil
	}

	message := fmt.Sprintf("Routing completed: %d routes selected", len(selectedRoutes))

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: message,
	}, nil
}

func (p *RouterProcessor) evaluateRoutingCondition(condition string, data map[string]interface{}) bool {
	// Simple condition evaluation - in real implementation, use expression parser
	if condition == "" {
		return false
	}

	// Support simple equality checks
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

	// Support simple greater than checks
	if strings.Contains(condition, ">") {
		parts := strings.Split(condition, ">")
		if len(parts) == 2 {
			field := strings.TrimSpace(parts[0])
			threshold := strings.TrimSpace(parts[1])

			if value, exists := data[field]; exists {
				if numValue, ok := value.(float64); ok {
					if thresholdValue, err := strconv.ParseFloat(threshold, 64); err == nil {
						return numValue > thresholdValue
					}
				}
			}
		}
	}

	return false
}

// StorageProcessor handles data storage operations
type StorageProcessor struct{}

func (p *StorageProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	storageType := config.StorageType
	if storageType == "" {
		storageType = "memory"
	}

	operation := config.StorageOperation
	if operation == "" {
		operation = "store"
	}

	key := config.StorageKey
	if key == "" {
		key = fmt.Sprintf("data_%d", time.Now().UnixNano())
	}

	switch operation {
	case "store":
		return p.storeData(storageType, key, input.Data)
	case "retrieve":
		return p.retrieveData(storageType, key)
	case "delete":
		return p.deleteData(storageType, key)
	default:
		return &ProcessingResult{
			Success: false,
			Error:   fmt.Sprintf("unsupported storage operation: %s", operation),
		}, nil
	}
}

func (p *StorageProcessor) storeData(storageType, key string, data map[string]interface{}) (*ProcessingResult, error) {
	// Simulate data storage
	time.Sleep(10 * time.Millisecond)

	result := map[string]interface{}{
		"storage_type": storageType,
		"operation":    "store",
		"key":          key,
		"stored_data":  data,
		"timestamp":    time.Now(),
		"size_bytes":   len(fmt.Sprintf("%v", data)),
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("Data stored successfully with key: %s", key),
	}, nil
}

func (p *StorageProcessor) retrieveData(storageType, key string) (*ProcessingResult, error) {
	// Simulate data retrieval
	time.Sleep(5 * time.Millisecond)

	// Mock retrieved data
	retrievedData := map[string]interface{}{
		"key":       key,
		"value":     "mock_stored_value",
		"timestamp": time.Now().Add(-1 * time.Hour),
	}

	result := map[string]interface{}{
		"storage_type":   storageType,
		"operation":      "retrieve",
		"key":            key,
		"retrieved_data": retrievedData,
		"found":          true,
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("Data retrieved successfully for key: %s", key),
	}, nil
}

func (p *StorageProcessor) deleteData(storageType, key string) (*ProcessingResult, error) {
	// Simulate data deletion
	time.Sleep(5 * time.Millisecond)

	result := map[string]interface{}{
		"storage_type": storageType,
		"operation":    "delete",
		"key":          key,
		"deleted":      true,
		"timestamp":    time.Now(),
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("Data deleted successfully for key: %s", key),
	}, nil
}

// NotifyProcessor handles notification operations
type NotifyProcessor struct{}

func (p *NotifyProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	notificationType := config.NotificationType
	if notificationType == "" {
		notificationType = "email"
	}

	recipients := config.NotificationRecipients
	if len(recipients) == 0 {
		return &ProcessingResult{
			Success: false,
			Error:   "no notification recipients specified",
		}, nil
	}

	message := config.NotificationMessage
	if message == "" {
		message = "Workflow notification"
	}

	// Process message template with input data
	processedMessage := p.processNotificationTemplate(message, input.Data)

	switch notificationType {
	case "email":
		return p.sendEmailNotification(recipients, processedMessage, config)
	case "sms":
		return p.sendSMSNotification(recipients, processedMessage, config)
	case "webhook":
		return p.sendWebhookNotification(recipients, processedMessage, input.Data, config)
	default:
		return &ProcessingResult{
			Success: false,
			Error:   fmt.Sprintf("unsupported notification type: %s", notificationType),
		}, nil
	}
}

func (p *NotifyProcessor) processNotificationTemplate(message string, data map[string]interface{}) string {
	result := message
	for key, value := range data {
		placeholder := fmt.Sprintf("{{%s}}", key)
		result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", value))
	}
	return result
}

func (p *NotifyProcessor) sendEmailNotification(recipients []string, message string, config NodeConfig) (*ProcessingResult, error) {
	// Simulate email sending
	time.Sleep(100 * time.Millisecond)

	results := []map[string]interface{}{}
	for _, recipient := range recipients {
		results = append(results, map[string]interface{}{
			"recipient": recipient,
			"status":    "sent",
			"type":      "email",
			"timestamp": time.Now(),
		})
	}

	result := map[string]interface{}{
		"notification_type": "email",
		"recipients":        recipients,
		"message":           message,
		"sent_count":        len(recipients),
		"results":           results,
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("Email notifications sent to %d recipients", len(recipients)),
	}, nil
}

func (p *NotifyProcessor) sendSMSNotification(recipients []string, message string, config NodeConfig) (*ProcessingResult, error) {
	// Simulate SMS sending
	time.Sleep(50 * time.Millisecond)

	results := []map[string]interface{}{}
	for _, recipient := range recipients {
		results = append(results, map[string]interface{}{
			"recipient": recipient,
			"status":    "sent",
			"type":      "sms",
			"timestamp": time.Now(),
		})
	}

	result := map[string]interface{}{
		"notification_type": "sms",
		"recipients":        recipients,
		"message":           message,
		"sent_count":        len(recipients),
		"results":           results,
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("SMS notifications sent to %d recipients", len(recipients)),
	}, nil
}

func (p *NotifyProcessor) sendWebhookNotification(recipients []string, message string, data map[string]interface{}, config NodeConfig) (*ProcessingResult, error) {
	// Simulate webhook sending
	time.Sleep(25 * time.Millisecond)

	results := []map[string]interface{}{}
	for _, recipient := range recipients {
		// Mock webhook response
		results = append(results, map[string]interface{}{
			"url":       recipient,
			"status":    "sent",
			"type":      "webhook",
			"response":  map[string]interface{}{"status": "ok", "code": 200},
			"timestamp": time.Now(),
		})
	}

	result := map[string]interface{}{
		"notification_type": "webhook",
		"urls":              recipients,
		"message":           message,
		"payload":           data,
		"sent_count":        len(recipients),
		"results":           results,
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("Webhook notifications sent to %d URLs", len(recipients)),
	}, nil
}

// WebhookReceiverProcessor handles incoming webhook processing
type WebhookReceiverProcessor struct{}

func (p *WebhookReceiverProcessor) Process(ctx context.Context, input ProcessingContext) (*ProcessingResult, error) {
	config := input.Node.Config

	expectedSignature := config.WebhookSignature
	secret := config.WebhookSecret

	// Extract webhook data from input
	webhookData, ok := input.Data["webhook_data"].(map[string]interface{})
	if !ok {
		return &ProcessingResult{
			Success: false,
			Error:   "no webhook data found in input",
		}, nil
	}

	// Verify webhook signature if provided
	if expectedSignature != "" && secret != "" {
		isValid := p.verifyWebhookSignature(webhookData, secret, expectedSignature)
		if !isValid {
			return &ProcessingResult{
				Success: false,
				Error:   "webhook signature verification failed",
			}, nil
		}
	}

	// Process webhook data based on source
	source, _ := webhookData["source"].(string)
	if source == "" {
		source = "unknown"
	}

	processedData := map[string]interface{}{
		"source":          source,
		"original_data":   webhookData,
		"processed_at":    time.Now(),
		"signature_valid": expectedSignature == "" || secret == "",
	}

	// Apply any data transformations specified in config
	if transformRules, exists := config.WebhookTransforms["transforms"]; exists {
		if rules, ok := transformRules.(map[string]interface{}); ok {
			for key, rule := range rules {
				if sourceField, ok := rule.(string); ok {
					if value, exists := webhookData[sourceField]; exists {
						processedData[key] = value
					}
				}
			}
		}
	}

	result := map[string]interface{}{
		"webhook_source":   source,
		"processed_data":   processedData,
		"original_payload": webhookData,
		"processing_time":  time.Now(),
	}

	return &ProcessingResult{
		Success: true,
		Data:    result,
		Message: fmt.Sprintf("Webhook from %s processed successfully", source),
	}, nil
}

func (p *WebhookReceiverProcessor) verifyWebhookSignature(data map[string]interface{}, secret, expectedSignature string) bool {
	// Convert data to JSON for signature verification
	payload, err := json.Marshal(data)
	if err != nil {
		return false
	}

	// Create HMAC signature
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(payload)
	computedSignature := hex.EncodeToString(h.Sum(nil))

	// Compare signatures (constant time comparison for security)
	return hmac.Equal([]byte(computedSignature), []byte(expectedSignature))
}
