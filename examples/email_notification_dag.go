package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/utils"

	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

func main() {
	var contactFormSchema = map[string]any{}
	content, err := os.ReadFile("app/schema.json")
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(content, &contactFormSchema); err != nil {
		panic(fmt.Errorf("failed to parse JSON schema: %w", err))
	}

	contactFormLayout, err := os.ReadFile("email/contact-form.html")
	if err != nil {
		panic(err)
	}

	flow := dag.NewDAG("Email Notification System", "email-notification", func(taskID string, result mq.Result) {
		fmt.Printf("Email notification workflow completed for task %s: %s\n", taskID, string(utils.RemoveRecursiveFromJSON(result.Payload, "html_content")))
	}, mq.WithSyncMode(true))

	// Add workflow nodes
	// Note: Page nodes have no timeout by default, allowing users unlimited time for form input
	flow.AddNode(dag.Page, "Contact Form", "ContactForm", &ConfigurableFormNode{Schema: contactFormSchema, HTMLLayout: string(contactFormLayout)}, true)
	flow.AddNode(dag.Function, "Validate Contact Data", "ValidateContact", &ValidateContactNode{})
	flow.AddNode(dag.Function, "Check User Type", "CheckUserType", &CheckUserTypeNode{})
	flow.AddNode(dag.Function, "Send Welcome Email", "SendWelcomeEmail", &SendWelcomeEmailNode{})
	flow.AddNode(dag.Function, "Send Premium Email", "SendPremiumEmail", &SendPremiumEmailNode{})
	flow.AddNode(dag.Function, "Send Standard Email", "SendStandardEmail", &SendStandardEmailNode{})
	flow.AddNode(dag.Page, "Success Page", "SuccessPage", &SuccessPageNode{})
	flow.AddNode(dag.Page, "Error Page", "ErrorPage", &EmailErrorPageNode{})

	// Define conditional flow
	flow.AddEdge(dag.Simple, "Form to Validation", "ContactForm", "ValidateContact")
	flow.AddCondition("ValidateContact", map[string]string{
		"valid":   "CheckUserType",
		"invalid": "ErrorPage",
	})
	flow.AddCondition("CheckUserType", map[string]string{
		"new_user":      "SendWelcomeEmail",
		"premium_user":  "SendPremiumEmail",
		"standard_user": "SendStandardEmail",
	})
	flow.AddCondition("SendWelcomeEmail", map[string]string{
		"sent":   "SuccessPage",
		"failed": "ErrorPage",
	})
	flow.AddCondition("SendPremiumEmail", map[string]string{
		"sent":   "SuccessPage",
		"failed": "ErrorPage",
	})
	flow.AddCondition("SendStandardEmail", map[string]string{
		"sent":   "SuccessPage",
		"failed": "ErrorPage",
	})

	// Start the flow
	if flow.Error != nil {
		panic(flow.Error)
	}

	fmt.Println("Starting Email Notification DAG server on http://0.0.0.0:8084")
	fmt.Println("Navigate to the URL to access the contact form")
	flow.Start(context.Background(), "0.0.0.0:8084")
}

// ConfigurableFormNode - Page node with JSONSchema-based fields and custom HTML layout
// Usage: Pass JSONSchema and HTML layout to the node for dynamic form rendering and validation

type ConfigurableFormNode struct {
	dag.Operation
	Schema           map[string]any // JSONSchema for fields and requirements
	HTMLLayout       string         // HTML layout template with placeholders for fields
	fieldsCache      []fieldInfo    // Cached field order and definitions
	cacheInitialized bool           // Whether cache is initialized
}

// fieldInfo caches field metadata for rendering
type fieldInfo struct {
	name         string
	order        int
	def          map[string]any
	definedIndex int // fallback to definition order
}

func (c *ConfigurableFormNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if task.Payload != nil && len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &inputData); err == nil {
			// Validate input against schema requirements
			validationErrors := validateAgainstSchema(inputData, c.Schema)
			if len(validationErrors) > 0 {
				inputData["validation_error"] = validationErrors[0] // Show first error
				bt, _ := json.Marshal(inputData)
				return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
			}
			return mq.Result{Payload: task.Payload, Ctx: ctx}
		}
	}

	// Initialize cache if not done
	if !c.cacheInitialized {
		c.fieldsCache = parseFieldsFromSchema(c.Schema)
		c.cacheInitialized = true
	}

	// Render form fields from cached field order
	formFieldsHTML := renderFieldsFromCache(c.fieldsCache)
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	layout := strings.Replace(c.HTMLLayout, "{{form_fields}}", formFieldsHTML, 1)
	rs, err := parser.ParseTemplate(layout, map[string]any{
		"task_id": ctx.Value("task_id"),
	})
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	data := map[string]any{
		"html_content": rs,
		"step":         "form",
	}
	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, Ctx: ctx}
}

// validateAgainstSchema checks inputData against JSONSchema requirements
func validateAgainstSchema(inputData map[string]any, schema map[string]any) []string {
	var errors []string
	if _, ok := schema["properties"].(map[string]any); ok {
		if required, ok := schema["required"].([]any); ok {
			for _, field := range required {
				fname := field.(string)
				if val, exists := inputData[fname]; !exists || val == "" {
					errors = append(errors, fname+" is required")
				}
			}
		}
		// Add more validation as needed (type, format, etc.)
	}
	return errors
}

// parseFieldsFromSchema extracts and sorts fields from schema, preserving order
func parseFieldsFromSchema(schema map[string]any) []fieldInfo {
	var fields []fieldInfo
	if props, ok := schema["properties"].(map[string]any); ok {
		keyOrder := make([]string, 0, len(props))
		for k := range props {
			keyOrder = append(keyOrder, k)
		}
		for idx, name := range keyOrder {
			field := props[name].(map[string]any)
			order := -1
			if o, ok := field["order"].(int); ok {
				order = o
			} else if o, ok := field["order"].(float64); ok {
				order = int(o)
			}
			fields = append(fields, fieldInfo{name: name, order: order, def: field, definedIndex: idx})
		}
		if len(fields) > 1 {
			sort.SliceStable(fields, func(i, j int) bool {
				if fields[i].order != -1 && fields[j].order != -1 {
					return fields[i].order < fields[j].order
				} else if fields[i].order != -1 {
					return true
				} else if fields[j].order != -1 {
					return false
				}
				return fields[i].definedIndex < fields[j].definedIndex
			})
		}
	}
	return fields
}

// renderFieldsFromCache generates HTML for form fields from cached field order
func renderFieldsFromCache(fields []fieldInfo) string {
	var html strings.Builder
	for _, f := range fields {
		label := f.name
		if l, ok := f.def["title"].(string); ok {
			label = l
		}
		// UI config
		ui := map[string]any{}
		if uiRaw, ok := f.def["ui"].(map[string]any); ok {
			ui = uiRaw
		}
		// Control type
		controlType := "input"
		if ct, ok := ui["control"].(string); ok {
			controlType = ct
		}
		// CSS classes
		classes := "form-group"
		if cls, ok := ui["class"].(string); ok {
			classes = cls
		}
		// Name attribute
		nameAttr := f.name
		if n, ok := ui["name"].(string); ok {
			nameAttr = n
		}
		// Type
		typeStr := "text"
		if t, ok := f.def["type"].(string); ok {
			switch t {
			case "string":
				typeStr = "text"
			case "email":
				typeStr = "email"
			case "number":
				typeStr = "number"
			case "textarea":
				typeStr = "textarea"
			}
		}
		// Render control
		if controlType == "textarea" || typeStr == "textarea" {
			html.WriteString(fmt.Sprintf(`<div class="%s"><label for="%s">%s:</label><textarea id="%s" name="%s" placeholder="%s"></textarea></div>`, classes, nameAttr, label, nameAttr, nameAttr, label))
		} else if controlType == "select" {
			// Optionally support select with options in ui["options"]
			optionsHTML := ""
			if opts, ok := ui["options"].([]any); ok {
				for _, opt := range opts {
					optStr := fmt.Sprintf("%v", opt)
					optionsHTML += fmt.Sprintf(`<option value="%s">%s</option>`, optStr, optStr)
				}
			}
			html.WriteString(fmt.Sprintf(`<div class="%s"><label for="%s">%s:</label><select id="%s" name="%s">%s</select></div>`, classes, nameAttr, label, nameAttr, nameAttr, optionsHTML))
		} else {
			html.WriteString(fmt.Sprintf(`<div class="%s"><label for="%s">%s:</label><input type="%s" id="%s" name="%s" placeholder="%s"></div>`, classes, nameAttr, label, typeStr, nameAttr, nameAttr, label))
		}
	}
	return html.String()
}

// ValidateContactNode - Validates contact form data
type ValidateContactNode struct {
	dag.Operation
}

func (v *ValidateContactNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{
			Error: fmt.Errorf("invalid input data: %v", err),
			Ctx:   ctx,
		}
	}

	// Extract form data
	firstName, _ := inputData["first_name"].(string)
	lastName, _ := inputData["last_name"].(string)
	email, _ := inputData["email"].(string)
	userType, _ := inputData["user_type"].(string)
	priority, _ := inputData["priority"].(string)
	subject, _ := inputData["subject"].(string)
	message, _ := inputData["message"].(string)

	// Validate required fields
	if firstName == "" {
		inputData["validation_error"] = "First name is required"
		inputData["error_field"] = "first_name"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	if lastName == "" {
		inputData["validation_error"] = "Last name is required"
		inputData["error_field"] = "last_name"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	if email == "" {
		inputData["validation_error"] = "Email address is required"
		inputData["error_field"] = "email"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	// Validate email format
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(email) {
		inputData["validation_error"] = "Please enter a valid email address"
		inputData["error_field"] = "email"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	if userType == "" {
		inputData["validation_error"] = "Please select your user type"
		inputData["error_field"] = "user_type"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	if priority == "" {
		inputData["validation_error"] = "Please select a priority level"
		inputData["error_field"] = "priority"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	if subject == "" {
		inputData["validation_error"] = "Subject is required"
		inputData["error_field"] = "subject"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	if message == "" {
		inputData["validation_error"] = "Message is required"
		inputData["error_field"] = "message"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	// Check for spam patterns
	spamPatterns := []string{"click here", "free money", "act now", "limited time"}
	messageLower := strings.ToLower(message)
	subjectLower := strings.ToLower(subject)

	for _, pattern := range spamPatterns {
		if strings.Contains(messageLower, pattern) || strings.Contains(subjectLower, pattern) {
			inputData["validation_error"] = "Message contains prohibited content"
			inputData["error_field"] = "message"
			bt, _ := json.Marshal(inputData)
			return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
		}
	}

	// All validations passed
	validatedData := map[string]any{
		"first_name":        firstName,
		"last_name":         lastName,
		"full_name":         fmt.Sprintf("%s %s", firstName, lastName),
		"email":             email,
		"user_type":         userType,
		"priority":          priority,
		"subject":           subject,
		"message":           message,
		"validated_at":      time.Now().Format("2006-01-02 15:04:05"),
		"validation_status": "success",
		"message_length":    len(message),
	}

	bt, _ := json.Marshal(validatedData)
	return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "valid"}
}

// CheckUserTypeNode - Determines routing based on user type
type CheckUserTypeNode struct {
	dag.Operation
}

func (c *CheckUserTypeNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	userType, _ := inputData["user_type"].(string)

	// Add timestamp and additional metadata
	inputData["processed_at"] = time.Now().Format("2006-01-02 15:04:05")
	inputData["routing_decision"] = userType

	var conditionStatus string
	switch userType {
	case "new":
		conditionStatus = "new_user"
		inputData["email_template"] = "welcome"
	case "premium":
		conditionStatus = "premium_user"
		inputData["email_template"] = "premium"
	case "standard":
		conditionStatus = "standard_user"
		inputData["email_template"] = "standard"
	default:
		conditionStatus = "standard_user"
		inputData["email_template"] = "standard"
	}

	fmt.Printf("🔀 Routing decision: %s -> %s\n", userType, conditionStatus)

	bt, _ := json.Marshal(inputData)
	return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: conditionStatus}
}

// Email sending nodes
type SendWelcomeEmailNode struct {
	dag.Operation
}

func (s *SendWelcomeEmailNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return s.sendEmail(ctx, task, "Welcome to our platform! 🎉")
}

type SendPremiumEmailNode struct {
	dag.Operation
}

func (s *SendPremiumEmailNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return s.sendEmail(ctx, task, "Premium Support Response 💎")
}

type SendStandardEmailNode struct {
	dag.Operation
}

func (s *SendStandardEmailNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return s.sendEmail(ctx, task, "Thank you for contacting us ⭐")
}

// Helper method for email sending
func (s *SendWelcomeEmailNode) sendEmail(ctx context.Context, task *mq.Task, emailType string) mq.Result {
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	email, _ := inputData["email"].(string)

	// Simulate email sending delay
	time.Sleep(300 * time.Millisecond)

	// Simulate occasional failures for demo purposes
	timestamp := time.Now()
	success := timestamp.Second()%15 != 0 // 93% success rate

	if !success {
		errorData := inputData
		errorData["email_status"] = "failed"
		errorData["error_message"] = "Email gateway temporarily unavailable. Please try again."
		errorData["sent_at"] = timestamp.Format("2006-01-02 15:04:05")
		errorData["retry_suggested"] = true

		bt, _ := json.Marshal(errorData)
		return mq.Result{
			Payload:         bt,
			Ctx:             ctx,
			ConditionStatus: "failed",
		}
	}

	// Generate mock email ID and response
	emailID := fmt.Sprintf("EMAIL_%d_%s", timestamp.Unix(), email[0:3])

	resultData := inputData
	resultData["email_status"] = "sent"
	resultData["email_id"] = emailID
	resultData["email_type"] = emailType
	resultData["sent_at"] = timestamp.Format("2006-01-02 15:04:05")
	resultData["delivery_estimate"] = "Instant"
	resultData["gateway"] = "MockEmail Gateway"

	fmt.Printf("📧 Email sent successfully! Type: %s, ID: %s, To: %s\n", emailType, emailID, email)

	bt, _ := json.Marshal(resultData)
	return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "sent"}
}

// Helper methods for other email nodes
func (s *SendPremiumEmailNode) sendEmail(ctx context.Context, task *mq.Task, emailType string) mq.Result {
	node := &SendWelcomeEmailNode{}
	return node.sendEmail(ctx, task, emailType)
}

func (s *SendStandardEmailNode) sendEmail(ctx context.Context, task *mq.Task, emailType string) mq.Result {
	node := &SendWelcomeEmailNode{}
	return node.sendEmail(ctx, task, emailType)
}

// SuccessPageNode - Shows successful email result
type SuccessPageNode struct {
	dag.Operation
}

func (s *SuccessPageNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	htmlTemplate, err := os.ReadFile("email/success.html")
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to read success template: %v", err)}
	}

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(string(htmlTemplate), inputData)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	finalData := map[string]any{
		"html_content": rs,
		"result":       inputData,
		"step":         "success",
	}
	bt, _ := json.Marshal(finalData)
	return mq.Result{Payload: bt, Ctx: ctx}
}

// EmailErrorPageNode - Shows validation or sending errors
type EmailErrorPageNode struct {
	dag.Operation
}

func (e *EmailErrorPageNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	// Determine error type and message
	errorMessage, _ := inputData["validation_error"].(string)
	errorField, _ := inputData["error_field"].(string)
	emailError, _ := inputData["error_message"].(string)

	if errorMessage == "" && emailError != "" {
		errorMessage = emailError
		errorField = "email_sending"
	}
	if errorMessage == "" {
		errorMessage = "An unknown error occurred"
	}

	htmlTemplate, err := os.ReadFile("email/error.html")
	if err != nil {
		return mq.Result{Error: fmt.Errorf("failed to read error template: %v", err)}
	}

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	templateData := map[string]any{
		"error_message":   errorMessage,
		"error_field":     errorField,
		"retry_suggested": inputData["retry_suggested"],
	}

	rs, err := parser.ParseTemplate(string(htmlTemplate), templateData)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	finalData := map[string]any{
		"html_content": rs,
		"error_data":   inputData,
		"step":         "error",
	}
	bt, _ := json.Marshal(finalData)
	return mq.Result{Payload: bt, Ctx: ctx}
}
