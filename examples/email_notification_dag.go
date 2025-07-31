package main

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq/dag"

	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

func main() {
	contactFormSchema := map[string]any{
		"properties": map[string]any{
			"first_name": map[string]any{
				"type":  "string",
				"title": "👤 First Name",
				"order": 1,
				"ui": map[string]any{
					"control": "input",
					"class":   "form-group",
					"name":    "first_name",
				},
			},
			"last_name": map[string]any{
				"type":  "string",
				"title": "👤 Last Name",
				"order": 2,
				"ui": map[string]any{
					"control": "input",
					"class":   "form-group",
					"name":    "last_name",
				},
			},
			"email": map[string]any{
				"type":  "email",
				"title": "📧 Email Address",
				"order": 3,
				"ui": map[string]any{
					"control": "input",
					"class":   "form-group",
					"name":    "email",
				},
			},
			"user_type": map[string]any{
				"type":  "string",
				"title": "👥 User Type",
				"order": 4,
				"ui": map[string]any{
					"control": "select",
					"class":   "form-group",
					"name":    "user_type",
					"options": []any{"new", "premium", "standard"},
				},
			},
			"priority": map[string]any{
				"type":  "string",
				"title": "🚨 Priority Level",
				"order": 5,
				"ui": map[string]any{
					"control": "select",
					"class":   "form-group",
					"name":    "priority",
					"options": []any{"low", "medium", "high", "urgent"},
				},
			},
			"subject": map[string]any{
				"type":  "string",
				"title": "📋 Subject",
				"order": 6,
				"ui": map[string]any{
					"control": "input",
					"class":   "form-group",
					"name":    "subject",
				},
			},
			"message": map[string]any{
				"type":  "textarea",
				"title": "💬 Message",
				"order": 7,
				"ui": map[string]any{
					"control": "textarea",
					"class":   "form-group",
					"name":    "message",
				},
			},
		},
		"required": []any{"first_name", "last_name", "email", "user_type", "priority", "subject", "message"},
	}

	contactFormLayout := `
<!DOCTYPE html>
<html>
<head>
    <title>Contact Us - Email Notification System</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 700px; margin: 50px auto; padding: 20px; background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); color: white; min-height: 100vh; }
        .form-container { background: rgba(255, 255, 255, 0.1); padding: 40px; border-radius: 20px; backdrop-filter: blur(15px); box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4); border: 1px solid rgba(255, 255, 255, 0.2); }
        h1 { text-align: center; margin-bottom: 10px; text-shadow: 2px 2px 4px rgba(0,0,0,0.3); font-size: 2.2em; }
        .subtitle { text-align: center; margin-bottom: 30px; opacity: 0.9; font-size: 1.1em; }
        .form-group { margin-bottom: 25px; }
        label { display: block; margin-bottom: 8px; font-weight: 600; text-shadow: 1px 1px 2px rgba(0,0,0,0.3); font-size: 1.1em; }
        input, textarea, select { width: 100%; padding: 15px; border: none; border-radius: 10px; font-size: 16px; background: rgba(255, 255, 255, 0.2); color: white; backdrop-filter: blur(5px); transition: all 0.3s ease; border: 1px solid rgba(255, 255, 255, 0.3); }
        input:focus, textarea:focus, select:focus { outline: none; background: rgba(255, 255, 255, 0.3); border: 1px solid rgba(255, 255, 255, 0.6); transform: translateY(-2px); box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2); }
        input::placeholder, textarea::placeholder { color: rgba(255, 255, 255, 0.7); }
        textarea { height: 120px; resize: vertical; }
        select { cursor: pointer; }
        select option { background: #2a5298; color: white; }
        .form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        @media (max-width: 600px) { .form-row { grid-template-columns: 1fr; } }
        button { background: linear-gradient(45deg, #FF6B6B, #4ECDC4); color: white; padding: 18px 40px; border: none; border-radius: 30px; cursor: pointer; font-size: 18px; font-weight: bold; width: 100%; transition: all 0.3s ease; box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3); text-transform: uppercase; letter-spacing: 1px; }
        button:hover { transform: translateY(-3px); box-shadow: 0 8px 25px rgba(0, 0, 0, 0.4); }
        .info-box { background: rgba(255, 255, 255, 0.15); padding: 20px; border-radius: 12px; margin-bottom: 25px; text-align: center; border-left: 4px solid #4ECDC4; }
        .feature-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .feature-item { background: rgba(255, 255, 255, 0.1); padding: 15px; border-radius: 8px; text-align: center; font-size: 14px; }
    </style>
</head>
<body>
    <div class="form-container">
        <h1>📧 Contact Us</h1>
        <div class="subtitle">Advanced Email Notification System with DAG Workflow</div>

        <div class="info-box">
            <p><strong>🔄 Smart Routing:</strong> Our system automatically routes your message based on your user type and preferences.</p>
        </div>

        <div class="feature-list">
            <div class="feature-item">
                <strong>📱 Instant Notifications</strong><br>
                Real-time email delivery
            </div>
            <div class="feature-item">
                <strong>🎯 Smart Targeting</strong><br>
                User-specific content
            </div>
            <div class="feature-item">
                <strong>🔒 Secure Processing</strong><br>
                Enterprise-grade security
            </div>
        </div>

        <form method="post" action="/process?task_id={{task_id}}&next=true">
            <div class="form-row">
                {{form_fields}}
            </div>

            <button type="submit">🚀 Send Message</button>
        </form>
    </div>
</body>
</html>`

	flow := dag.NewDAG("Email Notification System", "email-notification", func(taskID string, result mq.Result) {
		fmt.Printf("Email notification workflow completed for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))

	// Add workflow nodes
	// Note: Page nodes have no timeout by default, allowing users unlimited time for form input
	flow.AddNode(dag.Page, "Contact Form", "ContactForm", &ConfigurableFormNode{Schema: contactFormSchema, HTMLLayout: contactFormLayout}, true)
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

	htmlTemplate := `
<!DOCTYPE html>
<html>
<head>
    <title>Message Sent Successfully</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 700px;
            margin: 50px auto;
            padding: 20px;
            background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);
            color: white;
        }
        .result-container {
            background: rgba(255, 255, 255, 0.1);
            padding: 40px;
            border-radius: 20px;
            backdrop-filter: blur(15px);
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
            text-align: center;
        }
        .success-icon {
            font-size: 80px;
            margin-bottom: 20px;
            animation: bounce 2s infinite;
        }
        @keyframes bounce {
            0%, 20%, 50%, 80%, 100% { transform: translateY(0); }
            40% { transform: translateY(-10px); }
            60% { transform: translateY(-5px); }
        }
        h1 {
            margin-bottom: 30px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            font-size: 2.5em;
        }
        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
            text-align: left;
        }
        .info-item {
            background: rgba(255, 255, 255, 0.15);
            padding: 20px;
            border-radius: 12px;
            border-left: 4px solid #4ECDC4;
        }
        .info-label {
            font-weight: bold;
            margin-bottom: 8px;
            opacity: 0.9;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .info-value {
            font-size: 16px;
            word-break: break-word;
        }
        .message-preview {
            background: rgba(255, 255, 255, 0.1);
            padding: 25px;
            border-radius: 12px;
            margin: 30px 0;
            text-align: left;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        .actions {
            margin-top: 40px;
        }
        .btn {
            background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
            margin: 0 15px;
            text-decoration: none;
            display: inline-block;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .btn:hover {
            transform: translateY(-3px);
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.3);
        }
        .status-badge {
            background: #4CAF50;
            color: white;
            padding: 8px 20px;
            border-radius: 25px;
            font-size: 14px;
            font-weight: bold;
            display: inline-block;
            margin: 10px 0;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .workflow-info {
            background: rgba(255, 255, 255, 0.1);
            padding: 20px;
            border-radius: 12px;
            margin-top: 30px;
            font-size: 14px;
            opacity: 0.9;
        }
    </style>
</head>
<body>
    <div class="result-container">
        <div class="success-icon">✅</div>
        <h1>Message Sent Successfully!</h1>

        <div class="status-badge">{{email_status}}</div>

        <div class="info-grid">
            <div class="info-item">
                <div class="info-label">👤 Recipient</div>
                <div class="info-value">{{full_name}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">📧 Email Address</div>
                <div class="info-value">{{email}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">🆔 Email ID</div>
                <div class="info-value">{{email_id}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">⏰ Sent At</div>
                <div class="info-value">{{sent_at}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">📨 Email Type</div>
                <div class="info-value">{{email_type}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">👥 User Type</div>
                <div class="info-value">{{user_type}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">🚨 Priority</div>
                <div class="info-value">{{priority}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">🚚 Delivery</div>
                <div class="info-value">{{delivery_estimate}}</div>
            </div>
        </div>

        <div class="message-preview">
            <div class="info-label">📋 Subject:</div>
            <div class="info-value" style="margin: 10px 0; font-weight: bold; font-size: 18px;">
                {{subject}}
            </div>
            <div class="info-label">💬 Message ({{message_length}} chars):</div>
            <div class="info-value" style="margin-top: 15px; font-style: italic; line-height: 1.6;">
                "{{message}}"
            </div>
        </div>

        <div class="actions">
            <a href="/" class="btn">📧 Send Another Message</a>
            <a href="/api/metrics" class="btn">📊 View Metrics</a>
        </div>

        <div class="workflow-info">
            <strong>🔄 Workflow Details:</strong><br>
            Gateway: {{gateway}} | Template: {{email_template}} | Processed: {{processed_at}}<br>
            This message was processed through our advanced DAG workflow system with conditional routing.
        </div>
    </div>
</body>
</html>`

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(htmlTemplate, inputData)
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

	htmlTemplate := `
<!DOCTYPE html>
<html>
<head>
    <title>Email Error</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 700px;
            margin: 50px auto;
            padding: 20px;
            background: linear-gradient(135deg, #FF6B6B 0%, #FF5722 100%);
            color: white;
        }
        .error-container {
            background: rgba(255, 255, 255, 0.1);
            padding: 40px;
            border-radius: 20px;
            backdrop-filter: blur(15px);
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
            text-align: center;
        }
        .error-icon {
            font-size: 80px;
            margin-bottom: 20px;
            animation: shake 0.5s ease-in-out infinite alternate;
        }
        @keyframes shake {
            0% { transform: translateX(0); }
            100% { transform: translateX(5px); }
        }
        h1 {
            margin-bottom: 30px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            font-size: 2.5em;
        }
        .error-message {
            background: rgba(255, 255, 255, 0.2);
            padding: 25px;
            border-radius: 12px;
            margin: 25px 0;
            font-size: 18px;
            border-left: 6px solid #FFB6B6;
            line-height: 1.6;
        }
        .error-details {
            background: rgba(255, 255, 255, 0.15);
            padding: 20px;
            border-radius: 12px;
            margin: 25px 0;
            text-align: left;
        }
        .actions {
            margin-top: 40px;
        }
        .btn {
            background: linear-gradient(45deg, #4ECDC4, #44A08D);
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
            margin: 0 15px;
            text-decoration: none;
            display: inline-block;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .btn:hover {
            transform: translateY(-3px);
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.3);
        }
        .retry-btn {
            background: linear-gradient(45deg, #FFA726, #FF9800);
        }
    </style>
</head>
<body>
    <div class="error-container">
        <div class="error-icon">❌</div>
        <h1>Email Processing Error</h1>

        <div class="error-message">
            {{error_message}}
        </div>

        {{if error_field}}
        <div class="error-details">
            <strong>🎯 Error Field:</strong> {{error_field}}<br>
            <strong>⚡ Action Required:</strong> Please correct the highlighted field and try again.<br>
            <strong>💡 Tip:</strong> Make sure all required fields are properly filled out.
        </div>
        {{end}}

        {{if retry_suggested}}
        <div class="error-details">
            <strong>⚠️ Temporary Issue:</strong> This appears to be a temporary system issue.
            Please try sending your message again in a few moments.<br>
            <strong>🔄 Auto-Retry:</strong> Our system will automatically retry failed deliveries.
        </div>
        {{end}}

        <div class="actions">
            <a href="/" class="btn retry-btn">🔄 Try Again</a>
            <a href="/api/status" class="btn">📊 Check Status</a>
        </div>

        <div style="margin-top: 30px; font-size: 14px; opacity: 0.8;">
            🔄 DAG Error Handler | Email Notification Workflow Failed<br>
            Our advanced routing system ensures reliable message delivery.
        </div>
    </div>
</body>
</html>`

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	templateData := map[string]any{
		"error_message":   errorMessage,
		"error_field":     errorField,
		"retry_suggested": inputData["retry_suggested"],
	}

	rs, err := parser.ParseTemplate(htmlTemplate, templateData)
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
