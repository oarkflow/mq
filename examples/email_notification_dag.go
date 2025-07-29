package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq/dag"

	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

func main() {
	flow := dag.NewDAG("Email Notification System", "email-notification", func(taskID string, result mq.Result) {
		fmt.Printf("Email notification workflow completed for task %s: %s\n", taskID, string(result.Payload))
	})

	// Add workflow nodes
	// Note: Page nodes have no timeout by default, allowing users unlimited time for form input
	flow.AddNode(dag.Page, "Contact Form", "ContactForm", &ContactFormNode{}, true)
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

// ContactFormNode - Contact form with validation
type ContactFormNode struct {
	dag.Operation
}

func (c *ContactFormNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Check if this is a form submission
	var inputData map[string]any
	if task.Payload != nil && len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &inputData); err == nil {
			// If we have valid input data, pass it through for validation
			return mq.Result{Payload: task.Payload, Ctx: ctx}
		}
	}

	// Otherwise, show the form
	htmlTemplate := `
<!DOCTYPE html>
<html>
<head>
    <title>Contact Us - Email Notification System</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            max-width: 700px;
            margin: 50px auto;
            padding: 20px;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            min-height: 100vh;
        }
        .form-container {
            background: rgba(255, 255, 255, 0.1);
            padding: 40px;
            border-radius: 20px;
            backdrop-filter: blur(15px);
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        h1 {
            text-align: center;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            font-size: 2.2em;
        }
        .subtitle {
            text-align: center;
            margin-bottom: 30px;
            opacity: 0.9;
            font-size: 1.1em;
        }
        .form-group {
            margin-bottom: 25px;
        }
        label {
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
            text-shadow: 1px 1px 2px rgba(0,0,0,0.3);
            font-size: 1.1em;
        }
        input, textarea, select {
            width: 100%;
            padding: 15px;
            border: none;
            border-radius: 10px;
            font-size: 16px;
            background: rgba(255, 255, 255, 0.2);
            color: white;
            backdrop-filter: blur(5px);
            transition: all 0.3s ease;
            border: 1px solid rgba(255, 255, 255, 0.3);
        }
        input:focus, textarea:focus, select:focus {
            outline: none;
            background: rgba(255, 255, 255, 0.3);
            border: 1px solid rgba(255, 255, 255, 0.6);
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        }
        input::placeholder, textarea::placeholder {
            color: rgba(255, 255, 255, 0.7);
        }
        textarea {
            height: 120px;
            resize: vertical;
        }
        select {
            cursor: pointer;
        }
        select option {
            background: #2a5298;
            color: white;
        }
        .form-row {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }
        @media (max-width: 600px) {
            .form-row {
                grid-template-columns: 1fr;
            }
        }
        button {
            background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
            color: white;
            padding: 18px 40px;
            border: none;
            border-radius: 30px;
            cursor: pointer;
            font-size: 18px;
            font-weight: bold;
            width: 100%;
            transition: all 0.3s ease;
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        button:hover {
            transform: translateY(-3px);
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.4);
        }
        .info-box {
            background: rgba(255, 255, 255, 0.15);
            padding: 20px;
            border-radius: 12px;
            margin-bottom: 25px;
            text-align: center;
            border-left: 4px solid #4ECDC4;
        }
        .feature-list {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .feature-item {
            background: rgba(255, 255, 255, 0.1);
            padding: 15px;
            border-radius: 8px;
            text-align: center;
            font-size: 14px;
        }
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
                <div class="form-group">
                    <label for="first_name">👤 First Name:</label>
                    <input type="text" id="first_name" name="first_name"
                           placeholder="Enter your first name"
                           required>
                </div>
                <div class="form-group">
                    <label for="last_name">👤 Last Name:</label>
                    <input type="text" id="last_name" name="last_name"
                           placeholder="Enter your last name"
                           required>
                </div>
            </div>

            <div class="form-group">
                <label for="email">📧 Email Address:</label>
                <input type="email" id="email" name="email"
                       placeholder="your.email@example.com"
                       required>
            </div>

            <div class="form-row">
                <div class="form-group">
                    <label for="user_type">👥 User Type:</label>
                    <select id="user_type" name="user_type" required>
                        <option value="">Select your type</option>
                        <option value="new">🆕 New User</option>
                        <option value="standard">⭐ Standard User</option>
                        <option value="premium">💎 Premium User</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="priority">🚨 Priority Level:</label>
                    <select id="priority" name="priority" required>
                        <option value="">Select priority</option>
                        <option value="low">🟢 Low</option>
                        <option value="medium">🟡 Medium</option>
                        <option value="high">🔴 High</option>
                        <option value="urgent">⚡ Urgent</option>
                    </select>
                </div>
            </div>

            <div class="form-group">
                <label for="subject">📋 Subject:</label>
                <input type="text" id="subject" name="subject"
                       placeholder="Brief description of your inquiry"
                       required maxlength="100">
            </div>

            <div class="form-group">
                <label for="message">💬 Message:</label>
                <textarea id="message" name="message"
                         placeholder="Please provide detailed information about your request..."
                         required maxlength="1000"></textarea>
            </div>

            <button type="submit">🚀 Send Message</button>
        </form>
    </div>
</body>
</html>`

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(htmlTemplate, map[string]any{
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
