package main

import (
	"context"
	"fmt"
	"regexp"
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
	flow := dag.NewDAG("SMS Sender", "sms-sender", func(taskID string, result mq.Result) {
		fmt.Printf("SMS workflow completed for task %s: %s\n", taskID, string(utils.RemoveRecursiveFromJSON(result.Payload, "html_content")))
	}, mq.WithSyncMode(true))

	// Add SMS workflow nodes
	// Note: Page nodes have no timeout by default, allowing users unlimited time for form input

	flow.AddDAGNode(dag.Page, "Login", "login", loginSubDAG().Clone(), true)
	flow.AddNode(dag.Page, "SMS Form", "SMSForm", &SMSFormNode{})
	flow.AddNode(dag.Function, "Validate Input", "ValidateInput", &ValidateInputNode{})
	flow.AddNode(dag.Function, "Send SMS", "SendSMS", &SendSMSNode{})
	flow.AddNode(dag.Page, "SMS Result", "SMSResult", &SMSResultNode{})
	flow.AddNode(dag.Page, "Error Page", "ErrorPage", &ErrorPageNode{})

	// Define edges for SMS workflow
	flow.AddEdge(dag.Simple, "Login to Form", "login", "SMSForm")
	flow.AddEdge(dag.Simple, "Form to Validation", "SMSForm", "ValidateInput")
	flow.AddCondition("ValidateInput", map[string]string{"valid": "SendSMS"}) // Removed invalid -> ErrorPage since we use ResetTo
	flow.AddCondition("SendSMS", map[string]string{"sent": "SMSResult", "failed": "ErrorPage"})

	// Start the flow
	if flow.Error != nil {
		panic(flow.Error)
	}

	fmt.Println("Starting SMS DAG server on http://0.0.0.0:8083")
	fmt.Println("Navigate to the URL to access the SMS form")
	flow.Start(context.Background(), "0.0.0.0:8083")
}

// loginSubDAG creates a login sub-DAG with page for authentication
func loginSubDAG() *dag.DAG {
	login := dag.NewDAG("Login Sub DAG", "login-sub-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Login Sub DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))

	login.
		AddNode(dag.Page, "Login Page", "login-page", &LoginPage{}).
		AddNode(dag.Function, "Verify Credentials", "verify-credentials", &VerifyCredentials{}).
		AddNode(dag.Function, "Generate Token", "generate-token", &GenerateToken{}).
		AddEdge(dag.Simple, "Login to Verify", "login-page", "verify-credentials").
		AddEdge(dag.Simple, "Verify to Token", "verify-credentials", "generate-token")

	return login
}

type LoginPage struct {
	dag.Operation
}

func (p *LoginPage) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Check if this is a form submission
	var inputData map[string]any
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &inputData); err == nil {
			// Check if we have form data (username/password)
			if formData, ok := inputData["form"].(map[string]any); ok {
				// This is a form submission, pass it through for verification
				credentials := map[string]any{
					"username": formData["username"],
					"password": formData["password"],
				}
				inputData["credentials"] = credentials
				updatedPayload, _ := json.Marshal(inputData)
				return mq.Result{Payload: updatedPayload, Ctx: ctx}
			}
		}
	}

	// Otherwise, show the form
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		data = make(map[string]any)
	}

	// HTML content for login page
	htmlContent := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Phone Processing System - Login</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            margin: 0;
            padding: 0;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .login-container {
            background: white;
            padding: 2rem;
            border-radius: 10px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.2);
            width: 100%;
            max-width: 400px;
        }
        .login-header {
            text-align: center;
            margin-bottom: 2rem;
        }
        .login-header h1 {
            color: #333;
            margin: 0;
            font-size: 1.8rem;
        }
        .login-header p {
            color: #666;
            margin: 0.5rem 0 0 0;
        }
        .form-group {
            margin-bottom: 1.5rem;
        }
        .form-group label {
            display: block;
            margin-bottom: 0.5rem;
            color: #333;
            font-weight: 500;
        }
        .form-group input {
            width: 100%;
            padding: 0.75rem;
            border: 2px solid #e1e5e9;
            border-radius: 5px;
            font-size: 1rem;
            transition: border-color 0.3s;
            box-sizing: border-box;
        }
        .form-group input:focus {
            outline: none;
            border-color: #667eea;
        }
        .login-btn {
            width: 100%;
            padding: 0.75rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 5px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s;
        }
        .login-btn:hover {
            transform: translateY(-2px);
        }
        .login-btn:active {
            transform: scale(0.98);
        }
        .status-message {
            margin-top: 1rem;
            padding: 0.5rem;
            border-radius: 5px;
            text-align: center;
            font-weight: 500;
        }
        .status-success {
            background-color: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .status-error {
            background-color: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="login-header">
            <h1>📱 Phone Processing System</h1>
            <p>Please login to continue</p>
        </div>
        {{if error}}
        <div class="status-message status-error">
            <strong>❌ Login Failed:</strong> {{error}}
        </div>
        {{end}}
        <form method="post" action="/process?task_id={{task_id}}&next=true" id="loginForm">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" required placeholder="Enter your username" value="{{username}}">
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" required placeholder="Enter your password">
            </div>
            <button type="submit" class="login-btn">Login</button>
        </form>
        <div id="statusMessage"></div>
    </div>

    <script>
        // Form will submit naturally to the action URL
        document.getElementById('loginForm').addEventListener('submit', function(e) {
            // Optional: Add loading state
            const btn = e.target.querySelector('.login-btn');
            btn.textContent = 'Logging in...';
            btn.disabled = true;
        });
    </script>
</body>
</html>`

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(htmlContent, map[string]any{
		"task_id":  ctx.Value("task_id"),
		"error":    data["error"],
		"username": data["username"],
	})
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	resultData := map[string]any{
		"html_content": rs,
		"step":         "login",
		"data":         data,
	}

	resultPayload, _ := json.Marshal(resultData)
	return mq.Result{
		Payload: resultPayload,
		Ctx:     ctx,
	}
}

type VerifyCredentials struct {
	dag.Operation
}

func (p *VerifyCredentials) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("VerifyCredentials Error: %s", err.Error()), Ctx: ctx}
	}

	username, _ := data["username"].(string)
	password, _ := data["password"].(string)

	// Simple verification logic
	if username == "admin" && password == "password123" {
		data["authenticated"] = true
		data["user_role"] = "administrator"
	} else {
		data["authenticated"] = false
		data["error"] = "Invalid credentials"
		data["validation_error"] = "Phone number is required"
		data["error_field"] = "phone"
		bt, _ := json.Marshal(data)
		return mq.Result{
			Payload: bt,
			Ctx:     ctx,
			ResetTo: "back", // Reset to form instead of going to error page
		}
	}
	delete(data, "html_content")
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type GenerateToken struct {
	dag.Operation
}

func (p *GenerateToken) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("GenerateToken Error: %s", err.Error()), Ctx: ctx}
	}

	if authenticated, ok := data["authenticated"].(bool); ok && authenticated {
		data["auth_token"] = "jwt_token_123456789"
		data["token_expires"] = "2025-09-19T13:00:00Z"
		data["login_success"] = true
	}

	delete(data, "html_content")
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{
		Payload: updatedPayload,
		Ctx:     ctx,
		Status:  mq.Completed,
	}
}

// SMSFormNode - Initial form to collect SMS data
type SMSFormNode struct {
	dag.Operation
}

func (s *SMSFormNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if len(task.Payload) > 0 {
		json.Unmarshal(task.Payload, &inputData)
	}
	if inputData == nil {
		inputData = make(map[string]any)
	}
	// Show the form (either initial load or with validation errors)
	htmlTemplate := `
<!DOCTYPE html>
<html>
<head>
    <title>SMS Sender</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 600px;
            margin: 50px auto;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        .form-container {
            background: rgba(255, 255, 255, 0.1);
            padding: 30px;
            border-radius: 15px;
            backdrop-filter: blur(10px);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        }
        h1 {
            text-align: center;
            margin-bottom: 30px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
            text-shadow: 1px 1px 2px rgba(0,0,0,0.3);
        }
        input, textarea {
            width: 100%;
            padding: 12px;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            background: rgba(255, 255, 255, 0.2);
            color: white;
            backdrop-filter: blur(5px);
        }
        input::placeholder, textarea::placeholder {
            color: rgba(255, 255, 255, 0.7);
        }
        textarea {
            height: 100px;
            resize: vertical;
        }
        .char-count {
            text-align: right;
            font-size: 12px;
            margin-top: 5px;
            opacity: 0.8;
        }
        button {
            background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
            width: 100%;
            transition: transform 0.2s;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        }
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
        }
        .info {
            background: rgba(255, 255, 255, 0.1);
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="form-container">
        <h1>📱 SMS Sender</h1>
        <div class="info">
            <p>Send SMS messages through our secure DAG workflow</p>
        </div>

        {{if validation_error}}
        <div class="error-message" style="background: rgba(255, 100, 100, 0.2); border: 1px solid #ff6b6b; padding: 15px; border-radius: 8px; margin-bottom: 20px; color: #ffcccc;">
            <strong>⚠️ Validation Error:</strong> {{validation_error}}
        </div>
        {{end}}

        <form method="post" action="/process?task_id={{task_id}}&next=true">
            <div class="form-group">
                <label for="phone">📞 Phone Number:</label>
                <input type="tel" id="phone" name="phone"
                       placeholder="+1234567890 or 1234567890"
                       value="{{phone}}"
                       required
                       {{if error_field_phone}}style="border: 2px solid #ff6b6b; background: rgba(255, 100, 100, 0.1);"{{end}}>
                <div class="info" style="margin-top: 5px; font-size: 12px;">
                    Supports US format: +1234567890 or 1234567890
                </div>
            </div>

            <div class="form-group">
                <label for="message">💬 Message:</label>
                <textarea id="message" name="message"
                         placeholder="Enter your message here..."
                         maxlength="160"
                         required
                         oninput="updateCharCount()"
                         {{if error_field_message}}style="border: 2px solid #ff6b6b; background: rgba(255, 100, 100, 0.1);"{{end}}>{{message}}</textarea>
                <div class="char-count" id="charCount">{{message_length}}/160 characters</div>
            </div>

            <div class="form-group">
                <label for="sender_name">👤 Sender Name (Optional):</label>
                <input type="text" id="sender_name" name="sender_name"
                       placeholder="Your name or organization"
                       value="{{sender_name}}"
                       maxlength="50">
            </div>

            <button type="submit">🚀 Send SMS</button>
        </form>
    </div>

    <script>
        function updateCharCount() {
            const messageInput = document.getElementById('message');
            const charCount = document.getElementById('charCount');
            const count = messageInput.value.length;
            charCount.textContent = count + '/160 characters';

            if (count > 140) {
                charCount.style.color = '#FFB6B6';
            } else {
                charCount.style.color = 'rgba(255, 255, 255, 0.8)';
            }
        }

        // Format phone number as user types
        document.getElementById('phone').addEventListener('input', function(e) {
            let value = e.target.value.replace(/\D/g, '');
            if (value.length > 0 && !value.startsWith('1') && value.length === 10) {
                value = '1' + value;
            }
            if (value.length > 11) {
                value = value.substring(0, 11);
            }
            e.target.value = value;
        });
    </script>
</body>
</html>`

	messageStr, _ := inputData["message"].(string)
	messageLength := len(messageStr)

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(htmlTemplate, map[string]any{
		"task_id":             ctx.Value("task_id"),
		"validation_error":    inputData["validation_error"],
		"error_field":         inputData["error_field"],
		"error_field_phone":   inputData["error_field"] == "phone",
		"error_field_message": inputData["error_field"] == "message",
		"phone":               inputData["phone"],
		"message":             inputData["message"],
		"message_length":      messageLength,
		"sender_name":         inputData["sender_name"],
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

// ValidateInputNode - Validates phone number and message
type ValidateInputNode struct {
	dag.Operation
}

func (v *ValidateInputNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{
			Error: fmt.Errorf("invalid input data: %v", err),
			Ctx:   ctx,
		}
	}

	// Extract form data
	phone, _ := inputData["phone"].(string)
	message, _ := inputData["message"].(string)
	senderName, _ := inputData["sender_name"].(string)

	// Validate phone number
	if phone == "" {
		inputData["validation_error"] = "Phone number is required"
		inputData["error_field"] = "phone"
		bt, _ := json.Marshal(inputData)
		return mq.Result{
			Payload: bt,
			Ctx:     ctx,
			ResetTo: "back", // Reset to form instead of going to error page
		}
	}

	// Clean and validate phone number format
	cleanPhone := regexp.MustCompile(`\D`).ReplaceAllString(phone, "")

	// Check for valid US phone number (10 or 11 digits)
	if len(cleanPhone) == 10 {
		cleanPhone = "1" + cleanPhone // Add country code
	} else if len(cleanPhone) != 11 || !strings.HasPrefix(cleanPhone, "1") {
		inputData["validation_error"] = "Invalid phone number format. Please use US format: +1234567890 or 1234567890"
		inputData["error_field"] = "phone"
		bt, _ := json.Marshal(inputData)
		return mq.Result{
			Payload: bt,
			Ctx:     ctx,
			ResetTo: "SMSForm", // Reset to form instead of going to error page
		}
	}

	// Validate message
	if message == "" {
		inputData["validation_error"] = "Message is required"
		inputData["error_field"] = "message"
		bt, _ := json.Marshal(inputData)
		return mq.Result{
			Payload: bt,
			Ctx:     ctx,
			ResetTo: "SMSForm", // Reset to form instead of going to error page
		}
	}

	if len(message) > 160 {
		inputData["validation_error"] = "Message too long. Maximum 160 characters allowed"
		inputData["error_field"] = "message"
		bt, _ := json.Marshal(inputData)
		return mq.Result{
			Payload: bt,
			Ctx:     ctx,
			ResetTo: "SMSForm", // Reset to form instead of going to error page
		}
	}

	// Check for potentially harmful content
	forbiddenWords := []string{"spam", "scam", "fraud", "hack"}
	messageLower := strings.ToLower(message)
	for _, word := range forbiddenWords {
		if strings.Contains(messageLower, word) {
			inputData["validation_error"] = "Message contains prohibited content"
			inputData["error_field"] = "message"
			bt, _ := json.Marshal(inputData)
			return mq.Result{
				Payload: bt,
				Ctx:     ctx,
				ResetTo: "SMSForm", // Reset to form instead of going to error page
			}
		}
	}

	// All validations passed
	validatedData := map[string]any{
		"phone":             cleanPhone,
		"message":           message,
		"sender_name":       senderName,
		"validated_at":      time.Now().Format("2006-01-02 15:04:05"),
		"validation_status": "success",
		"formatted_phone":   formatPhoneForDisplay(cleanPhone),
		"char_count":        len(message),
	}

	bt, _ := json.Marshal(validatedData)
	return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "valid"}
}

// SendSMSNode - Simulates sending SMS
type SendSMSNode struct {
	dag.Operation
}

func (s *SendSMSNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	phone, _ := inputData["phone"].(string)
	message, _ := inputData["message"].(string)
	senderName, _ := inputData["sender_name"].(string)

	// Simulate SMS sending delay
	time.Sleep(500 * time.Millisecond)

	// Simulate occasional failures for demo purposes
	timestamp := time.Now()
	success := timestamp.Second()%10 != 0 // 90% success rate

	if !success {
		errorData := map[string]any{
			"phone":           phone,
			"message":         message,
			"sender_name":     senderName,
			"sms_status":      "failed",
			"error_message":   "SMS gateway temporarily unavailable. Please try again.",
			"sent_at":         timestamp.Format("2006-01-02 15:04:05"),
			"retry_suggested": true,
		}
		bt, _ := json.Marshal(errorData)
		return mq.Result{
			Payload:         bt,
			Ctx:             ctx,
			ConditionStatus: "failed",
		}
	}

	// Generate mock SMS ID and response
	smsID := fmt.Sprintf("SMS_%d_%s", timestamp.Unix(), phone[len(phone)-4:])

	resultData := map[string]any{
		"phone":             phone,
		"formatted_phone":   formatPhoneForDisplay(phone),
		"message":           message,
		"sender_name":       senderName,
		"sms_status":        "sent",
		"sms_id":            smsID,
		"sent_at":           timestamp.Format("2006-01-02 15:04:05"),
		"delivery_estimate": "1-2 minutes",
		"cost_estimate":     "$0.02",
		"gateway":           "MockSMS Gateway",
		"char_count":        len(message),
	}

	fmt.Printf("📱 SMS sent successfully! ID: %s, Phone: %s\n", smsID, formatPhoneForDisplay(phone))

	bt, _ := json.Marshal(resultData)
	return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "sent"}
}

// SMSResultNode - Shows successful SMS result
type SMSResultNode struct {
	dag.Operation
}

func (r *SMSResultNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &inputData); err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
	} else {
		inputData = make(map[string]any)
	}

	htmlTemplate := `
<!DOCTYPE html>
<html>
<head>
    <title>SMS Sent Successfully</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 600px;
            margin: 50px auto;
            padding: 20px;
            background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);
            color: white;
        }
        .result-container {
            background: rgba(255, 255, 255, 0.1);
            padding: 30px;
            border-radius: 15px;
            backdrop-filter: blur(10px);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            text-align: center;
        }
        .success-icon {
            font-size: 60px;
            margin-bottom: 20px;
        }
        h1 {
            margin-bottom: 30px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .info-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
            margin: 20px 0;
            text-align: left;
        }
        .info-item {
            background: rgba(255, 255, 255, 0.1);
            padding: 15px;
            border-radius: 8px;
        }
        .info-label {
            font-weight: bold;
            margin-bottom: 5px;
            opacity: 0.8;
        }
        .info-value {
            font-size: 16px;
        }
        .message-preview {
            background: rgba(255, 255, 255, 0.1);
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            text-align: left;
        }
        .actions {
            margin-top: 30px;
        }
        .btn {
            background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
            color: white;
            padding: 12px 25px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 14px;
            font-weight: bold;
            margin: 0 10px;
            text-decoration: none;
            display: inline-block;
            transition: transform 0.2s;
        }
        .btn:hover {
            transform: translateY(-2px);
        }
        .status-badge {
            background: #4CAF50;
            color: white;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
        }
    </style>
</head>
<body>
    <div class="result-container">
        <div class="success-icon">✅</div>
        <h1>SMS Sent Successfully!</h1>

        <div class="status-badge">{{sms_status}}</div>

        <div class="info-grid">
            <div class="info-item">
                <div class="info-label">📱 Phone Number</div>
                <div class="info-value">{{formatted_phone}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">🆔 SMS ID</div>
                <div class="info-value">{{sms_id}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">⏰ Sent At</div>
                <div class="info-value">{{sent_at}}</div>
            </div>
            <div class="info-item">
                <div class="info-label">🚚 Delivery</div>
                <div class="info-value">{{delivery_estimate}}</div>
            </div>
            {{if sender_name}}
            <div class="info-item">
                <div class="info-label">👤 Sender</div>
                <div class="info-value">{{sender_name}}</div>
            </div>
            {{end}}
            <div class="info-item">
                <div class="info-label">💰 Cost</div>
                <div class="info-value">{{cost_estimate}}</div>
            </div>
        </div>

        <div class="message-preview">
            <div class="info-label">💬 Message Sent ({{char_count}} chars):</div>
            <div class="info-value" style="margin-top: 10px; font-style: italic;">
                "{{message}}"
            </div>
        </div>

        <div class="actions">
            <a href="/" class="btn">📱 Send Another SMS</a>
            <a href="/api/metrics" class="btn">📊 View Metrics</a>
        </div>

        <div style="margin-top: 20px; font-size: 12px; opacity: 0.7;">
            Gateway: {{gateway}} | Task completed in DAG workflow
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

// ErrorPageNode - Shows validation or sending errors
type ErrorPageNode struct {
	dag.Operation
}

func (e *ErrorPageNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var inputData map[string]any
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	// Determine error type and message
	errorMessage, _ := inputData["validation_error"].(string)
	errorField, _ := inputData["error_field"].(string)
	smsError, _ := inputData["error_message"].(string)

	if errorMessage == "" && smsError != "" {
		errorMessage = smsError
		errorField = "sms_sending"
	}
	if errorMessage == "" {
		errorMessage = "An unknown error occurred"
	}

	htmlTemplate := `
<!DOCTYPE html>
<html>
<head>
    <title>SMS Error</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 600px;
            margin: 50px auto;
            padding: 20px;
            background: linear-gradient(135deg, #FF6B6B 0%, #FF5722 100%);
            color: white;
        }
        .error-container {
            background: rgba(255, 255, 255, 0.1);
            padding: 30px;
            border-radius: 15px;
            backdrop-filter: blur(10px);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            text-align: center;
        }
        .error-icon {
            font-size: 60px;
            margin-bottom: 20px;
        }
        h1 {
            margin-bottom: 30px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .error-message {
            background: rgba(255, 255, 255, 0.2);
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            font-size: 16px;
            border-left: 4px solid #FFB6B6;
        }
        .error-details {
            background: rgba(255, 255, 255, 0.1);
            padding: 15px;
            border-radius: 8px;
            margin: 20px 0;
            text-align: left;
        }
        .actions {
            margin-top: 30px;
        }
        .btn {
            background: linear-gradient(45deg, #4ECDC4, #44A08D);
            color: white;
            padding: 12px 25px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 14px;
            font-weight: bold;
            margin: 0 10px;
            text-decoration: none;
            display: inline-block;
            transition: transform 0.2s;
        }
        .btn:hover {
            transform: translateY(-2px);
        }
        .retry-btn {
            background: linear-gradient(45deg, #FFA726, #FF9800);
        }
    </style>
</head>
<body>
    <div class="error-container">
        <div class="error-icon">❌</div>
        <h1>SMS Error</h1>

        <div class="error-message">
            {{error_message}}
        </div>

        {{if error_field}}
        <div class="error-details">
            <strong>Error Field:</strong> {{error_field}}<br>
            <strong>Action Required:</strong> Please correct the highlighted field and try again.
        </div>
        {{end}}

        {{if retry_suggested}}
        <div class="error-details">
            <strong>⚠️ Temporary Issue:</strong> This appears to be a temporary gateway issue.
            Please try sending your SMS again in a few moments.
        </div>
        {{end}}

        <div class="actions">
            <a href="/" class="btn retry-btn">🔄 Try Again</a>
            <a href="/api/status" class="btn">📊 Check Status</a>
        </div>

        <div style="margin-top: 20px; font-size: 12px; opacity: 0.7;">
            DAG Error Handler | SMS Workflow Failed
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

// Helper function to format phone number for display
func formatPhoneForDisplay(phone string) string {
	if len(phone) == 11 && strings.HasPrefix(phone, "1") {
		// Format as +1 (XXX) XXX-XXXX
		return fmt.Sprintf("+1 (%s) %s-%s",
			phone[1:4],
			phone[4:7],
			phone[7:11])
	}
	return phone
}
