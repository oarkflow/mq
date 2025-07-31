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
	})

	// Add SMS workflow nodes
	// Note: Page nodes have no timeout by default, allowing users unlimited time for form input
	flow.AddNode(dag.Page, "SMS Form", "SMSForm", &SMSFormNode{})
	flow.AddNode(dag.Function, "Validate Input", "ValidateInput", &ValidateInputNode{})
	flow.AddNode(dag.Function, "Send SMS", "SendSMS", &SendSMSNode{})
	flow.AddNode(dag.Page, "SMS Result", "SMSResult", &SMSResultNode{})
	flow.AddNode(dag.Page, "Error Page", "ErrorPage", &ErrorPageNode{})

	// Define edges for SMS workflow
	flow.AddEdge(dag.Simple, "Form to Validation", "SMSForm", "ValidateInput")
	flow.AddCondition("ValidateInput", map[string]string{"valid": "SendSMS", "invalid": "ErrorPage"})
	flow.AddCondition("SendSMS", map[string]string{"sent": "SMSResult", "failed": "ErrorPage"})

	// Start the flow
	if flow.Error != nil {
		panic(flow.Error)
	}

	fmt.Println("Starting SMS DAG server on http://0.0.0.0:8083")
	fmt.Println("Navigate to the URL to access the SMS form")
	flow.Start(context.Background(), "0.0.0.0:8083")
}

// SMSFormNode - Initial form to collect SMS data
type SMSFormNode struct {
	dag.Operation
}

func (s *SMSFormNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
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
        <form method="post" action="/process?task_id={{task_id}}&next=true">
            <div class="form-group">
                <label for="phone">📞 Phone Number:</label>
                <input type="tel" id="phone" name="phone"
                       placeholder="+1234567890 or 1234567890"
                       required>
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
                         oninput="updateCharCount()"></textarea>
                <div class="char-count" id="charCount">0/160 characters</div>
            </div>

            <div class="form-group">
                <label for="sender_name">👤 Sender Name (Optional):</label>
                <input type="text" id="sender_name" name="sender_name"
                       placeholder="Your name or organization"
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
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
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
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	// Validate message
	if message == "" {
		inputData["validation_error"] = "Message is required"
		inputData["error_field"] = "message"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	if len(message) > 160 {
		inputData["validation_error"] = "Message too long. Maximum 160 characters allowed"
		inputData["error_field"] = "message"
		bt, _ := json.Marshal(inputData)
		return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
	}

	// Check for potentially harmful content
	forbiddenWords := []string{"spam", "scam", "fraud", "hack"}
	messageLower := strings.ToLower(message)
	for _, word := range forbiddenWords {
		if strings.Contains(messageLower, word) {
			inputData["validation_error"] = "Message contains prohibited content"
			inputData["error_field"] = "message"
			bt, _ := json.Marshal(inputData)
			return mq.Result{Payload: bt, Ctx: ctx, ConditionStatus: "invalid"}
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
	if err := json.Unmarshal(task.Payload, &inputData); err != nil {
		return mq.Result{Error: err, Ctx: ctx}
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
