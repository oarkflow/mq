package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/jet"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/dag"
)

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

func main() {
	flow := dag.NewDAG("Complex Phone Processing DAG with Pages", "complex-phone-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Complex DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	})
	flow.ConfigureMemoryStorage()

	// Main nodes - Login process as individual nodes (not sub-DAG) for proper page serving
	flow.AddNode(dag.Page, "Initialize", "init", &Initialize{}, true)
	flow.AddNode(dag.Page, "Login Page", "login-page", &LoginPage{})
	flow.AddNode(dag.Function, "Verify Credentials", "verify-credentials", &VerifyCredentials{})
	flow.AddNode(dag.Function, "Generate Token", "generate-token", &GenerateToken{})
	flow.AddNode(dag.Page, "Upload Phone Data", "upload-page", &UploadPhoneDataPage{})
	flow.AddNode(dag.Function, "Parse Phone Numbers", "parse-phones", &ParsePhoneNumbers{})
	flow.AddNode(dag.Function, "Phone Loop", "phone-loop", &PhoneLoop{})
	flow.AddNode(dag.Function, "Validate Phone", "validate-phone", &ValidatePhone{})
	flow.AddNode(dag.Function, "Send Welcome SMS", "send-welcome", &SendWelcomeSMS{})
	flow.AddNode(dag.Function, "Collect Valid Phones", "collect-valid", &CollectValidPhones{})
	flow.AddNode(dag.Function, "Collect Invalid Phones", "collect-invalid", &CollectInvalidPhones{})
	flow.AddNode(dag.Function, "Generate Report", "generate-report", &GenerateReport{})
	flow.AddNode(dag.Function, "Send Summary Email", "send-summary", &SendSummaryEmail{})
	flow.AddNode(dag.Function, "Final Cleanup", "cleanup", &FinalCleanup{})

	// Edges - Connect login flow individually
	flow.AddEdge(dag.Simple, "Init to Login", "init", "login-page")
	flow.AddEdge(dag.Simple, "Login to Verify", "login-page", "verify-credentials")
	flow.AddEdge(dag.Simple, "Verify to Token", "verify-credentials", "generate-token")
	flow.AddEdge(dag.Simple, "Token to Upload", "generate-token", "upload-page")
	flow.AddEdge(dag.Simple, "Upload to Parse", "upload-page", "parse-phones")
	flow.AddEdge(dag.Simple, "Parse to Loop", "parse-phones", "phone-loop")
	flow.AddEdge(dag.Iterator, "Loop over phones", "phone-loop", "validate-phone")
	flow.AddCondition("validate-phone", map[string]string{"valid": "send-welcome", "invalid": "collect-invalid"})
	flow.AddEdge(dag.Simple, "Welcome to Collect", "send-welcome", "collect-valid")
	flow.AddEdge(dag.Simple, "Invalid to Collect", "collect-invalid", "collect-valid")
	flow.AddEdge(dag.Simple, "Loop to Report", "phone-loop", "generate-report")
	flow.AddEdge(dag.Simple, "Report to Summary", "generate-report", "send-summary")
	flow.AddEdge(dag.Simple, "Summary to Cleanup", "send-summary", "cleanup")

	// Check for DAG errors
	// if flow.Error != nil {
	// 	fmt.Printf("DAG Error: %v\n", flow.Error)
	// 	panic(flow.Error)
	// }

	fmt.Println("Starting Complex Phone Processing DAG server on http://0.0.0.0:8080")
	fmt.Println("Navigate to the URL to access the login page")
	flow.Start(context.Background(), ":8080")
}

// Task implementations

type Initialize struct {
	dag.Operation
}

func (p *Initialize) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		data = make(map[string]interface{})
	}
	data["initialized"] = true
	data["timestamp"] = "2025-09-19T12:00:00Z"

	// Add sample phone data for testing
	sampleCSV := `name,phone
John Doe,+1234567890
Jane Smith,0987654321
Bob Johnson,1555123456
Alice Brown,invalid-phone
Charlie Wilson,+441234567890`

	data["phone_data"] = map[string]interface{}{
		"content":    sampleCSV,
		"format":     "csv",
		"source":     "sample_data",
		"created_at": "2025-09-19T12:00:00Z",
	}

	// Generate a task ID for this workflow instance
	taskID := "workflow-" + fmt.Sprintf("%d", time.Now().Unix())

	// Since this is a page node, show a welcome page that auto-redirects to login
	htmlContent := `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="3;url=/process">
    <title>Phone Processing System</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            text-align: center;
            padding: 50px;
            margin: 0;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .welcome {
            background: rgba(255, 255, 255, 0.1);
            padding: 40px;
            border-radius: 15px;
            backdrop-filter: blur(10px);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            max-width: 500px;
            width: 100%;
        }
        .welcome h1 {
            margin-bottom: 20px;
            font-size: 2.5em;
        }
        .welcome p {
            margin-bottom: 30px;
            font-size: 1.1em;
            opacity: 0.9;
        }
        .features {
            margin-top: 30px;
            text-align: left;
            opacity: 0.8;
        }
        .features h3 {
            margin-bottom: 15px;
            color: #fff;
        }
        .features ul {
            list-style: none;
            padding: 0;
        }
        .features li {
            margin-bottom: 8px;
            padding-left: 20px;
            position: relative;
        }
        .features li:before {
            content: "✓";
            position: absolute;
            left: 0;
            color: #4CAF50;
        }
        .countdown {
            margin-top: 20px;
            font-size: 1.2em;
            opacity: 0.9;
        }
    </style>
</head>
<body>
    <div class="welcome">
        <h1>📱 Phone Processing System</h1>
        <p>Welcome to our advanced phone number processing workflow</p>

        <div class="features">
            <h3>Features:</h3>
            <ul>
                <li>CSV/JSON file upload support</li>
                <li>Phone number validation and formatting</li>
                <li>Automated welcome SMS sending</li>
                <li>Invalid number filtering</li>
                <li>Comprehensive processing reports</li>
            </ul>
        </div>

        <div class="countdown">
            <p>Initializing workflow...</p>
            <p>Task ID: ` + taskID + `</p>
            <p>Redirecting to login page in <span id="countdown">3</span> seconds...</p>
        </div>
    </div>

    <script>
        let countdown = 3;
        const countdownElement = document.getElementById('countdown');
        const interval = setInterval(() => {
            countdown--;
            countdownElement.textContent = countdown;
            if (countdown <= 0) {
                clearInterval(interval);
            }
        }, 1000);
    </script>
</body>
</html>`

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(htmlContent, map[string]any{})
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	resultData := map[string]any{
		"html_content": rs,
		"step":         "initialize",
		"data":         data,
	}

	resultPayload, _ := json.Marshal(resultData)
	return mq.Result{Payload: resultPayload, Ctx: ctx}
}

type LoginPage struct {
	dag.Operation
}

func (p *LoginPage) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Check if this is a form submission
	var inputData map[string]interface{}
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &inputData); err == nil {
			// Check if we have form data (username/password)
			if formData, ok := inputData["form"].(map[string]interface{}); ok {
				// This is a form submission, pass it through for verification
				credentials := map[string]interface{}{
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
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		data = make(map[string]interface{})
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
        <form method="post" action="/process?task_id={{task_id}}" id="loginForm">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" required placeholder="Enter your username">
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
	rs, err := parser.ParseTemplate(htmlContent, map[string]any{})
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
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("VerifyCredentials Error: %s", err.Error()), Ctx: ctx}
	}

	credentials, ok := data["credentials"].(map[string]interface{})
	if !ok {
		return mq.Result{Error: fmt.Errorf("credentials not found"), Ctx: ctx}
	}

	username, _ := credentials["username"].(string)
	password, _ := credentials["password"].(string)

	// Simple verification logic
	if username == "admin" && password == "password123" {
		data["authenticated"] = true
		data["user_role"] = "administrator"
	} else {
		data["authenticated"] = false
		data["error"] = "Invalid credentials"
	}

	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type GenerateToken struct {
	dag.Operation
}

func (p *GenerateToken) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("GenerateToken Error: %s", err.Error()), Ctx: ctx}
	}

	if authenticated, ok := data["authenticated"].(bool); ok && authenticated {
		data["auth_token"] = "jwt_token_123456789"
		data["token_expires"] = "2025-09-19T13:00:00Z"
	}

	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type UploadPhoneDataPage struct {
	dag.Operation
}

func (p *UploadPhoneDataPage) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// Check if this is a form submission
	var inputData map[string]interface{}
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &inputData); err == nil {
			// Check if we have form data (phone_data)
			if formData, ok := inputData["form"].(map[string]interface{}); ok {
				// This is a form submission, pass it through for processing
				if phoneData, exists := formData["phone_data"]; exists && phoneData != "" {
					inputData["phone_data"] = map[string]interface{}{
						"content":    phoneData.(string),
						"format":     "csv",
						"source":     "user_input",
						"created_at": "2025-09-19T12:00:00Z",
					}
				}
				updatedPayload, _ := json.Marshal(inputData)
				return mq.Result{Payload: updatedPayload, Ctx: ctx}
			}
		}
	}

	// Otherwise, show the form
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		data = make(map[string]interface{})
	}

	// HTML content for upload page
	htmlContent := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Phone Processing System - Upload Data</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
            margin: 0;
            padding: 0;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .upload-container {
            background: white;
            padding: 2rem;
            border-radius: 10px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.2);
            width: 100%;
            max-width: 500px;
        }
        .upload-header {
            text-align: center;
            margin-bottom: 2rem;
        }
        .upload-header h1 {
            color: #333;
            margin: 0;
            font-size: 1.8rem;
        }
        .upload-header p {
            color: #666;
            margin: 0.5rem 0 0 0;
        }
        .upload-area {
            border: 2px dashed #667eea;
            border-radius: 8px;
            padding: 2rem;
            text-align: center;
            margin-bottom: 1.5rem;
            transition: border-color 0.3s;
            cursor: pointer;
        }
        .upload-area:hover {
            border-color: #764ba2;
        }
        .upload-area.dragover {
            border-color: #28a745;
            background: #f8fff9;
        }
        .upload-icon {
            font-size: 3rem;
            color: #667eea;
            margin-bottom: 1rem;
        }
        .upload-text {
            color: #666;
            margin-bottom: 0.5rem;
        }
        .file-info {
            background: #f8f9fa;
            padding: 1rem;
            border-radius: 5px;
            margin-bottom: 1rem;
            display: none;
        }
        .file-info.show {
            display: block;
        }
        .file-name {
            font-weight: bold;
            color: #333;
        }
        .file-size {
            color: #666;
            font-size: 0.9rem;
        }
        .upload-btn {
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
        .upload-btn:hover {
            transform: translateY(-2px);
        }
        .upload-btn:active {
            transform: scale(0.98);
        }
        .upload-btn:disabled {
            background: #ccc;
            cursor: not-allowed;
            transform: none;
        }
        .progress-bar {
            width: 100%;
            height: 8px;
            background: #e9ecef;
            border-radius: 4px;
            margin-top: 1rem;
            overflow: hidden;
            display: none;
        }
        .progress-bar.show {
            display: block;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            width: 0%;
            transition: width 0.3s ease;
        }
        .status-message {
            margin-top: 1rem;
            padding: 0.5rem;
            border-radius: 5px;
            text-align: center;
            font-weight: 500;
            display: none;
        }
        .status-message.show {
            display: block;
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
    <div class="upload-container">
        <div class="upload-header">
            <h1>📤 Upload Phone Data</h1>
            <p>Upload your CSV file containing phone numbers for processing</p>
        </div>

        <form method="post" action="/process" id="uploadForm" enctype="multipart/form-data">
            <div class="upload-area" id="uploadArea">
                <div class="upload-icon">📁</div>
                <div class="upload-text">Drag & drop your CSV file here or click to browse</div>
                <div style="color: #999; font-size: 0.9rem; margin-top: 0.5rem;">Supported format: CSV with name,phone columns</div>
                <input type="file" id="fileInput" name="file" accept=".csv,.json" style="display: none;">
            </div>

            <div style="margin: 20px 0; text-align: center; color: #666;">OR</div>

            <div class="form-group">
                <label for="phoneData" style="color: #333; font-weight: bold;">Paste CSV/JSON Data:</label>
                <textarea id="phoneData" name="phone_data" rows="8" placeholder="name,phone&#10;John Doe,+1234567890&#10;Jane Smith,0987654321&#10;Or paste JSON array..." style="width: 100%; padding: 10px; border: 2px solid #e1e5e9; border-radius: 5px; font-family: monospace; resize: vertical;">name,phone
John Doe,+1234567890
Jane Smith,0987654321
Bob Johnson,1555123456
Alice Brown,invalid-phone
Charlie Wilson,+441234567890</textarea>
            </div>

            <button type="submit" class="upload-btn" id="uploadBtn">Upload & Process</button>        <div class="progress-bar" id="progressBar">
            <div class="progress-fill" id="progressFill"></div>
        </div>

        <div class="status-message" id="statusMessage"></div>
    </div>

    <script>
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('fileInput');
        const phoneDataTextarea = document.getElementById('phoneData');
        const uploadBtn = document.getElementById('uploadBtn');
        const uploadForm = document.getElementById('uploadForm');

        // Upload area click handler
        uploadArea.addEventListener('click', () => {
            fileInput.click();
        });

        // File input change handler
        fileInput.addEventListener('change', (e) => {
            const file = e.target.files[0];
            if (file) {
                // Clear textarea if file is selected
                phoneDataTextarea.value = '';
                phoneDataTextarea.disabled = true;
            } else {
                phoneDataTextarea.disabled = false;
            }
        });

        // Textarea input handler
        phoneDataTextarea.addEventListener('input', () => {
            if (phoneDataTextarea.value.trim()) {
                // Clear file input if textarea has content
                fileInput.value = '';
            }
        });

        // Form submission handler
        uploadForm.addEventListener('submit', (e) => {
            uploadBtn.textContent = 'Processing...';
            uploadBtn.disabled = true;
        });

        // Drag and drop handlers
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.classList.add('dragover');
        });

        uploadArea.addEventListener('dragleave', () => {
            uploadArea.classList.remove('dragover');
        });

        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadArea.classList.remove('dragover');
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                fileInput.files = files;
                fileInput.dispatchEvent(new Event('change'));
            }
        });
    </script>
</body>
</html>`

	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	rs, err := parser.ParseTemplate(htmlContent, map[string]any{})
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}

	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	resultData := map[string]any{
		"html_content": rs,
		"step":         "upload",
		"data":         data,
	}

	resultPayload, _ := json.Marshal(resultData)
	return mq.Result{
		Payload: resultPayload,
		Ctx:     ctx,
	}
}

type ParsePhoneNumbers struct {
	dag.Operation
}

func (p *ParsePhoneNumbers) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("ParsePhoneNumbers Error: %s", err.Error()), Ctx: ctx}
	}

	phoneData, ok := data["phone_data"].(map[string]interface{})
	if !ok {
		return mq.Result{Error: fmt.Errorf("phone_data not found"), Ctx: ctx}
	}

	content, ok := phoneData["content"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("phone data content not found"), Ctx: ctx}
	}

	var phones []map[string]interface{}

	// Parse CSV content
	lines := strings.Split(content, "\n")
	if len(lines) > 1 {
		headers := strings.Split(lines[0], ",")
		for i := 1; i < len(lines); i++ {
			if lines[i] == "" {
				continue
			}
			values := strings.Split(lines[i], ",")
			if len(values) >= len(headers) {
				phone := make(map[string]interface{})
				for j, header := range headers {
					phone[strings.TrimSpace(header)] = strings.TrimSpace(values[j])
				}
				phones = append(phones, phone)
			}
		}
	}

	data["parsed_phones"] = phones
	data["total_phones"] = len(phones)
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type PhoneLoop struct {
	dag.Operation
}

func (p *PhoneLoop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("PhoneLoop Error: %s", err.Error()), Ctx: ctx}
	}

	// Extract parsed phones for iteration
	if phones, ok := data["parsed_phones"].([]interface{}); ok {
		updatedPayload, _ := json.Marshal(phones)
		return mq.Result{Payload: updatedPayload, Ctx: ctx}
	}

	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type ValidatePhone struct {
	dag.Operation
}

func (p *ValidatePhone) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var phone map[string]interface{}
	if err := json.Unmarshal(task.Payload, &phone); err != nil {
		return mq.Result{Error: fmt.Errorf("ValidatePhone Error: %s", err.Error()), Ctx: ctx}
	}

	phoneStr, ok := phone["phone"].(string)
	if !ok {
		return mq.Result{Payload: task.Payload, ConditionStatus: "invalid", Ctx: ctx}
	}

	// Simple phone validation regex (supports international format)
	validPhone := regexp.MustCompile(`^\+?[1-9]\d{1,14}$`)
	if validPhone.MatchString(phoneStr) {
		phone["valid"] = true
		phone["formatted_phone"] = phoneStr
		return mq.Result{Payload: task.Payload, ConditionStatus: "valid", Ctx: ctx}
	}

	phone["valid"] = false
	phone["validation_error"] = "Invalid phone number format"
	return mq.Result{Payload: task.Payload, ConditionStatus: "invalid", Ctx: ctx}
}

type SendWelcomeSMS struct {
	dag.Operation
}

func (p *SendWelcomeSMS) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var phone map[string]interface{}
	if err := json.Unmarshal(task.Payload, &phone); err != nil {
		return mq.Result{Error: fmt.Errorf("SendWelcomeSMS Error: %s", err.Error()), Ctx: ctx}
	}

	phoneStr, ok := phone["phone"].(string)
	if !ok {
		return mq.Result{Error: fmt.Errorf("phone number not found"), Ctx: ctx}
	}

	// Simulate sending welcome SMS
	phone["welcome_sent"] = true
	phone["welcome_message"] = "Welcome! Your phone number has been verified."
	phone["sent_at"] = "2025-09-19T12:10:00Z"

	fmt.Printf("Welcome SMS sent to %s\n", phoneStr)
	updatedPayload, _ := json.Marshal(phone)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type CollectValidPhones struct {
	dag.Operation
}

func (p *CollectValidPhones) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	// This node collects all processed phone results
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type CollectInvalidPhones struct {
	dag.Operation
}

func (p *CollectInvalidPhones) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var phone map[string]interface{}
	if err := json.Unmarshal(task.Payload, &phone); err != nil {
		return mq.Result{Error: fmt.Errorf("CollectInvalidPhones Error: %s", err.Error()), Ctx: ctx}
	}

	phone["discarded"] = true
	phone["discard_reason"] = "Invalid phone number"

	fmt.Printf("Invalid phone discarded: %v\n", phone["phone"])
	updatedPayload, _ := json.Marshal(phone)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type GenerateReport struct {
	dag.Operation
}

func (p *GenerateReport) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		// If it's an array, wrap it in a map
		var arr []interface{}
		if err2 := json.Unmarshal(task.Payload, &arr); err2 != nil {
			return mq.Result{Error: fmt.Errorf("GenerateReport Error: %s", err.Error()), Ctx: ctx}
		}
		data = map[string]interface{}{
			"processed_results": arr,
		}
	}

	// Generate processing report
	validCount := 0
	invalidCount := 0

	if results, ok := data["processed_results"].([]interface{}); ok {
		for _, result := range results {
			if resultMap, ok := result.(map[string]interface{}); ok {
				if _, isValid := resultMap["welcome_sent"]; isValid {
					validCount++
				} else if _, isInvalid := resultMap["discarded"]; isInvalid {
					invalidCount++
				}
			}
		}
	}

	report := map[string]interface{}{
		"total_processed": validCount + invalidCount,
		"valid_phones":    validCount,
		"invalid_phones":  invalidCount,
		"processed_at":    "2025-09-19T12:15:00Z",
		"success":         true,
	}

	data["report"] = report
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type SendSummaryEmail struct {
	dag.Operation
}

func (p *SendSummaryEmail) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("SendSummaryEmail Error: %s", err.Error()), Ctx: ctx}
	}

	// Simulate sending summary email
	data["summary_email_sent"] = true
	data["summary_recipient"] = "admin@company.com"
	data["summary_sent_at"] = "2025-09-19T12:20:00Z"

	fmt.Println("Summary email sent to admin")
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type FinalCleanup struct {
	dag.Operation
}

func (p *FinalCleanup) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("FinalCleanup Error: %s", err.Error()), Ctx: ctx}
	}

	// Perform final cleanup
	data["completed"] = true
	data["completed_at"] = "2025-09-19T12:25:00Z"
	data["workflow_status"] = "success"

	fmt.Println("Workflow completed successfully")
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}
