package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq"
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

// phoneProcessingSubDAG creates a sub-DAG for processing phone numbers
func phoneProcessingSubDAG() *dag.DAG {
	phone := dag.NewDAG("Phone Processing Sub DAG", "phone-processing-sub-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Phone Processing Sub DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))

	phone.
		AddNode(dag.Function, "Parse Phone Numbers", "parse-phones", &ParsePhoneNumbers{}).
		AddNode(dag.Function, "Phone Loop", "phone-loop", &PhoneLoop{}).
		AddNode(dag.Function, "Validate Phone", "validate-phone", &ValidatePhone{}).
		AddNode(dag.Function, "Send Welcome SMS", "send-welcome", &SendWelcomeSMS{}).
		AddNode(dag.Function, "Collect Valid Phones", "collect-valid", &CollectValidPhones{}).
		AddNode(dag.Function, "Collect Invalid Phones", "collect-invalid", &CollectInvalidPhones{}).
		AddEdge(dag.Simple, "Parse to Loop", "parse-phones", "phone-loop").
		AddEdge(dag.Iterator, "Loop over phones", "phone-loop", "validate-phone").
		AddCondition("validate-phone", map[string]string{"valid": "send-welcome", "invalid": "collect-invalid"}).
		AddEdge(dag.Simple, "Welcome to Collect", "send-welcome", "collect-valid").
		AddEdge(dag.Simple, "Invalid to Collect", "collect-invalid", "collect-valid").
		AddEdge(dag.Simple, "Loop to Collect", "phone-loop", "collect-valid")

	return phone
}

func main() {
	flow := dag.NewDAG("Complex Phone Processing DAG with Pages", "complex-phone-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Complex DAG Final result for task %s: %s\n", taskID, string(result.Payload))
	})
	flow.ConfigureMemoryStorage()

	// Main nodes
	flow.AddNode(dag.Function, "Initialize", "init", &Initialize{}, true)
	flow.AddDAGNode(dag.Function, "Login Process", "login", loginSubDAG())
	flow.AddNode(dag.Function, "Upload Phone Data", "upload-page", &UploadPhoneDataPage{})
	flow.AddDAGNode(dag.Function, "Process Phones", "process-phones", phoneProcessingSubDAG())
	flow.AddNode(dag.Function, "Generate Report", "generate-report", &GenerateReport{})
	flow.AddNode(dag.Function, "Send Summary Email", "send-summary", &SendSummaryEmail{})
	flow.AddNode(dag.Function, "Final Cleanup", "cleanup", &FinalCleanup{})

	// Edges
	flow.AddEdge(dag.Simple, "Init to Login", "init", "login")
	flow.AddEdge(dag.Simple, "Login to Upload", "login", "upload-page")
	flow.AddEdge(dag.Simple, "Upload to Process", "upload-page", "process-phones")
	flow.AddEdge(dag.Simple, "Process to Report", "process-phones", "generate-report")
	flow.AddEdge(dag.Simple, "Report to Summary", "generate-report", "send-summary")
	flow.AddEdge(dag.Simple, "Summary to Cleanup", "send-summary", "cleanup")

	// Sample data for testing
	data := map[string]interface{}{
		"user_id": "user123",
		"session_data": map[string]interface{}{
			"authenticated": false,
		},
		"phone_data": map[string]interface{}{
			"format":  "csv",
			"content": "name,phone\nJohn Doe,+1234567890\nJane Smith,+1987654321\nBob Johnson,invalid-phone\nAlice Brown,+1555123456",
		},
	}

	jsonData, _ := json.Marshal(data)
	if flow.Error != nil {
		panic(flow.Error)
	}

	rs := flow.Process(context.Background(), jsonData)
	if rs.Error != nil {
		panic(rs.Error)
	}
	fmt.Println("Complex Phone DAG Status:", rs.Status, "Topic:", rs.Topic)
	fmt.Println("Final Payload:", string(rs.Payload))
}

// Task implementations

type Initialize struct {
	dag.Operation
}

func (p *Initialize) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("Initialize Error: %s", err.Error()), Ctx: ctx}
	}
	data["initialized"] = true
	data["timestamp"] = "2025-09-19T12:00:00Z"
	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
}

type LoginPage struct {
	dag.Operation
}

func (p *LoginPage) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("LoginPage Error: %s", err.Error()), Ctx: ctx}
	}

	// Simulate user input from page
	data["credentials"] = map[string]interface{}{
		"username": "admin",
		"password": "password123",
	}
	data["login_attempted"] = true

	updatedPayload, _ := json.Marshal(data)
	return mq.Result{
		Payload: updatedPayload,
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
	var data map[string]interface{}
	if err := json.Unmarshal(task.Payload, &data); err != nil {
		return mq.Result{Error: fmt.Errorf("UploadPhoneDataPage Error: %s", err.Error()), Ctx: ctx}
	}

	// Simulate user interaction - in a real scenario, this would be user input
	// The phone data is already in the payload from initialization
	data["upload_completed"] = true
	data["uploaded_at"] = "2025-09-19T12:05:00Z"
	data["user_interaction"] = map[string]interface{}{
		"confirmed_upload": true,
		"upload_method":    "file_upload",
	}

	updatedPayload, _ := json.Marshal(data)
	return mq.Result{Payload: updatedPayload, Ctx: ctx}
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
