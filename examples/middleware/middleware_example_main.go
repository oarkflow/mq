package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

// User represents a user with roles
type User struct {
	ID    string   `json:"id"`
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

// HasRole checks if user has a specific role
func (u *User) HasRole(role string) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// HasAnyRole checks if user has any of the specified roles
func (u *User) HasAnyRole(roles ...string) bool {
	for _, requiredRole := range roles {
		if u.HasRole(requiredRole) {
			return true
		}
	}
	return false
}

// LoggingMiddleware logs the start and end of task processing
func LoggingMiddleware(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("Middleware: Starting processing for node %s, task %s", task.Topic, task.ID)
	start := time.Now()

	// For middleware, we return a successful result to continue to next middleware/processor
	// The actual processing will happen after all middlewares
	result := mq.Result{
		Status:  mq.Completed,
		Ctx:     ctx,
		Payload: task.Payload, // Pass through the payload
	}

	log.Printf("Middleware: Completed in %v", time.Since(start))
	return result
}

// ValidationMiddleware validates the task payload
func ValidationMiddleware(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("ValidationMiddleware: Validating payload for node %s", task.Topic)

	// Check if payload is empty
	if len(task.Payload) == 0 {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("empty payload not allowed"),
			Ctx:    ctx,
		}
	}

	log.Printf("ValidationMiddleware: Payload validation passed")
	return mq.Result{
		Status:  mq.Completed,
		Ctx:     ctx,
		Payload: task.Payload,
	}
}

// RoleCheckMiddleware checks if the user has required roles for accessing a sub-DAG
func RoleCheckMiddleware(requiredRoles ...string) mq.Handler {
	return func(ctx context.Context, task *mq.Task) mq.Result {
		log.Printf("RoleCheckMiddleware: Checking roles %v for node %s", requiredRoles, task.Topic)

		// Extract user from payload
		var payload map[string]any
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			return mq.Result{
				Status: mq.Failed,
				Error:  fmt.Errorf("invalid payload format: %v", err),
				Ctx:    ctx,
			}
		}

		userData, exists := payload["user"]
		if !exists {
			return mq.Result{
				Status: mq.Failed,
				Error:  fmt.Errorf("user information not found in payload"),
				Ctx:    ctx,
			}
		}

		userBytes, err := json.Marshal(userData)
		if err != nil {
			return mq.Result{
				Status: mq.Failed,
				Error:  fmt.Errorf("invalid user data: %v", err),
				Ctx:    ctx,
			}
		}

		var user User
		if err := json.Unmarshal(userBytes, &user); err != nil {
			return mq.Result{
				Status: mq.Failed,
				Error:  fmt.Errorf("invalid user format: %v", err),
				Ctx:    ctx,
			}
		}

		if !user.HasAnyRole(requiredRoles...) {
			return mq.Result{
				Status: mq.Failed,
				Error:  fmt.Errorf("user %s does not have required roles %v. User roles: %v", user.Name, requiredRoles, user.Roles),
				Ctx:    ctx,
			}
		}

		log.Printf("RoleCheckMiddleware: User %s authorized for roles %v", user.Name, requiredRoles)
		return mq.Result{
			Status:  mq.Completed,
			Ctx:     ctx,
			Payload: task.Payload,
		}
	}
}

// TimingMiddleware measures execution time
func TimingMiddleware(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("TimingMiddleware: Starting timing for node %s", task.Topic)

	// Add timing info to context
	ctx = context.WithValue(ctx, "start_time", time.Now())

	return mq.Result{
		Status:  mq.Completed,
		Ctx:     ctx,
		Payload: task.Payload,
	}
}

// Example processor that simulates some work
type ExampleProcessor struct {
	dag.Operation
}

func (p *ExampleProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("Processor: Processing task %s on node %s", task.ID, task.Topic)

	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)

	// Parse the payload as JSON
	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("invalid payload: %v", err),
			Ctx:    ctx,
		}
	}

	// Add processing information
	payload["processed_by"] = "ExampleProcessor"
	payload["processing_time"] = time.Now().Format(time.RFC3339)

	resultPayload, err := json.Marshal(payload)
	if err != nil {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to marshal result: %v", err),
			Ctx:    ctx,
		}
	}

	return mq.Result{
		Status:  mq.Completed,
		Payload: resultPayload,
		Ctx:     ctx,
	}
}

// AdminProcessor handles admin-specific tasks
type AdminProcessor struct {
	dag.Operation
}

func (p *AdminProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("AdminProcessor: Processing admin task %s on node %s", task.ID, task.Topic)

	// Simulate admin processing
	time.Sleep(200 * time.Millisecond)

	// Parse the payload as JSON
	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("invalid payload: %v", err),
			Ctx:    ctx,
		}
	}

	// Add admin-specific processing information
	payload["processed_by"] = "AdminProcessor"
	payload["admin_action"] = "validated_and_processed"
	payload["processing_time"] = time.Now().Format(time.RFC3339)

	resultPayload, err := json.Marshal(payload)
	if err != nil {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to marshal result: %v", err),
			Ctx:    ctx,
		}
	}

	return mq.Result{
		Status:  mq.Completed,
		Payload: resultPayload,
		Ctx:     ctx,
	}
}

// UserProcessor handles user-specific tasks
type UserProcessor struct {
	dag.Operation
}

func (p *UserProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("UserProcessor: Processing user task %s on node %s", task.ID, task.Topic)

	// Simulate user processing
	time.Sleep(150 * time.Millisecond)

	// Parse the payload as JSON
	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("invalid payload: %v", err),
			Ctx:    ctx,
		}
	}

	// Add user-specific processing information
	payload["processed_by"] = "UserProcessor"
	payload["user_action"] = "authenticated_and_processed"
	payload["processing_time"] = time.Now().Format(time.RFC3339)

	resultPayload, err := json.Marshal(payload)
	if err != nil {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to marshal result: %v", err),
			Ctx:    ctx,
		}
	}

	return mq.Result{
		Status:  mq.Completed,
		Payload: resultPayload,
		Ctx:     ctx,
	}
}

// GuestProcessor handles guest-specific tasks
type GuestProcessor struct {
	dag.Operation
}

func (p *GuestProcessor) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	log.Printf("GuestProcessor: Processing guest task %s on node %s", task.ID, task.Topic)

	// Simulate guest processing
	time.Sleep(100 * time.Millisecond)

	// Parse the payload as JSON
	var payload map[string]any
	if err := json.Unmarshal(task.Payload, &payload); err != nil {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("invalid payload: %v", err),
			Ctx:    ctx,
		}
	}

	// Add guest-specific processing information
	payload["processed_by"] = "GuestProcessor"
	payload["guest_action"] = "limited_access_processed"
	payload["processing_time"] = time.Now().Format(time.RFC3339)

	resultPayload, err := json.Marshal(payload)
	if err != nil {
		return mq.Result{
			Status: mq.Failed,
			Error:  fmt.Errorf("failed to marshal result: %v", err),
			Ctx:    ctx,
		}
	}

	return mq.Result{
		Status:  mq.Completed,
		Payload: resultPayload,
		Ctx:     ctx,
	}
}

// createAdminSubDAG creates a sub-DAG for admin operations
func createAdminSubDAG() *dag.DAG {
	adminDAG := dag.NewDAG("Admin Sub-DAG", "admin-subdag", func(taskID string, result mq.Result) {
		log.Printf("Admin Sub-DAG completed for task %s: %s", taskID, string(result.Payload))
	})

	adminDAG.AddNode(dag.Function, "Admin Validate", "admin_validate", &AdminProcessor{Operation: dag.Operation{Type: dag.Function}}, true)
	adminDAG.AddNode(dag.Function, "Admin Process", "admin_process", &AdminProcessor{Operation: dag.Operation{Type: dag.Function}})
	adminDAG.AddNode(dag.Function, "Admin Finalize", "admin_finalize", &AdminProcessor{Operation: dag.Operation{Type: dag.Function}})

	adminDAG.AddEdge(dag.Simple, "Validate to Process", "admin_validate", "admin_process")
	adminDAG.AddEdge(dag.Simple, "Process to Finalize", "admin_process", "admin_finalize")

	return adminDAG
}

// createUserSubDAG creates a sub-DAG for user operations
func createUserSubDAG() *dag.DAG {
	userDAG := dag.NewDAG("User Sub-DAG", "user-subdag", func(taskID string, result mq.Result) {
		log.Printf("User Sub-DAG completed for task %s: %s", taskID, string(result.Payload))
	})

	userDAG.AddNode(dag.Function, "User Auth", "user_auth", &UserProcessor{Operation: dag.Operation{Type: dag.Function}}, true)
	userDAG.AddNode(dag.Function, "User Process", "user_process", &UserProcessor{Operation: dag.Operation{Type: dag.Function}})
	userDAG.AddNode(dag.Function, "User Notify", "user_notify", &UserProcessor{Operation: dag.Operation{Type: dag.Function}})

	userDAG.AddEdge(dag.Simple, "Auth to Process", "user_auth", "user_process")
	userDAG.AddEdge(dag.Simple, "Process to Notify", "user_process", "user_notify")

	return userDAG
}

// createGuestSubDAG creates a sub-DAG for guest operations
func createGuestSubDAG() *dag.DAG {
	guestDAG := dag.NewDAG("Guest Sub-DAG", "guest-subdag", func(taskID string, result mq.Result) {
		log.Printf("Guest Sub-DAG completed for task %s: %s", taskID, string(result.Payload))
	})

	guestDAG.AddNode(dag.Function, "Guest Welcome", "guest_welcome", &GuestProcessor{Operation: dag.Operation{Type: dag.Function}}, true)
	guestDAG.AddNode(dag.Function, "Guest Info", "guest_info", &GuestProcessor{Operation: dag.Operation{Type: dag.Function}})

	guestDAG.AddEdge(dag.Simple, "Welcome to Info", "guest_welcome", "guest_info")

	return guestDAG
}

func main() {
	// Create the main DAG
	flow := dag.NewDAG("Role-Based Access Control DAG", "rbac-dag", func(taskID string, result mq.Result) {
		log.Printf("Main DAG completed for task %s: %s", taskID, string(result.Payload))
	})

	// Add entry point
	flow.AddNode(dag.Function, "Entry Point", "entry", &ExampleProcessor{Operation: dag.Operation{Type: dag.Function}}, true)

	// Add sub-DAGs with role-based access
	flow.AddDAGNode(dag.Function, "Admin Operations", "admin_ops", createAdminSubDAG())
	flow.AddDAGNode(dag.Function, "User Operations", "user_ops", createUserSubDAG())
	flow.AddDAGNode(dag.Function, "Guest Operations", "guest_ops", createGuestSubDAG())

	// Add edges from entry to sub-DAGs
	flow.AddEdge(dag.Simple, "Entry to Admin", "entry", "admin_ops")
	flow.AddEdge(dag.Simple, "Entry to User", "entry", "user_ops")
	flow.AddEdge(dag.Simple, "Entry to Guest", "entry", "guest_ops")

	// Add global middlewares
	flow.Use(LoggingMiddleware, ValidationMiddleware)

	// Add role-based middlewares for sub-DAGs
	flow.UseNodeMiddlewares(
		dag.NodeMiddleware{
			Node:        "admin_ops",
			Middlewares: []mq.Handler{RoleCheckMiddleware("admin", "superuser")},
		},
		dag.NodeMiddleware{
			Node:        "user_ops",
			Middlewares: []mq.Handler{RoleCheckMiddleware("user", "admin", "superuser")},
		},
		dag.NodeMiddleware{
			Node:        "guest_ops",
			Middlewares: []mq.Handler{RoleCheckMiddleware("guest", "user", "admin", "superuser")},
		},
	)

	if flow.Error != nil {
		panic(flow.Error)
	}

	// Define test users with different roles
	users := []User{
		{ID: "1", Name: "Alice", Roles: []string{"admin", "superuser"}},
		{ID: "2", Name: "Bob", Roles: []string{"user"}},
		{ID: "3", Name: "Charlie", Roles: []string{"guest"}},
		{ID: "4", Name: "Dave", Roles: []string{"user", "admin"}},
		{ID: "5", Name: "Eve", Roles: []string{}}, // No roles
	}

	// Test each user
	for _, user := range users {
		log.Printf("\n=== Testing user: %s (Roles: %v) ===", user.Name, user.Roles)

		// Create payload with user information
		payload := map[string]any{
			"user":    user,
			"message": fmt.Sprintf("Request from %s", user.Name),
			"data":    "test data",
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Failed to marshal payload for user %s: %v", user.Name, err)
			continue
		}

		log.Printf("Processing request for user %s with payload: %s", user.Name, string(payloadBytes))

		result := flow.Process(context.Background(), payloadBytes)
		if result.Error != nil {
			log.Printf("❌ DAG processing failed for user %s: %v", user.Name, result.Error)
		} else {
			log.Printf("✅ DAG processing completed successfully for user %s: %s", user.Name, string(result.Payload))
		}
	}
}
