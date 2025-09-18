package main

import (
	"context"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/oarkflow/mq/workflow"
)

// SMS Demo server with comprehensive workflow pipeline
func main() {
	// Initialize workflow engine
	config := &workflow.Config{
		MaxWorkers:       10,
		ExecutionTimeout: 30 * time.Minute,
		EnableMetrics:    true,
		EnableAudit:      true,
		EnableTracing:    true,
	}

	engine := workflow.NewWorkflowEngine(config)
	ctx := context.Background()
	engine.Start(ctx)
	defer engine.Stop(ctx)

	// Initialize user manager and middleware
	userManager := workflow.NewUserManager()
	middlewareManager := workflow.NewMiddlewareManager()

	// Create demo users
	createDemoUsers(userManager)

	// Setup middleware
	setupMiddleware(middlewareManager)

	// Register SMS workflow pipeline
	registerSMSWorkflows(engine)

	// Start HTTP server
	app := fiber.New(fiber.Config{
		AppName: "Advanced SMS Workflow Engine",
	})

	// Add fiber middleware
	app.Use(cors.New())
	app.Use(logger.New())
	app.Use(recover.New())

	// Setup routes
	setupRoutes(app, engine, userManager, middlewareManager)

	log.Println("🚀 Advanced SMS Workflow Engine started on http://localhost:3000")
	log.Println("📱 SMS Pipeline Demo: http://localhost:3000/sms")
	log.Println("👤 User Auth Demo: http://localhost:3000/auth")
	log.Println("📊 Admin Dashboard: http://localhost:3000/admin")
	log.Println("📝 API Documentation: http://localhost:3000/docs")
	log.Fatal(app.Listen(":3000"))
}

func createDemoUsers(userManager *workflow.UserManager) {
	users := []*workflow.User{
		{
			ID:          "admin",
			Username:    "admin",
			Email:       "admin@company.com",
			Role:        workflow.UserRoleAdmin,
			Permissions: []string{"admin"},
		},
		{
			ID:          "manager",
			Username:    "manager",
			Email:       "manager@company.com",
			Role:        workflow.UserRoleManager,
			Permissions: []string{"read", "write", "execute"},
		},
		{
			ID:          "operator",
			Username:    "operator",
			Email:       "operator@company.com",
			Role:        workflow.UserRoleOperator,
			Permissions: []string{"read", "execute"},
		},
	}

	for _, user := range users {
		if err := userManager.CreateUser(user); err != nil {
			log.Printf("Error creating user %s: %v", user.Username, err)
		}
	}

	log.Println("✅ Demo users created: admin/password, manager/password, operator/password")
}

func setupMiddleware(middlewareManager *workflow.MiddlewareManager) {
	// Add logging middleware
	loggingMiddleware := workflow.Middleware{
		ID:       "logging",
		Name:     "Request Logging",
		Type:     workflow.MiddlewareLogging,
		Priority: 1,
		Enabled:  true,
		Config:   map[string]interface{}{},
	}
	middlewareManager.AddMiddleware(loggingMiddleware)

	// Add rate limiting middleware
	rateLimitMiddleware := workflow.Middleware{
		ID:       "rate_limit",
		Name:     "Rate Limiting",
		Type:     workflow.MiddlewareRateLimit,
		Priority: 2,
		Enabled:  true,
		Config: map[string]interface{}{
			"requests_per_minute": 100,
		},
	}
	middlewareManager.AddMiddleware(rateLimitMiddleware)

	// Add auth middleware
	authMiddleware := workflow.Middleware{
		ID:       "auth",
		Name:     "Authentication",
		Type:     workflow.MiddlewareAuth,
		Priority: 3,
		Enabled:  true,
		Config:   map[string]interface{}{},
	}
	middlewareManager.AddMiddleware(authMiddleware)

	log.Println("✅ Middleware configured: logging, rate limiting, authentication")
}

func registerSMSWorkflows(engine *workflow.WorkflowEngine) {
	ctx := context.Background()

	// 1. User Authentication Sub-DAG
	authWorkflow := createAuthSubDAG()
	if err := engine.RegisterWorkflow(ctx, authWorkflow); err != nil {
		log.Printf("Error registering auth workflow: %v", err)
	}

	// 2. Main SMS Pipeline Workflow
	smsWorkflow := createSMSPipelineWorkflow()
	if err := engine.RegisterWorkflow(ctx, smsWorkflow); err != nil {
		log.Printf("Error registering SMS workflow: %v", err)
	}

	// 3. Webhook Handler Workflow
	webhookWorkflow := createWebhookHandlerWorkflow()
	if err := engine.RegisterWorkflow(ctx, webhookWorkflow); err != nil {
		log.Printf("Error registering webhook workflow: %v", err)
	}

	log.Println("✅ SMS workflow pipeline registered successfully")
}

func createAuthSubDAG() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		ID:          "user-auth-subdag",
		Name:        "User Authentication Sub-DAG",
		Description: "Handles user login and token validation",
		Version:     "1.0.0",
		Status:      workflow.WorkflowStatusActive,
		Nodes: []workflow.WorkflowNode{
			{
				ID:          "validate-credentials",
				Name:        "Validate User Credentials",
				Type:        workflow.NodeTypeAuth,
				Description: "Authenticate user with credentials",
				Config: workflow.NodeConfig{
					AuthType: "login",
					Credentials: map[string]string{
						"admin":    "password",
						"manager":  "password",
						"operator": "password",
					},
					TokenExpiry: 24 * time.Hour,
				},
			},
			{
				ID:          "check-permissions",
				Name:        "Check SMS Permissions",
				Type:        workflow.NodeTypeValidator,
				Description: "Validate user has SMS sending permissions",
				Config: workflow.NodeConfig{
					ValidationType: "strict",
					ValidationRules: []workflow.ValidationRule{
						{
							Field:   "permissions",
							Type:    "required",
							Message: "User permissions required",
						},
						{
							Field:   "role",
							Type:    "required",
							Message: "User role required",
						},
					},
				},
			},
		},
		Edges: []workflow.WorkflowEdge{
			{
				ID:       "auth-to-permissions",
				FromNode: "validate-credentials",
				ToNode:   "check-permissions",
			},
		},
	}
}

func createSMSPipelineWorkflow() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		ID:          "sms-pipeline",
		Name:        "Comprehensive SMS Pipeline",
		Description: "Complete SMS workflow with authentication, validation, routing, and reporting",
		Version:     "1.0.0",
		Status:      workflow.WorkflowStatusActive,
		Nodes: []workflow.WorkflowNode{
			// Step 1: User Authentication (Sub-DAG)
			{
				ID:          "user-authentication",
				Name:        "User Authentication",
				Type:        workflow.NodeTypeSubDAG,
				Description: "Authenticate user and validate permissions",
				Config: workflow.NodeConfig{
					SubWorkflowID: "user-auth-subdag",
					InputMapping: map[string]string{
						"username": "username",
						"password": "password",
					},
					OutputMapping: map[string]string{
						"auth_token": "token",
						"user_info":  "user",
					},
				},
			},
			// Step 2: SMS HTML Page Generation
			{
				ID:          "generate-sms-page",
				Name:        "Generate SMS HTML Page",
				Type:        workflow.NodeTypeHTML,
				Description: "Create dynamic HTML page for SMS composition",
				Config: workflow.NodeConfig{
					Template: `
<!DOCTYPE html>
<html>
<head>
    <title>SMS Composer</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        input, textarea, select { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
        button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
        .info { background: #e3f2fd; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="info">
        <h2>Welcome, {{.user.username}}!</h2>
        <p>Role: {{.user.role}} | Permissions: {{.user.permissions}}</p>
    </div>
    <h1>SMS Composer</h1>
    <form id="sms-form">
        <div class="form-group">
            <label>Recipients (comma-separated phone numbers):</label>
            <input type="text" name="recipients" placeholder="+1234567890, +0987654321" required>
        </div>
        <div class="form-group">
            <label>Message:</label>
            <textarea name="message" rows="4" placeholder="Enter your SMS message here..." required></textarea>
        </div>
        <div class="form-group">
            <label>Provider:</label>
            <select name="provider">
                <option value="auto">Auto-select</option>
                <option value="twilio">Twilio</option>
                <option value="nexmo">Nexmo</option>
                <option value="sendgrid">SendGrid</option>
            </select>
        </div>
        <button type="submit">Send SMS</button>
    </form>
</body>
</html>`,
					TemplateData: map[string]string{
						"timestamp": "{{.timestamp}}",
					},
					OutputPath: "/tmp/sms-composer.html",
				},
			},
			// Step 3: SMS Validation
			{
				ID:          "validate-sms-data",
				Name:        "Validate SMS Data",
				Type:        workflow.NodeTypeValidator,
				Description: "Validate phone numbers and message content",
				Config: workflow.NodeConfig{
					ValidationType: "strict",
					ValidationRules: []workflow.ValidationRule{
						{
							Field:   "recipients",
							Type:    "required",
							Message: "Recipients are required",
						},
						{
							Field:   "message",
							Type:    "required",
							Message: "Message content is required",
						},
						{
							Field: "message",
							Type:  "length",
							Value: map[string]interface{}{
								"min": 1,
								"max": 160,
							},
							Message: "Message must be 1-160 characters",
						},
					},
				},
			},
			// Step 4: Provider Selection & Routing
			{
				ID:          "route-sms-provider",
				Name:        "Route SMS Provider",
				Type:        workflow.NodeTypeRouter,
				Description: "Select SMS provider based on routing rules",
				Config: workflow.NodeConfig{
					RoutingRules: []workflow.RoutingRule{
						{
							Condition:   "recipient_count > 100",
							Destination: "bulk_provider",
							Priority:    1,
							Weight:      100,
						},
						{
							Condition:   "country == 'US'",
							Destination: "twilio",
							Priority:    2,
							Weight:      80,
						},
						{
							Condition:   "country == 'UK'",
							Destination: "nexmo",
							Priority:    2,
							Weight:      80,
						},
					},
					DefaultRoute: "standard_provider",
				},
			},
			// Step 5: Phone & Message Validation
			{
				ID:          "validate-phones-spam",
				Name:        "Phone & Spam Validation",
				Type:        workflow.NodeTypeValidator,
				Description: "Validate phone numbers and check for spam",
				Config: workflow.NodeConfig{
					ValidationType: "strict",
					ValidationRules: []workflow.ValidationRule{
						{
							Field:   "recipients",
							Type:    "pattern",
							Value:   `^\+?[1-9]\d{1,14}$`,
							Message: "Invalid phone number format",
						},
					},
				},
			},
			// Step 6: SMS Dispatch
			{
				ID:          "dispatch-sms",
				Name:        "Dispatch SMS",
				Type:        workflow.NodeTypeSMS,
				Description: "Send SMS through selected provider",
				Config: workflow.NodeConfig{
					Provider:    "auto", // Will be set by router
					From:        "+1234567890",
					MessageType: "transactional",
				},
			},
			// Step 7: Send User Notification
			{
				ID:          "notify-user",
				Name:        "Send User Notification",
				Type:        workflow.NodeTypeNotify,
				Description: "Notify user about SMS status",
				Config: workflow.NodeConfig{
					NotifyType: "email",
					Channel:    "smtp",
				},
			},
			// Step 8: Store SMS Report
			{
				ID:          "store-sms-report",
				Name:        "Store SMS Report",
				Type:        workflow.NodeTypeStorage,
				Description: "Store SMS delivery report",
				Config: workflow.NodeConfig{
					StorageType: "database",
					StoragePath: "sms_reports",
					StorageConfig: map[string]string{
						"table":      "sms_reports",
						"connection": "main_db",
					},
				},
			},
			// Step 9: Webhook Receiver for Provider Callbacks
			{
				ID:          "webhook-receiver",
				Name:        "SMS Provider Webhook",
				Type:        workflow.NodeTypeWebhookRx,
				Description: "Receive delivery status from SMS provider",
				Config: workflow.NodeConfig{
					ListenPath: "/webhook/sms/status",
					Secret:     "webhook_secret_key",
					Timeout:    30 * time.Second,
				},
			},
		},
		Edges: []workflow.WorkflowEdge{
			{
				ID:       "auth-to-html",
				FromNode: "user-authentication",
				ToNode:   "generate-sms-page",
			},
			{
				ID:       "html-to-validate",
				FromNode: "generate-sms-page",
				ToNode:   "validate-sms-data",
			},
			{
				ID:       "validate-to-route",
				FromNode: "validate-sms-data",
				ToNode:   "route-sms-provider",
			},
			{
				ID:       "route-to-phone-validate",
				FromNode: "route-sms-provider",
				ToNode:   "validate-phones-spam",
			},
			{
				ID:       "phone-validate-to-dispatch",
				FromNode: "validate-phones-spam",
				ToNode:   "dispatch-sms",
			},
			{
				ID:       "dispatch-to-notify",
				FromNode: "dispatch-sms",
				ToNode:   "notify-user",
			},
			{
				ID:       "notify-to-store",
				FromNode: "notify-user",
				ToNode:   "store-sms-report",
			},
			{
				ID:       "store-to-webhook",
				FromNode: "store-sms-report",
				ToNode:   "webhook-receiver",
			},
		},
		Variables: map[string]workflow.Variable{
			"username": {
				Name:        "username",
				Type:        "string",
				Required:    true,
				Description: "User username for authentication",
			},
			"password": {
				Name:        "password",
				Type:        "string",
				Required:    true,
				Description: "User password for authentication",
			},
			"recipients": {
				Name:        "recipients",
				Type:        "array",
				Required:    true,
				Description: "List of SMS recipients",
			},
			"message": {
				Name:        "message",
				Type:        "string",
				Required:    true,
				Description: "SMS message content",
			},
		},
	}
}

func createWebhookHandlerWorkflow() *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		ID:          "sms-webhook-handler",
		Name:        "SMS Webhook Handler",
		Description: "Processes SMS delivery status webhooks from providers",
		Version:     "1.0.0",
		Status:      workflow.WorkflowStatusActive,
		Nodes: []workflow.WorkflowNode{
			{
				ID:          "parse-webhook",
				Name:        "Parse Webhook Data",
				Type:        workflow.NodeTypeTransform,
				Description: "Parse incoming webhook payload",
				Config: workflow.NodeConfig{
					TransformType: "json_parse",
					Expression:    "$.webhook_data",
				},
			},
			{
				ID:          "validate-webhook",
				Name:        "Validate Webhook",
				Type:        workflow.NodeTypeValidator,
				Description: "Validate webhook signature and data",
				Config: workflow.NodeConfig{
					ValidationType: "strict",
					ValidationRules: []workflow.ValidationRule{
						{
							Field:   "message_id",
							Type:    "required",
							Message: "Message ID required",
						},
						{
							Field:   "status",
							Type:    "required",
							Message: "Delivery status required",
						},
					},
				},
			},
			{
				ID:          "update-report",
				Name:        "Update SMS Report",
				Type:        workflow.NodeTypeStorage,
				Description: "Update SMS report with delivery status",
				Config: workflow.NodeConfig{
					StorageType: "database",
					StoragePath: "sms_reports",
					StorageConfig: map[string]string{
						"operation": "update",
						"table":     "sms_reports",
					},
				},
			},
			{
				ID:          "send-final-notification",
				Name:        "Send Final Notification",
				Type:        workflow.NodeTypeNotify,
				Description: "Send final delivery notification to user",
				Config: workflow.NodeConfig{
					NotifyType: "email",
					Channel:    "smtp",
				},
			},
		},
		Edges: []workflow.WorkflowEdge{
			{
				ID:       "parse-to-validate",
				FromNode: "parse-webhook",
				ToNode:   "validate-webhook",
			},
			{
				ID:       "validate-to-update",
				FromNode: "validate-webhook",
				ToNode:   "update-report",
			},
			{
				ID:       "update-to-notify",
				FromNode: "update-report",
				ToNode:   "send-final-notification",
			},
		},
	}
}

func setupRoutes(app *fiber.App, engine *workflow.WorkflowEngine, userManager *workflow.UserManager, middlewareManager *workflow.MiddlewareManager) {
	// Main page
	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "🚀 Advanced SMS Workflow Engine",
			"version": "2.0.0",
			"features": []string{
				"Sub-DAG Support",
				"HTML Page Generation",
				"SMS Pipeline with Provider Routing",
				"User Authentication & Authorization",
				"Middleware System",
				"Webhook Receivers",
				"Real-time Reporting",
			},
			"endpoints": map[string]string{
				"sms":     "/sms - SMS Pipeline Demo",
				"auth":    "/auth - Authentication Demo",
				"admin":   "/admin - Admin Dashboard",
				"docs":    "/docs - API Documentation",
				"webhook": "/webhook/sms/status - SMS Status Webhook",
			},
		})
	})

	// SMS Pipeline routes
	app.Post("/sms/send", func(c *fiber.Ctx) error {
		var request map[string]interface{}
		if err := c.BodyParser(&request); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
		}

		// Execute middleware chain
		result := middlewareManager.Execute(c.Context(), request)
		if !result.Continue {
			return c.Status(401).JSON(fiber.Map{"error": result.Error.Error()})
		}

		// Execute SMS workflow
		execution, err := engine.ExecuteWorkflow(c.Context(), "sms-pipeline", request, &workflow.ExecutionOptions{
			Priority: workflow.PriorityHigh,
			Owner:    "sms-api",
		})

		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"success":      true,
			"execution_id": execution.ID,
			"status":       execution.Status,
			"message":      "SMS workflow started successfully",
		})
	})

	// Authentication routes
	app.Post("/auth/login", func(c *fiber.Ctx) error {
		var credentials struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}

		if err := c.BodyParser(&credentials); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid credentials format"})
		}

		authContext, err := userManager.AuthenticateUser(credentials.Username, credentials.Password)
		if err != nil {
			return c.Status(401).JSON(fiber.Map{"error": "Invalid credentials"})
		}

		return c.JSON(fiber.Map{
			"success":    true,
			"token":      authContext.Token,
			"user":       authContext.User,
			"expires_at": time.Now().Add(24 * time.Hour),
		})
	})

	app.Post("/auth/validate", func(c *fiber.Ctx) error {
		token := c.Get("Authorization")
		if token == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Authorization header required"})
		}

		authContext, err := userManager.ValidateSession(token)
		if err != nil {
			return c.Status(401).JSON(fiber.Map{"error": "Invalid token"})
		}

		return c.JSON(fiber.Map{
			"valid": true,
			"user":  authContext.User,
		})
	})

	// Admin routes
	app.Get("/admin/workflows", func(c *fiber.Ctx) error {
		workflows, err := engine.ListWorkflows(c.Context(), &workflow.WorkflowFilter{})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"workflows": workflows,
			"total":     len(workflows),
		})
	})

	app.Get("/admin/executions", func(c *fiber.Ctx) error {
		executions, err := engine.ListExecutions(c.Context(), &workflow.ExecutionFilter{})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"executions": executions,
			"total":      len(executions),
		})
	})

	// SMS status webhook endpoint
	app.Post("/webhook/sms/status", func(c *fiber.Ctx) error {
		var webhookData map[string]interface{}
		if err := c.BodyParser(&webhookData); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid webhook data"})
		}

		// Execute webhook handler workflow
		execution, err := engine.ExecuteWorkflow(c.Context(), "sms-webhook-handler", webhookData, &workflow.ExecutionOptions{
			Priority: workflow.PriorityHigh,
			Owner:    "webhook-handler",
		})

		if err != nil {
			log.Printf("Webhook handler error: %v", err)
			return c.Status(500).JSON(fiber.Map{"error": "Webhook processing failed"})
		}

		log.Printf("SMS webhook processed: execution_id=%s", execution.ID)
		return c.JSON(fiber.Map{"status": "processed", "execution_id": execution.ID})
	})

	// Demo pages with proper authentication flow
	app.Get("/login", func(c *fiber.Ctx) error {
		html := `
<!DOCTYPE html>
<html>
<head>
    <title>SMS Workflow - Login</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 500px; margin: 100px auto; padding: 20px; }
        .login-container { padding: 40px; border: 1px solid #ddd; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .form-group { margin-bottom: 20px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        input { width: 100%; padding: 12px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
        button { width: 100%; background: #007bff; color: white; padding: 12px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
        button:hover { background: #0056b3; }
        .error { background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; padding: 10px; border-radius: 4px; margin-top: 10px; }
        .success { background: #d4edda; border: 1px solid #c3e6cb; color: #155724; padding: 10px; border-radius: 4px; margin-top: 10px; }
        .demo-users { background: #e3f2fd; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="login-container">
        <h1>🔐 SMS Workflow Login</h1>

        <div class="demo-users">
            <h3>Demo Users:</h3>
            <p><strong>admin</strong> / password (Full access)</p>
            <p><strong>manager</strong> / password (Write access)</p>
            <p><strong>operator</strong> / password (Read access)</p>
        </div>

        <form id="loginForm">
            <div class="form-group">
                <label for="username">Username:</label>
                <input type="text" id="username" name="username" required>
            </div>

            <div class="form-group">
                <label for="password">Password:</label>
                <input type="password" id="password" name="password" required>
            </div>

            <button type="submit">Login</button>
        </form>

        <div id="result"></div>
    </div>

    <script>
        document.getElementById('loginForm').addEventListener('submit', async function(e) {
            e.preventDefault();

            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            const resultDiv = document.getElementById('result');

            try {
                const response = await fetch('/auth/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username, password })
                });

                const result = await response.json();

                if (result.success) {
                    resultDiv.innerHTML = '<div class="success">✅ Login successful! Redirecting...</div>';
                    // Store token in sessionStorage
                    sessionStorage.setItem('authToken', result.token);
                    sessionStorage.setItem('user', JSON.stringify(result.user));
                    // Redirect to SMS page
                    setTimeout(() => {
                        window.location.href = '/sms';
                    }, 1000);
                } else {
                    resultDiv.innerHTML = '<div class="error">❌ ' + result.error + '</div>';
                }
            } catch (error) {
                resultDiv.innerHTML = '<div class="error">❌ ' + error.message + '</div>';
            }
        });
    </script>
</body>
</html>
		`
		return c.Type("html").Send([]byte(html))
	})

	app.Get("/sms", func(c *fiber.Ctx) error {
		// For /sms page, we'll check authentication on the client side
		// since we store the token in sessionStorage
		// The server will serve the page and let JavaScript handle auth check

		html := `
<!DOCTYPE html>
<html>
<head>
    <title>SMS Workflow Pipeline</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 30px; padding: 20px; background: #f8f9fa; border-radius: 8px; }
        .user-info { font-size: 14px; color: #666; }
        .logout-btn { background: #dc3545; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; }
        .step { margin-bottom: 30px; padding: 20px; border: 1px solid #ddd; border-radius: 8px; }
        .step.active { border-color: #007bff; background: #f8f9ff; }
        .step.completed { border-color: #28a745; background: #f8fff8; }
        .step.disabled { opacity: 0.5; pointer-events: none; }
        .step-header { display: flex; align-items: center; margin-bottom: 15px; }
        .step-number { background: #007bff; color: white; width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin-right: 15px; font-weight: bold; }
        .step.completed .step-number { background: #28a745; }
        .step.disabled .step-number { background: #6c757d; }
        button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; margin: 5px; }
        button:disabled { background: #6c757d; cursor: not-allowed; }
        button.next { background: #28a745; }
        input, textarea, select { width: 100%; padding: 8px; margin: 5px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        .result { margin-top: 15px; padding: 10px; border-radius: 4px; }
        .success { background: #d4edda; border: 1px solid #c3e6cb; color: #155724; }
        .error { background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; }
        .info { background: #e3f2fd; border: 1px solid #bbdefb; color: #0c5460; }
        .provider-option { display: flex; align-items: center; margin: 10px 0; padding: 15px; border: 1px solid #ddd; border-radius: 4px; cursor: pointer; }
        .provider-option:hover { background: #f8f9fa; }
        .provider-option.selected { border-color: #007bff; background: #f8f9ff; }
        .waiting { display: none; padding: 20px; text-align: center; }
        .spinner { border: 4px solid #f3f3f3; border-top: 4px solid #3498db; border-radius: 50%; width: 40px; height: 40px; animation: spin 2s linear infinite; margin: 0 auto 15px; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 SMS Workflow Pipeline</h1>
        <div>
            <span class="user-info" id="userInfo">Loading...</span>
            <button class="logout-btn" onclick="logout()">Logout</button>
        </div>
    </div>

    <!-- Step 1: SMS Message Input -->
    <div class="step active" id="step1">
        <div class="step-header">
            <div class="step-number">1</div>
            <h2>Enter SMS Details</h2>
        </div>
        <div class="form-group">
            <label for="recipients">Recipients (comma-separated phone numbers):</label>
            <input type="text" id="recipients" placeholder="+1234567890,+0987654321" value="+1234567890">
        </div>
        <div class="form-group">
            <label for="message">Message:</label>
            <textarea id="message" rows="4" placeholder="Enter your SMS message here...">Hello! This is a test SMS from the advanced workflow engine.</textarea>
        </div>
        <button class="next" onclick="nextStep(1)">Next: Choose Provider</button>
        <div id="step1-result" class="result" style="display: none;"></div>
    </div>

    <!-- Step 2: Provider Selection -->
    <div class="step disabled" id="step2">
        <div class="step-header">
            <div class="step-number">2</div>
            <h2>Choose SMS Provider</h2>
        </div>
        <div id="providerOptions">
            <div class="provider-option" onclick="selectProvider('auto')">
                <div>
                    <h3>🤖 Auto-Select (Recommended)</h3>
                    <p>Let the system choose the best provider based on cost and delivery rates</p>
                </div>
            </div>
            <div class="provider-option" onclick="selectProvider('twilio')">
                <div>
                    <h3>📱 Twilio</h3>
                    <p>Premium provider with high delivery rates - $0.0075/SMS</p>
                </div>
            </div>
            <div class="provider-option" onclick="selectProvider('nexmo')">
                <div>
                    <h3>🌍 Vonage (Nexmo)</h3>
                    <p>Global coverage with competitive pricing - $0.0065/SMS</p>
                </div>
            </div>
            <div class="provider-option" onclick="selectProvider('aws')">
                <div>
                    <h3>☁️ AWS SNS</h3>
                    <p>Reliable cloud-based SMS service - $0.0055/SMS</p>
                </div>
            </div>
        </div>
        <button onclick="previousStep(2)">Previous</button>
        <button class="next" onclick="nextStep(2)" disabled id="step2-next">Next: Send SMS</button>
        <div id="step2-result" class="result" style="display: none;"></div>
    </div>

    <!-- Step 3: Send SMS and Wait for Callback -->
    <div class="step disabled" id="step3">
        <div class="step-header">
            <div class="step-number">3</div>
            <h2>Send SMS & Wait for Delivery Status</h2>
        </div>
        <div id="smsPreview">
            <h3>📋 SMS Preview:</h3>
            <div id="previewContent" style="background: #f8f9fa; padding: 15px; border-radius: 4px; margin-bottom: 15px;"></div>
        </div>
        <button onclick="previousStep(3)">Previous</button>
        <button class="next" onclick="sendSMS()" id="sendButton">🚀 Send SMS</button>

        <div class="waiting" id="waiting">
            <div class="spinner"></div>
            <h3>Sending SMS...</h3>
            <p>Please wait while we process your SMS through the workflow pipeline</p>
        </div>

        <div id="step3-result" class="result" style="display: none;"></div>
    </div>

    <script>
        let currentStep = 1;
        let selectedProvider = '';
        let smsData = {};
        let authToken = sessionStorage.getItem('authToken');
        let user = JSON.parse(sessionStorage.getItem('user') || '{}');

        // Initialize page
        window.onload = function() {
            if (!authToken) {
                window.location.href = '/login';
                return;
            }
            document.getElementById('userInfo').textContent = 'Logged in as: ' + user.username + ' (' + user.role + ')';
        };

        function nextStep(step) {
            if (step === 1) {
                // Validate SMS input
                const recipients = document.getElementById('recipients').value;
                const message = document.getElementById('message').value;

                if (!recipients || !message) {
                    showResult('step1-result', 'error', '❌ Please fill in all fields');
                    return;
                }

                smsData.recipients = recipients;
                smsData.message = message;

                showResult('step1-result', 'success', '✅ SMS details saved');

                // Move to step 2
                document.getElementById('step1').classList.remove('active');
                document.getElementById('step1').classList.add('completed');
                document.getElementById('step2').classList.remove('disabled');
                document.getElementById('step2').classList.add('active');
                currentStep = 2;

            } else if (step === 2) {
                if (!selectedProvider) {
                    showResult('step2-result', 'error', '❌ Please select a provider');
                    return;
                }

                smsData.provider = selectedProvider;
                showResult('step2-result', 'success', '✅ Provider selected: ' + selectedProvider);

                // Move to step 3
                document.getElementById('step2').classList.remove('active');
                document.getElementById('step2').classList.add('completed');
                document.getElementById('step3').classList.remove('disabled');
                document.getElementById('step3').classList.add('active');
                currentStep = 3;

                // Update preview
                updatePreview();
            }
        }

        function previousStep(step) {
            if (step === 2) {
                document.getElementById('step2').classList.remove('active');
                document.getElementById('step2').classList.add('disabled');
                document.getElementById('step1').classList.remove('completed');
                document.getElementById('step1').classList.add('active');
                currentStep = 1;
            } else if (step === 3) {
                document.getElementById('step3').classList.remove('active');
                document.getElementById('step3').classList.add('disabled');
                document.getElementById('step2').classList.remove('completed');
                document.getElementById('step2').classList.add('active');
                currentStep = 2;
            }
        }

        function selectProvider(provider) {
            selectedProvider = provider;

            // Update UI
            document.querySelectorAll('.provider-option').forEach(option => {
                option.classList.remove('selected');
            });
            event.currentTarget.classList.add('selected');

            document.getElementById('step2-next').disabled = false;
        }

        function updatePreview() {
            const preview = document.getElementById('previewContent');
            preview.innerHTML =
                '<strong>Recipients:</strong> ' + smsData.recipients + '<br>' +
                '<strong>Provider:</strong> ' + smsData.provider + '<br>' +
                '<strong>Message:</strong><br>' + smsData.message;
        }

        async function sendSMS() {
            const waiting = document.getElementById('waiting');
            const sendButton = document.getElementById('sendButton');

            waiting.style.display = 'block';
            sendButton.disabled = true;

            try {
                const response = await fetch('/sms/send', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + authToken
                    },
                    body: JSON.stringify({
                        recipients: smsData.recipients.split(','),
                        message: smsData.message,
                        provider: smsData.provider
                    })
                });

                const result = await response.json();
                waiting.style.display = 'none';

                if (result.success) {
                    showResult('step3-result', 'success',
                        '✅ SMS sent successfully!<br>' +
                        'Execution ID: ' + result.execution_id + '<br>' +
                        'Status: ' + result.status + '<br>' +
                        '<button onclick="checkStatus(\'' + result.execution_id + '\')">Check Status</button>'
                    );
                } else {
                    showResult('step3-result', 'error', '❌ ' + result.error);
                    sendButton.disabled = false;
                }
            } catch (error) {
                waiting.style.display = 'none';
                showResult('step3-result', 'error', '❌ ' + error.message);
                sendButton.disabled = false;
            }
        }

        async function checkStatus(executionId) {
            try {
                const response = await fetch('/admin/executions?id=' + executionId, {
                    headers: { 'Authorization': 'Bearer ' + authToken }
                });
                const result = await response.json();

                showResult('step3-result', 'info',
                    '📊 Status: ' + result.status + '<br>' +
                    'Progress: ' + result.executed_nodes.length + ' steps completed'
                );
            } catch (error) {
                showResult('step3-result', 'error', '❌ Failed to check status: ' + error.message);
            }
        }

        function showResult(elementId, type, message) {
            const element = document.getElementById(elementId);
            element.className = 'result ' + type;
            element.innerHTML = message;
            element.style.display = 'block';
        }

        function logout() {
            sessionStorage.removeItem('authToken');
            sessionStorage.removeItem('user');
            window.location.href = '/login';
        }
    </script>
</body>
</html>
		`
		return c.Type("html").Send([]byte(html))
	})

	// API Documentation
	app.Get("/docs", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"title":   "Advanced SMS Workflow Engine API",
			"version": "2.0.0",
			"endpoints": map[string]interface{}{
				"POST /auth/login": map[string]interface{}{
					"description": "Authenticate user and get token",
					"body": map[string]string{
						"username": "string",
						"password": "string",
					},
				},
				"POST /sms/send": map[string]interface{}{
					"description": "Send SMS through workflow pipeline",
					"headers": map[string]string{
						"Authorization": "Bearer token",
					},
					"body": map[string]interface{}{
						"recipients": []string{"+1234567890"},
						"message":    "string",
						"provider":   "auto|twilio|nexmo",
					},
				},
				"POST /webhook/sms/status": map[string]interface{}{
					"description": "Receive SMS delivery status webhook",
					"body": map[string]interface{}{
						"message_id": "string",
						"status":     "delivered|failed|pending",
						"timestamp":  "ISO 8601 string",
					},
				},
			},
			"workflows": []string{
				"user-auth-subdag - User authentication sub-workflow",
				"sms-pipeline - Complete SMS sending pipeline",
				"sms-webhook-handler - SMS delivery status handler",
			},
		})
	})

	log.Println("✅ All routes configured successfully")
}
