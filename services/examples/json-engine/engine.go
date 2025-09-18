package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
	htmlTemplate "text/template"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/oarkflow/mq/dag"
)

// Helper functions for default values
func getDefaultInt(value, defaultValue int) int {
	if value != 0 {
		return value
	}
	return defaultValue
}

func getDefaultDuration(value string, defaultValue time.Duration) time.Duration {
	if value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}

// NewJSONEngine creates a new JSON-driven workflow engine
func NewJSONEngine(config *AppConfiguration) *JSONEngine {
	// Use configuration from JSON or defaults
	var workflowEngineConfig *WorkflowEngineConfig
	if config.WorkflowEngine != nil {
		workflowEngineConfig = config.WorkflowEngine
	} else {
		// Default configuration
		workflowEngineConfig = &WorkflowEngineConfig{
			MaxWorkers:       4,
			ExecutionTimeout: "30s",
			EnableMetrics:    true,
			EnableAudit:      true,
			EnableTracing:    true,
			LogLevel:         "info",
			Storage: StorageConfig{
				Type:           "memory",
				MaxConnections: 10,
			},
			Security: SecurityConfig{
				EnableAuth:     false,
				AllowedOrigins: []string{"*"},
			},
		}
	}

	dagWorkflowConfig := &dag.WorkflowEngineConfig{
		MaxConcurrentExecutions: getDefaultInt(workflowEngineConfig.MaxWorkers, 10),
		DefaultTimeout:          getDefaultDuration(workflowEngineConfig.ExecutionTimeout, 30*time.Second),
		EnablePersistence:       true, // Enable persistence for enhanced features
		EnableSecurity:          workflowEngineConfig.Security.EnableAuth,
		EnableMiddleware:        true, // Enable middleware for enhanced features
		EnableScheduling:        true, // Enable scheduling for enhanced features
	}
	workflowEngine := dag.NewWorkflowEngineManager(dagWorkflowConfig)

	engine := &JSONEngine{
		workflowEngine:       workflowEngine,
		workflowEngineConfig: workflowEngineConfig,
		templates:            make(map[string]*Template),
		workflows:            make(map[string]*Workflow),
		functions:            make(map[string]*Function),
		validators:           make(map[string]*Validator),
		middleware:           make(map[string]*Middleware),
		data:                 make(map[string]any),
		genericData:          make(map[string]any),
	}

	// Store the configuration
	engine.config = config

	return engine
}

// LoadConfiguration loads and parses JSON configuration
func (e *JSONEngine) LoadConfiguration(configPath string) error {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %v", err)
	}

	var config AppConfiguration
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse JSON config: %v", err)
	}

	e.config = &config
	return nil
}

// Compile compiles the JSON configuration into executable components
func (e *JSONEngine) Compile() error {
	log.Println("🔨 Compiling JSON configuration...")

	// Store global data and merge with genericData
	e.data = e.config.Data

	// Initialize genericData with config data for backward compatibility
	if e.genericData == nil {
		e.genericData = make(map[string]any)
	}

	// Merge config data into genericData
	for key, value := range e.config.Data {
		e.genericData[key] = value
	}

	// 1. Compile templates
	if err := e.compileTemplates(); err != nil {
		return fmt.Errorf("template compilation failed: %v", err)
	}

	// 2. Compile functions
	if err := e.compileFunctions(); err != nil {
		return fmt.Errorf("function compilation failed: %v", err)
	}

	// 3. Compile validators
	if err := e.compileValidators(); err != nil {
		return fmt.Errorf("validator compilation failed: %v", err)
	}

	// 4. Compile workflows
	if err := e.compileWorkflows(); err != nil {
		return fmt.Errorf("workflow compilation failed: %v", err)
	}

	// 5. Compile middleware
	if err := e.compileMiddleware(); err != nil {
		return fmt.Errorf("middleware compilation failed: %v", err)
	}

	// 6. Store global data and merge with genericData
	e.data = e.config.Data

	// Initialize genericData with config data for backward compatibility
	if e.genericData == nil {
		e.genericData = make(map[string]any)
	}

	// Merge config data into genericData
	for key, value := range e.config.Data {
		e.genericData[key] = value
	}

	log.Println("✅ JSON configuration compiled successfully")
	return nil
}

// Start starts the HTTP server with compiled configuration
func (e *JSONEngine) Start() error {
	// Create Fiber app
	e.app = fiber.New(fiber.Config{
		AppName: e.config.App.Name,
	})

	// Add default middleware
	e.app.Use(cors.New())
	e.app.Use(logger.New())
	e.app.Use(recover.New())

	// Setup middleware
	if err := e.setupMiddleware(); err != nil {
		return fmt.Errorf("middleware setup failed: %v", err)
	}

	// Setup routes
	if err := e.setupRoutes(); err != nil {
		return fmt.Errorf("route setup failed: %v", err)
	}

	// Start workflow engine
	if err := e.workflowEngine.Start(context.Background()); err != nil {
		return fmt.Errorf("failed to start workflow engine: %v", err)
	}

	// Start server
	host := e.config.App.Host
	if host == "" {
		host = "localhost"
	}
	port := e.config.App.Port
	if port == "" {
		port = "3000"
	}

	address := host + ":" + port
	log.Printf("🚀 %s started on http://%s", e.config.App.Name, address)
	log.Printf("📖 Description: %s", e.config.App.Description)
	log.Printf("🔢 Version: %s", e.config.App.Version)

	return e.app.Listen(":" + port)
}

// compileTemplates compiles all HTML templates
func (e *JSONEngine) compileTemplates() error {
	for id, templateConfig := range e.config.Templates {
		log.Printf("Compiling template: %s", id)

		// Get template content from either Content or Template field
		templateContent := templateConfig.Content
		if templateContent == "" {
			templateContent = templateConfig.Template
		}

		if templateContent == "" {
			return fmt.Errorf("template %s has no content", id)
		}

		// Create template
		tmpl, err := htmlTemplate.New(id).Parse(templateContent)
		if err != nil {
			return fmt.Errorf("failed to parse template %s: %v", id, err)
		}

		e.templates[id] = &Template{
			ID:       id,
			Config:   templateConfig,
			Compiled: tmpl,
		}
	}
	return nil
}

// compileFunctions compiles all function definitions dynamically
func (e *JSONEngine) compileFunctions() error {
	for id, functionConfig := range e.config.Functions {
		log.Printf("Compiling function: %s", id)

		function := &Function{
			ID:      id,
			Config:  functionConfig,
			Runtime: make(map[string]any),
		}

		// Compile function based on type - completely generic approach
		switch functionConfig.Type {
		case "http":
			function.Handler = e.createHTTPFunction(functionConfig)
		case "expression":
			function.Handler = e.createExpressionFunction(functionConfig)
		case "template":
			function.Handler = e.createTemplateFunction(functionConfig)
		case "js", "javascript":
			function.Handler = e.createJSFunction(functionConfig)
		case "builtin":
			function.Handler = e.createBuiltinFunction(functionConfig)
		case "custom":
			function.Handler = e.createCustomFunction(functionConfig)
		default:
			// For unknown types, create a generic function that can be extended
			function.Handler = e.createGenericFunction(functionConfig)
		}

		e.functions[id] = function
	}
	return nil
}

// compileValidators compiles all validators with generic approach
func (e *JSONEngine) compileValidators() error {
	for id, validatorConfig := range e.config.Validators {
		log.Printf("Compiling validator: %s", id)

		e.validators[id] = &Validator{
			ID:      id,
			Config:  validatorConfig,
			Rules:   validatorConfig.Rules, // Now using generic map
			Runtime: make(map[string]any),
		}
	}
	return nil
}

// compileWorkflows compiles all workflows
func (e *JSONEngine) compileWorkflows() error {
	for _, workflowConfig := range e.config.Workflows {
		log.Printf("Compiling workflow: %s", workflowConfig.ID)

		workflow := &Workflow{
			ID:     workflowConfig.ID,
			Config: workflowConfig,
			Nodes:  make(map[string]*Node),
			Edges:  make([]*Edge, 0),
			Runtime: &WorkflowRuntime{
				Context:   make(map[string]any),
				Variables: workflowConfig.Variables,
				Status:    "ready",
			},
		}

		// Compile nodes
		for _, nodeConfig := range workflowConfig.Nodes {
			node := &Node{
				ID:      nodeConfig.ID,
				Config:  nodeConfig,
				Inputs:  make(map[string]any),
				Outputs: make(map[string]any),
			}

			// Link function if specified
			if nodeConfig.Function != "" {
				if function, exists := e.functions[nodeConfig.Function]; exists {
					node.Function = function
				}
			}

			workflow.Nodes[nodeConfig.ID] = node
		}

		// Compile edges
		for _, edgeConfig := range workflowConfig.Edges {
			edge := &Edge{
				ID:     edgeConfig.ID,
				Config: edgeConfig,
				From:   workflow.Nodes[edgeConfig.From],
				To:     workflow.Nodes[edgeConfig.To],
			}
			workflow.Edges = append(workflow.Edges, edge)
		}

		e.workflows[workflowConfig.ID] = workflow
	}
	return nil
}

// compileMiddleware compiles all middleware
func (e *JSONEngine) compileMiddleware() error {
	for _, middlewareConfig := range e.config.Middleware {
		if !middlewareConfig.Enabled {
			continue
		}

		log.Printf("Compiling middleware: %s", middlewareConfig.ID)

		middleware := &Middleware{
			ID:     middlewareConfig.ID,
			Config: middlewareConfig,
		}

		// Create middleware handler based on type
		switch middlewareConfig.Type {
		case "auth":
			middleware.Handler = e.createAuthMiddleware(middlewareConfig)
		case "logging":
			middleware.Handler = e.createLoggingMiddleware(middlewareConfig)
		case "ratelimit":
			middleware.Handler = e.createRateLimitMiddleware(middlewareConfig)
		case "custom":
			middleware.Handler = e.createCustomMiddleware(middlewareConfig)
		default:
			log.Printf("Unknown middleware type: %s", middlewareConfig.Type)
			continue
		}

		e.middleware[middlewareConfig.ID] = middleware
	}
	return nil
}

// setupMiddleware sets up middleware in the Fiber app
func (e *JSONEngine) setupMiddleware() error {
	// Sort middleware by priority
	middlewares := make([]*Middleware, 0, len(e.middleware))
	for _, middleware := range e.middleware {
		middlewares = append(middlewares, middleware)
	}

	// Simple priority sort (lower number = higher priority)
	for i := 0; i < len(middlewares)-1; i++ {
		for j := i + 1; j < len(middlewares); j++ {
			if middlewares[i].Config.Priority > middlewares[j].Config.Priority {
				middlewares[i], middlewares[j] = middlewares[j], middlewares[i]
			}
		}
	}

	// Apply middleware
	for _, middleware := range middlewares {
		e.app.Use(middleware.Handler)
		log.Printf("Applied middleware: %s (priority: %d)", middleware.Config.ID, middleware.Config.Priority)
	}

	return nil
}

// setupRoutes sets up all routes from configuration
func (e *JSONEngine) setupRoutes() error {
	for _, routeConfig := range e.config.Routes {
		log.Printf("Setting up route: %s %s", routeConfig.Method, routeConfig.Path)

		handler := e.createRouteHandler(routeConfig)

		// Apply route to Fiber app
		switch strings.ToUpper(routeConfig.Method) {
		case "GET":
			e.app.Get(routeConfig.Path, handler)
		case "POST":
			e.app.Post(routeConfig.Path, handler)
		case "PUT":
			e.app.Put(routeConfig.Path, handler)
		case "DELETE":
			e.app.Delete(routeConfig.Path, handler)
		case "PATCH":
			e.app.Patch(routeConfig.Path, handler)
		default:
			return fmt.Errorf("unsupported HTTP method: %s", routeConfig.Method)
		}
	}
	return nil
}

// createRouteHandler creates a Fiber handler for a route configuration
func (e *JSONEngine) createRouteHandler(routeConfig RouteConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		log.Printf("Handler called for route: %s %s", routeConfig.Method, routeConfig.Path)

		// Create execution context with enhanced generic data
		ctx := &ExecutionContext{
			Request:    c,
			Data:       make(map[string]any),
			Variables:  make(map[string]any),
			Session:    make(map[string]any),
			User:       make(map[string]any),
			Functions:  e.functions,
			Validators: e.validators,
			Config:     e.config,
			Runtime:    make(map[string]any),
			Context:    make(map[string]any),
		}

		// Add global and generic data to context
		for k, v := range e.data {
			ctx.Data[k] = v
		}
		for k, v := range e.genericData {
			ctx.Data[k] = v
		} // Apply route middleware - skip auth middleware as we handle it below
		for _, middlewareID := range routeConfig.Middleware {
			if middlewareID == "auth" {
				continue // Skip auth middleware, handle it in route
			}
			if middleware, exists := e.middleware[middlewareID]; exists {
				if err := middleware.Handler(c); err != nil {
					log.Printf("Middleware %s failed: %v", middlewareID, err)
					return err
				}
			}
		}

		// Check authentication if required
		if routeConfig.Auth != nil && routeConfig.Auth.Required {
			if err := e.checkAuthentication(ctx, routeConfig.Auth); err != nil {
				if routeConfig.Auth.Redirect != "" {
					return c.Redirect(routeConfig.Auth.Redirect)
				}
				return c.Status(401).JSON(fiber.Map{"error": "Authentication required"})
			}
		}

		// Execute handler based on type - with generic fallback
		switch routeConfig.Handler.Type {
		case "template":
			return e.handleTemplate(ctx, routeConfig)
		case "workflow":
			return e.handleWorkflow(ctx, routeConfig)
		case "function":
			return e.handleFunction(ctx, routeConfig)
		case "redirect":
			return c.Redirect(routeConfig.Handler.Target)
		case "static":
			return e.handleStatic(ctx, routeConfig)
		case "json":
			return e.handleJSON(ctx, routeConfig)
		case "api":
			return e.handleAPI(ctx, routeConfig)
		default:
			// Generic handler for unknown types
			return e.handleGeneric(ctx, routeConfig)
		}
	}
}

// Template execution methods will be implemented next...
func (e *JSONEngine) handleTemplate(ctx *ExecutionContext, routeConfig RouteConfig) error {
	templateID := routeConfig.Handler.Target
	template, exists := e.templates[templateID]
	if !exists {
		return ctx.Request.Status(404).JSON(fiber.Map{"error": "Template not found: " + templateID})
	}

	// Prepare template data
	data := make(map[string]any)

	// Add global data
	for k, v := range e.data {
		data[k] = v
	}

	// Add template-specific data
	for k, v := range template.Config.Data {
		data[k] = v
	}

	// Add input data from handler configuration
	for k, v := range routeConfig.Handler.Input {
		data[k] = v
	}

	// For employee_form template, ensure proper employee data structure
	if templateID == "employee_form" {
		if emp, exists := data["employee"]; !exists || emp == nil {
			// For add mode: provide empty employee object and set isEditMode to false
			data["employee"] = map[string]any{
				"id":         "",
				"name":       "",
				"email":      "",
				"department": "",
				"position":   "",
				"salary":     "",
				"hire_date":  "",
				"status":     "active",
			}
			data["isEditMode"] = false
		} else {
			// For edit mode: ensure employee has all required fields and set isEditMode to true
			if empMap, ok := emp.(map[string]any); ok {
				// Fill in any missing fields with empty values
				fields := []string{"id", "name", "email", "department", "position", "salary", "hire_date", "status"}
				for _, field := range fields {
					if _, exists := empMap[field]; !exists {
						empMap[field] = ""
					}
				}
				data["employee"] = empMap
				data["isEditMode"] = true
			}
		}
	}

	// Add request data
	data["request"] = map[string]any{
		"method":  ctx.Request.Method(),
		"path":    ctx.Request.Path(),
		"query":   ctx.Request.Queries(),
		"headers": ctx.Request.GetReqHeaders(),
		"body":    string(ctx.Request.Body()),
	}

	// Add context data
	data["user"] = ctx.User
	data["session"] = ctx.Session
	data["variables"] = ctx.Variables

	// Execute template
	tmpl := template.Compiled.(*htmlTemplate.Template)
	var buf strings.Builder

	log.Printf("DEBUG: About to execute template %s with data keys: %v", templateID, func() []string {
		keys := make([]string, 0, len(data))
		for k := range data {
			keys = append(keys, k)
		}
		return keys
	}())

	if err := tmpl.Execute(&buf, data); err != nil {
		log.Printf("DEBUG: Template execution error: %v", err)
		return ctx.Request.Status(500).JSON(fiber.Map{"error": "Template execution failed: " + err.Error()})
	}

	// Set response type
	contentType := "text/html; charset=utf-8"
	if template.Config.Type == "json" {
		contentType = "application/json"
	} else if template.Config.Type == "xml" {
		contentType = "application/xml"
	} else if template.Config.Type == "text" {
		contentType = "text/plain"
	}

	ctx.Request.Set("Content-Type", contentType)
	return ctx.Request.Send([]byte(buf.String()))
}

// renderTemplate renders a template with data
func (e *JSONEngine) renderTemplate(template *Template, data map[string]any) (string, error) {
	tmpl := template.Compiled.(*htmlTemplate.Template)
	var buf strings.Builder
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (e *JSONEngine) handleWorkflow(ctx *ExecutionContext, routeConfig RouteConfig) error {
	workflowID := routeConfig.Handler.Target
	log.Printf("Looking for workflow: %s", workflowID)
	log.Printf("Available workflows: %v", func() []string {
		keys := make([]string, 0, len(e.workflows))
		for k := range e.workflows {
			keys = append(keys, k)
		}
		return keys
	}())

	workflow, exists := e.workflows[workflowID]
	if !exists {
		return ctx.Request.Status(404).JSON(fiber.Map{"error": "Workflow not found: " + workflowID})
	}

	// Execute workflow
	result, err := e.executeWorkflow(ctx, workflow, routeConfig.Handler.Input)
	if err != nil {
		return ctx.Request.Status(500).JSON(fiber.Map{"error": "Workflow execution failed: " + err.Error()})
	}

	// Return result based on response configuration
	if routeConfig.Response.Type == "json" {
		// Clean the result to avoid circular references
		cleanResult := e.sanitizeResult(result)
		return ctx.Request.JSON(cleanResult)
	} else if routeConfig.Response.Type == "html" && routeConfig.Response.Template != "" {
		// Render result using template
		template, exists := e.templates[routeConfig.Response.Template]
		if !exists {
			return ctx.Request.Status(500).JSON(fiber.Map{"error": "Response template not found"})
		}

		tmpl := template.Compiled.(*htmlTemplate.Template)
		var buf strings.Builder
		if err := tmpl.Execute(&buf, result); err != nil {
			return ctx.Request.Status(500).JSON(fiber.Map{"error": "Response template execution failed"})
		}

		return ctx.Request.Type("text/html").Send([]byte(buf.String()))
	}

	// Clean the result to avoid circular references
	cleanResult := e.sanitizeResult(result)
	return ctx.Request.JSON(cleanResult)
}

func (e *JSONEngine) handleFunction(ctx *ExecutionContext, routeConfig RouteConfig) error {
	functionID := routeConfig.Handler.Target

	// Look up the function in our compiled functions
	function, exists := e.functions[functionID]
	if !exists {
		return ctx.Request.Status(404).JSON(fiber.Map{"error": "Function not found: " + functionID})
	}

	// Prepare input data
	input := make(map[string]any)

	// Add handler input if specified
	if routeConfig.Handler.Input != nil {
		for k, v := range routeConfig.Handler.Input {
			input[k] = v
		}
	}

	// Add request body for POST/PUT requests
	if ctx.Request.Method() == "POST" || ctx.Request.Method() == "PUT" {
		var body map[string]any
		if err := ctx.Request.BodyParser(&body); err == nil {
			for k, v := range body {
				input[k] = v
			}
		}
	}

	// Add query parameters
	for k, v := range ctx.Request.Queries() {
		input[k] = v
	}

	// Add route parameters (if any) - specifically handle common parameter names
	if id := ctx.Request.Params("id"); id != "" {
		input["id"] = id
	}
	if userId := ctx.Request.Params("userId"); userId != "" {
		input["userId"] = userId
	}
	if employeeId := ctx.Request.Params("employeeId"); employeeId != "" {
		input["employeeId"] = employeeId
	}
	// Add any other route parameters that might exist
	// Note: Fiber v2 doesn't have a direct AllParams() method, so we handle common cases

	// Execute function
	result, err := e.executeFunction(ctx, function, input)
	if err != nil {
		return ctx.Request.Status(500).JSON(fiber.Map{"error": "Function execution failed: " + err.Error()})
	}

	// Check if we should render a template with the result
	if routeConfig.Handler.Template != "" {
		templateID := routeConfig.Handler.Template
		template, exists := e.templates[templateID]
		if !exists {
			return ctx.Request.Status(404).JSON(fiber.Map{"error": "Template not found: " + templateID})
		}

		// Merge function result with context data
		templateData := make(map[string]any)

		// Add global data first
		for k, v := range ctx.Data {
			templateData[k] = v
		}

		// Add all function result data directly (this includes the employee field)
		for k, v := range result {
			templateData[k] = v
		}

		// For employee_form template, ensure employee field exists (even if nil)
		if templateID == "employee_form" && templateData["employee"] == nil {
			templateData["employee"] = nil
		}

		// Render template
		rendered, err := e.renderTemplate(template, templateData)
		if err != nil {
			return ctx.Request.Status(500).JSON(fiber.Map{"error": "Template rendering failed: " + err.Error()})
		}

		return ctx.Request.Type("html").Send([]byte(rendered))
	}

	return ctx.Request.JSON(result)
}

func (e *JSONEngine) handleStatic(ctx *ExecutionContext, routeConfig RouteConfig) error {
	// Serve static content
	return ctx.Request.SendFile(routeConfig.Handler.Target)
}

// Utility methods for creating different types of handlers and middleware
func (e *JSONEngine) checkAuthentication(ctx *ExecutionContext, auth *AuthConfig) error {
	// Simple session-based authentication for demo
	if auth.Type == "session" {
		token := ctx.Request.Get("Authorization")
		if token == "" {
			// Check for token in query params or body
			token = ctx.Request.Query("token")
		}
		if token == "" && ctx.Request.Method() == "POST" {
			var body map[string]any
			if err := ctx.Request.BodyParser(&body); err == nil {
				if t, ok := body["token"].(string); ok {
					token = t
				}
			}
		}

		if token == "" {
			return fmt.Errorf("no authentication token provided")
		}

		// Simple token validation (in real app, validate JWT or session)
		ctx.User = map[string]any{
			"id":       "user_" + token,
			"username": "demo_user",
			"role":     "user",
		}
	}

	return nil
}

// Function executors
func (e *JSONEngine) createHTTPFunction(config FunctionConfig) any {
	return func(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
		client := &http.Client{Timeout: getDefaultDuration(e.workflowEngineConfig.ExecutionTimeout, 30*time.Second)}

		method := config.Method
		if method == "" {
			method = "GET"
		}

		req, err := http.NewRequest(method, config.URL, nil)
		if err != nil {
			return nil, err
		}

		// Add headers with type assertion
		for k, v := range config.Headers {
			if vStr, ok := v.(string); ok {
				req.Header.Set(k, vStr)
			} else {
				req.Header.Set(k, fmt.Sprintf("%v", v))
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			// If not JSON, return as string
			result = map[string]any{
				"status": resp.StatusCode,
				"body":   string(body),
			}
		}

		return result, nil
	}
}

func (e *JSONEngine) createExpressionFunction(config FunctionConfig) any {
	return func(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
		// Special handling for authentication function
		if config.ID == "authenticate_user" || strings.Contains(config.Code, "validate user credentials") {
			return e.handleAuthentication(ctx, input)
		}

		// If there's a response configuration, use it directly
		if config.Response != nil {
			result := make(map[string]any)

			// Process response template with data substitution
			for key, value := range config.Response {
				if valueStr, ok := value.(string); ok {
					// Handle template substitution like {{.employees}}, {{.blog_posts}}, etc.
					if strings.Contains(valueStr, "{{.") {
						// Extract the data key from the template using regex
						re := regexp.MustCompile(`\{\{\.(\w+)\}\}`)
						matches := re.FindAllStringSubmatch(valueStr, -1)

						resultValue := valueStr
						for _, match := range matches {
							if len(match) > 1 {
								dataKey := match[1]
								if dataValue, exists := e.data[dataKey]; exists {
									// Replace the template with actual data
									resultValue = strings.ReplaceAll(resultValue, "{{"+dataKey+"}}", "")
									result[key] = dataValue
								} else {
									result[key] = valueStr // Keep original if data not found
								}
							}
						}

						if resultValue != valueStr {
							// Template was processed
							continue
						}
					}
					result[key] = value
				} else {
					result[key] = value
				}
			}

			return result, nil
		}

		// Enhanced expression evaluation with JSON parsing and variable substitution
		expression := config.Code
		if expression == "" {
			expression = config.Expression
		}

		// Only replace variables that are formatted as placeholders {{variable}} to avoid corrupting JSON
		for key, value := range input {
			placeholder := "{{" + key + "}}"
			var valueStr string
			switch v := value.(type) {
			case string:
				valueStr = fmt.Sprintf("\"%s\"", v)
			case int, int64, float64:
				valueStr = fmt.Sprintf("%v", v)
			case bool:
				valueStr = fmt.Sprintf("%t", v)
			default:
				valueStr = fmt.Sprintf("\"%v\"", v)
			}
			expression = strings.ReplaceAll(expression, placeholder, valueStr)
		}

		// Try to parse as JSON first
		if strings.HasPrefix(strings.TrimSpace(expression), "{") {
			var jsonResult map[string]any
			if err := json.Unmarshal([]byte(expression), &jsonResult); err == nil {
				return jsonResult, nil
			} else {
				log.Printf("Failed to parse JSON expression: %s, error: %v", expression, err)
			}
		}

		// If not JSON, return as simple result
		return map[string]any{
			"result": expression,
		}, nil
	}
}

func (e *JSONEngine) createTemplateFunction(config FunctionConfig) any {
	return func(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
		tmpl, err := htmlTemplate.New("function").Parse(config.Code)
		if err != nil {
			return nil, err
		}

		var buf strings.Builder
		if err := tmpl.Execute(&buf, input); err != nil {
			return nil, err
		}

		return map[string]any{
			"result": buf.String(),
		}, nil
	}
}

func (e *JSONEngine) createJSFunction(config FunctionConfig) any {
	return func(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
		// Placeholder for JavaScript execution (would use goja or similar in production)
		return map[string]any{
			"result": "JavaScript execution not implemented in demo",
			"code":   config.Code,
			"input":  input,
		}, nil
	}
}

// Additional generic route handlers for any application type
func (e *JSONEngine) handleJSON(ctx *ExecutionContext, routeConfig RouteConfig) error {
	// Handle pure JSON responses
	response := make(map[string]any)

	// Add handler output if specified
	if routeConfig.Handler.Output != nil {
		for k, v := range routeConfig.Handler.Output {
			response[k] = v
		}
	}

	// Add any data from the handler input
	if routeConfig.Handler.Input != nil {
		for k, v := range routeConfig.Handler.Input {
			response[k] = v
		}
	}

	// Add request data if available
	if ctx.Request.Method() == "POST" || ctx.Request.Method() == "PUT" {
		var body map[string]any
		if err := ctx.Request.BodyParser(&body); err == nil {
			response["request_data"] = body
		}
	}

	return ctx.Request.JSON(response)
}

func (e *JSONEngine) handleAPI(ctx *ExecutionContext, routeConfig RouteConfig) error {
	// Generic API handler - can execute functions, workflows, or return configured data
	target := routeConfig.Handler.Target

	// Check if target is a function
	if function, exists := e.functions[target]; exists {
		result, err := e.executeFunction(ctx, function, routeConfig.Handler.Input)
		if err != nil {
			return ctx.Request.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return ctx.Request.JSON(result)
	}

	// Check if target is a workflow
	if workflow, exists := e.workflows[target]; exists {
		result, err := e.executeWorkflow(ctx, workflow, routeConfig.Handler.Input)
		if err != nil {
			return ctx.Request.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return ctx.Request.JSON(result)
	}

	// Return configured output or input as fallback
	response := make(map[string]any)
	if routeConfig.Handler.Output != nil {
		response = routeConfig.Handler.Output
	} else if routeConfig.Handler.Input != nil {
		response = routeConfig.Handler.Input
	} else {
		response["message"] = "API endpoint: " + target
		response["method"] = ctx.Request.Method()
		response["path"] = ctx.Request.Path()
	}

	return ctx.Request.JSON(response)
}

func (e *JSONEngine) handleGeneric(ctx *ExecutionContext, routeConfig RouteConfig) error {
	// Generic handler for unknown types - maximum flexibility
	log.Printf("Using generic handler for type: %s", routeConfig.Handler.Type)

	response := map[string]any{
		"handler_type": routeConfig.Handler.Type,
		"target":       routeConfig.Handler.Target,
		"method":       ctx.Request.Method(),
		"path":         ctx.Request.Path(),
		"timestamp":    time.Now().Format(time.RFC3339),
	}

	// Add handler output if available
	if routeConfig.Handler.Output != nil {
		response["output"] = routeConfig.Handler.Output
	}

	// Add handler input if available
	if routeConfig.Handler.Input != nil {
		response["input"] = routeConfig.Handler.Input
	}

	// Add request body for POST/PUT requests
	if ctx.Request.Method() == "POST" || ctx.Request.Method() == "PUT" {
		var body map[string]any
		if err := ctx.Request.BodyParser(&body); err == nil {
			response["request_body"] = body
		}
	}

	// Add query parameters
	if len(ctx.Request.Queries()) > 0 {
		response["query_params"] = ctx.Request.Queries()
	}

	// Check if there's a function with this handler type as ID
	if function, exists := e.functions[routeConfig.Handler.Type]; exists {
		result, err := e.executeFunction(ctx, function, response)
		if err != nil {
			response["function_error"] = err.Error()
		} else {
			response["function_result"] = result
		}
	}

	return ctx.Request.JSON(response)
}

// Middleware creators
func (e *JSONEngine) createAuthMiddleware(config MiddlewareConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Simple auth middleware
		if config.Config["skip_paths"] != nil {
			skipPaths := config.Config["skip_paths"].([]any)
			for _, path := range skipPaths {
				if c.Path() == path.(string) {
					return c.Next()
				}
			}
		}
		return c.Next()
	}
}

func (e *JSONEngine) createLoggingMiddleware(config MiddlewareConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		log.Printf("[%s] %s %s - %v", config.Name, c.Method(), c.Path(), time.Since(start))
		return err
	}
}

func (e *JSONEngine) createRateLimitMiddleware(config MiddlewareConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Simple rate limiting placeholder
		return c.Next()
	}
}

func (e *JSONEngine) createCustomMiddleware(config MiddlewareConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Execute custom functions if specified
		for _, functionID := range config.Functions {
			if function, exists := e.functions[functionID]; exists {
				ctx := &ExecutionContext{Request: c}
				_, err := e.executeFunction(ctx, function, nil)
				if err != nil {
					return err
				}
			}
		}
		return c.Next()
	}
}

// Workflow execution using real workflow engine
func (e *JSONEngine) executeWorkflow(ctx *ExecutionContext, workflow *Workflow, input map[string]any) (map[string]any, error) {
	log.Printf("Executing workflow: %s", workflow.ID)

	// Initialize workflow context
	workflowCtx := make(map[string]any)
	for k, v := range input {
		workflowCtx[k] = v
	}
	for k, v := range workflow.Runtime.Variables {
		workflowCtx[k] = v
	}

	// Simple sequential execution
	finalResult := make(map[string]any)
	var lastNodeResult map[string]any

	for _, node := range workflow.Nodes {
		ctx.Node = node
		nodeResult, err := e.executeNode(ctx, node, workflowCtx)
		if err != nil {
			return nil, fmt.Errorf("node %s failed: %v", node.ID, err)
		}

		// Update workflow context with node results
		for k, v := range nodeResult {
			workflowCtx[k] = v
		}

		// Keep track of the last node result
		lastNodeResult = nodeResult

		// Only store the final output from specific result nodes
		if node.Config.Type == "output" || node.ID == "log_result" ||
			node.Config.Function == "log_sms_result" ||
			strings.Contains(node.ID, "log") {
			// Store all results from the final node
			for k, v := range nodeResult {
				finalResult[k] = v
			}
		}
	}

	// If no specific output nodes, use the last node's result
	if len(finalResult) == 0 && lastNodeResult != nil {
		finalResult = lastNodeResult
	}

	// If still no result, return the last meaningful result
	if len(finalResult) == 0 {
		// Return only safe, non-circular data
		finalResult = map[string]any{
			"status":  "completed",
			"message": workflowCtx["result"],
		}

		// Add any simple result values
		for k, v := range workflowCtx {
			switch v.(type) {
			case string, int, int64, float64, bool:
				if k != "input" && k != "ctx" && k != "context" {
					finalResult[k] = v
				}
			}
		}
	}

	return finalResult, nil
}

// sanitizeResult removes circular references and non-serializable data
func (e *JSONEngine) sanitizeResult(input map[string]any) map[string]any {
	// Create a clean result with only the essential workflow output
	result := make(map[string]any)

	// Include all safe fields that don't cause circular references
	for key, value := range input {
		// Skip potentially problematic keys that might contain circular references
		if key == "ctx" || key == "context" || key == "request" || key == "node" || key == "workflow" ||
			key == "functions" || key == "validators" || key == "templates" || key == "Function" ||
			key == "Config" || key == "Compiled" || key == "Handler" || key == "Runtime" ||
			key == "Nodes" || key == "Edges" {
			continue
		}

		// Include the cleaned value
		result[key] = e.cleanValue(value)
	}

	return result
} // cleanValue safely converts values to JSON-serializable types
func (e *JSONEngine) cleanValue(value any) any {
	switch v := value.(type) {
	case string, int, int64, float64, bool, nil:
		return v
	case []any:
		cleanArray := make([]any, 0, len(v))
		for _, item := range v {
			cleanArray = append(cleanArray, e.cleanValue(item))
		}
		return cleanArray
	case map[string]any:
		cleanMap := make(map[string]any)
		for k, val := range v {
			// Only include simple fields in nested maps
			switch val.(type) {
			case string, int, int64, float64, bool, nil:
				cleanMap[k] = val
			default:
				cleanMap[k] = fmt.Sprintf("%v", val)
			}
		}
		return cleanMap
	default:
		// Convert unknown types to strings
		return fmt.Sprintf("%v", v)
	}
}

// Execute individual nodes - simplified implementation for now
func (e *JSONEngine) executeNode(ctx *ExecutionContext, node *Node, input map[string]any) (map[string]any, error) {
	log.Printf("Executing node: %s (type: %s)", node.ID, node.Config.Type)

	switch node.Config.Type {
	case "subworkflow":
		// Execute sub-workflow
		subWorkflowID := node.Config.SubWorkflow
		if subWorkflow, exists := e.workflows[subWorkflowID]; exists {
			log.Printf("Executing sub-workflow: %s", subWorkflowID)

			// Map inputs if specified
			subInput := make(map[string]any)
			if node.Config.InputMapping != nil {
				for sourceKey, targetKey := range node.Config.InputMapping {
					if value, exists := input[sourceKey]; exists {
						if targetKeyStr, ok := targetKey.(string); ok {
							subInput[targetKeyStr] = value
						}
					}
				}
			} else {
				// Use all input if no mapping specified
				subInput = input
			}

			result, err := e.executeWorkflow(ctx, subWorkflow, subInput)
			if err != nil {
				return nil, fmt.Errorf("sub-workflow %s failed: %v", subWorkflowID, err)
			}

			// Map outputs if specified
			if node.Config.OutputMapping != nil {
				mappedResult := make(map[string]any)
				for sourceKey, targetKey := range node.Config.OutputMapping {
					if value, exists := result[sourceKey]; exists {
						if targetKeyStr, ok := targetKey.(string); ok {
							mappedResult[targetKeyStr] = value
						}
					}
				}
				return mappedResult, nil
			}

			return result, nil
		}
		return nil, fmt.Errorf("sub-workflow %s not found", subWorkflowID)

	case "function":
		// Execute function
		if node.Function != nil {
			return e.executeFunction(ctx, node.Function, input)
		}
		return input, nil

	case "condition":
		// Simple condition evaluation
		if condition, exists := node.Config.Config["condition"]; exists {
			conditionStr := fmt.Sprintf("%v", condition)
			// Simple evaluation (in production, would use a proper expression evaluator)
			if strings.Contains(conditionStr, "true") {
				return map[string]any{"result": true}, nil
			}
		}
		return map[string]any{"result": false}, nil

	case "data":
		// Return configured data
		if data, exists := node.Config.Config["data"]; exists {
			return map[string]any{"data": data}, nil
		}
		return input, nil

	default:
		log.Printf("Unknown node type: %s, returning input", node.Config.Type)
		return input, nil
	}
}

// Function execution using the compiled function handlers
func (e *JSONEngine) executeFunction(ctx *ExecutionContext, function *Function, input map[string]any) (map[string]any, error) {
	if function.Handler == nil {
		return nil, fmt.Errorf("function handler not compiled")
	}

	switch handler := function.Handler.(type) {
	case func(*ExecutionContext, map[string]any) (map[string]any, error):
		return handler(ctx, input)
	default:
		return nil, fmt.Errorf("unknown function handler type")
	}
}

// createBuiltinFunction creates handlers for built-in functions
func (e *JSONEngine) createBuiltinFunction(config FunctionConfig) any {
	return func(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
		switch config.Handler {
		case "authenticate":
			// Handle user authentication
			username, _ := input["username"].(string)
			password, _ := input["password"].(string)

			if username == "" || password == "" {
				return map[string]any{
					"success": false,
					"error":   "Username and password required",
				}, nil
			}

			// Generic authentication using user data from configuration
			// Look for users in multiple possible data keys for flexibility
			var users []any

			if demoUsers, ok := e.data["demo_users"].([]any); ok {
				users = demoUsers
			} else if configUsers, ok := e.data["users"].([]any); ok {
				users = configUsers
			} else if authUsers, ok := e.data["auth_users"].([]any); ok {
				users = authUsers
			} else {
				return map[string]any{
					"success": false,
					"error":   "User authentication data not configured",
				}, nil
			}

			for _, userInterface := range users {
				user, ok := userInterface.(map[string]any)
				if !ok {
					continue
				}

				userUsername, _ := user["username"].(string)
				userPassword, _ := user["password"].(string)
				role, _ := user["role"].(string)

				if userUsername == username && userPassword == password {
					// Generate simple token (in production, use JWT)
					token := fmt.Sprintf("token_%s_%d", username, time.Now().Unix())

					return map[string]any{
						"success": true,
						"token":   token,
						"user": map[string]any{
							"username": username,
							"role":     role,
						},
					}, nil
				}
			}

			return map[string]any{
				"success": false,
				"error":   "Invalid credentials",
			}, nil

		case "echo":
			return input, nil
		case "log":
			log.Printf("Builtin log: %+v", input)
			return map[string]any{"logged": true}, nil
		case "validate":
			// Simple validation example
			return map[string]any{"valid": true}, nil
		case "transform":
			// Simple data transformation
			if data, exists := input["data"]; exists {
				return map[string]any{"transformed": data}, nil
			}
			return input, nil
		default:
			return nil, fmt.Errorf("unknown builtin function: %s", config.Handler)
		}
	}
}

// createCustomFunction creates handlers for custom user-defined functions
func (e *JSONEngine) createCustomFunction(config FunctionConfig) any {
	return func(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
		// Execute custom code from config.Code
		if config.Code != "" {
			// For now, just return the configured response or echo input
			if config.Response != nil {
				return config.Response, nil
			}
		}

		// Handle CRUD operations based on config
		if config.Config != nil {
			action, _ := config.Config["action"].(string)
			entity, _ := config.Config["entity"].(string)

			switch action {
			case "create":
				return e.handleCreateEntity(ctx, entity, input)
			case "update":
				return e.handleUpdateEntity(ctx, entity, input)
			case "delete":
				return e.handleDeleteEntity(ctx, entity, input)
			case "get":
				return e.handleGetEntity(ctx, entity, input)
			case "list":
				return e.handleListEntity(ctx, entity, input)
			case "send_campaign":
				return e.handleSendCampaign(ctx, input)
			case "publish":
				return e.handlePublishEntity(ctx, entity, input)
			}
		}

		// Simple key-value transformation based on config
		result := make(map[string]any)
		for k, v := range input {
			result[k] = v
		}

		// Apply any transformations from config
		if config.Config != nil {
			for key, value := range config.Config {
				result[key] = value
			}
		}

		return result, nil
	}
}

// CRUD operation handlers
func (e *JSONEngine) handleCreateEntity(ctx *ExecutionContext, entity string, input map[string]any) (map[string]any, error) {
	switch entity {
	case "employee":
		// Create new employee
		return map[string]any{
			"success": true,
			"message": "Employee created successfully",
			"id":      time.Now().Unix(), // Simple ID generation
			"data":    input,
		}, nil
	case "post":
		// Create new blog post
		return map[string]any{
			"success": true,
			"message": "Blog post created successfully",
			"id":      time.Now().Unix(),
			"data":    input,
		}, nil
	case "email":
		// Create email campaign
		return map[string]any{
			"success": true,
			"message": "Email campaign created successfully",
			"id":      time.Now().Unix(),
			"data":    input,
		}, nil
	default:
		return map[string]any{
			"success": true,
			"message": fmt.Sprintf("%s created successfully", entity),
			"id":      time.Now().Unix(),
			"data":    input,
		}, nil
	}
}

func (e *JSONEngine) handleUpdateEntity(ctx *ExecutionContext, entity string, input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	return map[string]any{
		"success": true,
		"message": fmt.Sprintf("%s updated successfully", entity),
		"id":      id,
		"data":    input,
	}, nil
}

func (e *JSONEngine) handleDeleteEntity(ctx *ExecutionContext, entity string, input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	return map[string]any{
		"success": true,
		"message": fmt.Sprintf("%s deleted successfully", entity),
		"id":      id,
	}, nil
}

func (e *JSONEngine) handleGetEntity(ctx *ExecutionContext, entity string, input map[string]any) (map[string]any, error) {
	// Get entity ID from input
	var entityID string
	if idVal, ok := input["id"]; ok {
		entityID = fmt.Sprintf("%v", idVal)
	}

	if entityID == "" {
		return map[string]any{
			"success": false,
			"error":   entity + " ID is required",
		}, nil
	}

	// Look up entity data from configuration
	entityDataKey := entity + "s" // Assume plural form (employees, posts, etc.)
	if entityData, ok := e.data[entityDataKey]; ok {
		if entityList, ok := entityData.([]any); ok {
			for _, item := range entityList {
				if itemMap, ok := item.(map[string]any); ok {
					if itemIDVal, ok := itemMap["id"]; ok {
						itemIDStr := fmt.Sprintf("%v", itemIDVal)
						if itemIDStr == entityID {
							// Found the entity, return it with all required data
							result := make(map[string]any)

							// Add the entity with singular name
							result[entity] = itemMap

							// Add other required data for the template
							if appTitle, ok := e.data["app_title"]; ok {
								result["app_title"] = appTitle
							}

							// Add any other data that might be needed by templates
							for key, value := range e.data {
								if key != entityDataKey { // Don't duplicate the main entity data
									result[key] = value
								}
							}

							return result, nil
						}
					}
				}
			}
		}
	}

	return map[string]any{
		"success": false,
		"error":   entity + " not found",
	}, nil
}

func (e *JSONEngine) handleListEntity(ctx *ExecutionContext, entity string, input map[string]any) (map[string]any, error) {
	// Look up entity data from configuration using plural form
	entityDataKey := entity + "s" // Assume plural form (employees, posts, etc.)
	if entityData, ok := e.data[entityDataKey]; ok {
		result := map[string]any{
			"success": true,
		}
		result[entityDataKey] = entityData

		// Add other data that might be needed
		for key, value := range e.data {
			if key != entityDataKey {
				result[key] = value
			}
		}

		return result, nil
	}

	// If no data found, return empty result
	return map[string]any{
		"success":    true,
		entity + "s": []any{},
	}, nil
}

func (e *JSONEngine) handleSendCampaign(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
	return map[string]any{
		"success":     true,
		"campaign_id": fmt.Sprintf("campaign_%d", time.Now().Unix()),
		"emails_sent": 10, // Mock value
		"status":      "sent",
	}, nil
}

func (e *JSONEngine) handlePublishEntity(ctx *ExecutionContext, entity string, input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	return map[string]any{
		"success": true,
		"message": fmt.Sprintf("%s published successfully", entity),
		"id":      id,
		"status":  "published",
	}, nil
}

// createGenericFunction creates a generic function handler for unknown types
func (e *JSONEngine) createGenericFunction(config FunctionConfig) any {
	return func(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
		log.Printf("Executing generic function: %s with type: %s", config.ID, config.Type)

		// For unknown function types, we create a flexible handler that:
		// 1. Returns configured response if available
		if config.Response != nil {
			return config.Response, nil
		}

		// 2. Applies any transformations from config
		result := make(map[string]any)
		for k, v := range input {
			result[k] = v
		}

		if config.Config != nil {
			for key, value := range config.Config {
				result[key] = value
			}
		}

		// 3. Add metadata about the function execution
		result["_function_id"] = config.ID
		result["_function_type"] = config.Type
		result["_executed_at"] = time.Now().Format(time.RFC3339)

		return result, nil
	}
}

// handleAuthentication handles user authentication with actual validation
func (e *JSONEngine) handleAuthentication(ctx *ExecutionContext, input map[string]any) (map[string]any, error) {
	username, _ := input["username"].(string)
	password, _ := input["password"].(string)

	if username == "" || password == "" {
		return map[string]any{
			"success": false,
			"error":   "Username and password required",
		}, nil
	}

	// Generic authentication using user data from configuration
	// Look for users in multiple possible data keys for flexibility
	var users []any

	if demoUsers, ok := e.data["demo_users"].([]any); ok {
		users = demoUsers
	} else if configUsers, ok := e.data["users"].([]any); ok {
		users = configUsers
	} else if authUsers, ok := e.data["auth_users"].([]any); ok {
		users = authUsers
	} else {
		return map[string]any{
			"success": false,
			"error":   "User authentication data not configured",
		}, nil
	}

	for _, userInterface := range users {
		user, ok := userInterface.(map[string]any)
		if !ok {
			continue
		}

		userUsername, _ := user["username"].(string)
		userPassword, _ := user["password"].(string)
		role, _ := user["role"].(string)

		if userUsername == username && userPassword == password {
			// Generate simple token (in production, use JWT)
			token := fmt.Sprintf("token_%s_%d", username, time.Now().Unix())

			return map[string]any{
				"success": true,
				"token":   token,
				"user": map[string]any{
					"username": username,
					"role":     role,
				},
			}, nil
		}
	}

	return map[string]any{
		"success": false,
		"error":   "Invalid credentials",
	}, nil
}
