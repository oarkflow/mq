package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	htmlTemplate "text/template"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

// NewJSONEngine creates a new JSON-driven workflow engine
func NewJSONEngine() *JSONEngine {
	return &JSONEngine{
		templates:  make(map[string]*Template),
		workflows:  make(map[string]*Workflow),
		functions:  make(map[string]*Function),
		validators: make(map[string]*Validator),
		middleware: make(map[string]*Middleware),
		data:       make(map[string]interface{}),
	}
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

	// 6. Store global data
	e.data = e.config.Data

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

		// Create template
		tmpl, err := htmlTemplate.New(id).Parse(templateConfig.Template)
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

// compileFunctions compiles all function definitions
func (e *JSONEngine) compileFunctions() error {
	for id, functionConfig := range e.config.Functions {
		log.Printf("Compiling function: %s", id)

		function := &Function{
			ID:     id,
			Config: functionConfig,
		}

		// Compile function based on type
		switch functionConfig.Type {
		case "http":
			function.Handler = e.createHTTPFunction(functionConfig)
		case "expression":
			function.Handler = e.createExpressionFunction(functionConfig)
		case "template":
			function.Handler = e.createTemplateFunction(functionConfig)
		case "js":
			function.Handler = e.createJSFunction(functionConfig)
		default:
			return fmt.Errorf("unknown function type: %s", functionConfig.Type)
		}

		e.functions[id] = function
	}
	return nil
}

// compileValidators compiles all validators
func (e *JSONEngine) compileValidators() error {
	for id, validatorConfig := range e.config.Validators {
		log.Printf("Compiling validator: %s", id)

		e.validators[id] = &Validator{
			ID:     id,
			Config: validatorConfig,
			Rules:  validatorConfig.Rules,
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
				Context:   make(map[string]interface{}),
				Variables: workflowConfig.Variables,
				Status:    "ready",
			},
		}

		// Compile nodes
		for _, nodeConfig := range workflowConfig.Nodes {
			node := &Node{
				ID:      nodeConfig.ID,
				Config:  nodeConfig,
				Inputs:  make(map[string]interface{}),
				Outputs: make(map[string]interface{}),
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
		log.Printf("Applied middleware: %s (priority: %d)", middleware.Config.Name, middleware.Config.Priority)
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

		// Create execution context
		ctx := &ExecutionContext{
			Request:    c,
			Data:       make(map[string]interface{}),
			Variables:  make(map[string]interface{}),
			Session:    make(map[string]interface{}),
			User:       make(map[string]interface{}),
			Functions:  e.functions,
			Validators: e.validators,
		}

		// Apply route middleware - skip auth middleware as we handle it below
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

		// Execute handler based on type
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
		default:
			return c.Status(500).JSON(fiber.Map{"error": "Unknown handler type: " + routeConfig.Handler.Type})
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
	data := make(map[string]interface{})

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

	// Add request data
	data["request"] = map[string]interface{}{
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
	if err := tmpl.Execute(&buf, data); err != nil {
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
		return ctx.Request.JSON(result)
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

	return ctx.Request.JSON(result)
}

func (e *JSONEngine) handleFunction(ctx *ExecutionContext, routeConfig RouteConfig) error {
	functionID := routeConfig.Handler.Target

	// Handle special built-in functions
	switch functionID {
	case "authenticate_user":
		return e.handleAuthFunction(ctx)
	default:
		function, exists := e.functions[functionID]
		if !exists {
			return ctx.Request.Status(404).JSON(fiber.Map{"error": "Function not found: " + functionID})
		}

		// Execute function
		result, err := e.executeFunction(ctx, function, routeConfig.Handler.Input)
		if err != nil {
			return ctx.Request.Status(500).JSON(fiber.Map{"error": "Function execution failed: " + err.Error()})
		}

		return ctx.Request.JSON(result)
	}
}

func (e *JSONEngine) handleStatic(ctx *ExecutionContext, routeConfig RouteConfig) error {
	// Serve static content
	return ctx.Request.SendFile(routeConfig.Handler.Target)
}

// handleAuthFunction handles user authentication
func (e *JSONEngine) handleAuthFunction(ctx *ExecutionContext) error {
	var credentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := ctx.Request.BodyParser(&credentials); err != nil {
		return ctx.Request.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Simple authentication using demo users from data
	demoUsers, ok := e.data["demo_users"].([]interface{})
	if !ok {
		return ctx.Request.Status(500).JSON(fiber.Map{"error": "User data not configured"})
	}

	for _, userInterface := range demoUsers {
		user, ok := userInterface.(map[string]interface{})
		if !ok {
			continue
		}

		username, _ := user["username"].(string)
		password, _ := user["password"].(string)
		role, _ := user["role"].(string)

		if username == credentials.Username && password == credentials.Password {
			// Generate simple token (in production, use JWT)
			token := fmt.Sprintf("token_%s_%d", username, time.Now().Unix())

			return ctx.Request.JSON(fiber.Map{
				"success": true,
				"token":   token,
				"user": map[string]interface{}{
					"username": username,
					"role":     role,
				},
			})
		}
	}

	return ctx.Request.Status(401).JSON(fiber.Map{
		"success": false,
		"error":   "Invalid credentials",
	})
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
			var body map[string]interface{}
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
		ctx.User = map[string]interface{}{
			"id":       "user_" + token,
			"username": "demo_user",
			"role":     "user",
		}
	}

	return nil
}

// Function executors
func (e *JSONEngine) createHTTPFunction(config FunctionConfig) interface{} {
	return func(ctx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
		client := &http.Client{Timeout: 30 * time.Second}

		method := config.Method
		if method == "" {
			method = "GET"
		}

		req, err := http.NewRequest(method, config.URL, nil)
		if err != nil {
			return nil, err
		}

		// Add headers
		for k, v := range config.Headers {
			req.Header.Set(k, v)
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

		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			// If not JSON, return as string
			result = map[string]interface{}{
				"status": resp.StatusCode,
				"body":   string(body),
			}
		}

		return result, nil
	}
}

func (e *JSONEngine) createExpressionFunction(config FunctionConfig) interface{} {
	return func(ctx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
		// Simple expression evaluation (would use a proper expression engine in production)
		result := map[string]interface{}{
			"result": config.Code, // Placeholder
			"input":  input,
		}
		return result, nil
	}
}

func (e *JSONEngine) createTemplateFunction(config FunctionConfig) interface{} {
	return func(ctx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
		tmpl, err := htmlTemplate.New("function").Parse(config.Code)
		if err != nil {
			return nil, err
		}

		var buf strings.Builder
		if err := tmpl.Execute(&buf, input); err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"result": buf.String(),
		}, nil
	}
}

func (e *JSONEngine) createJSFunction(config FunctionConfig) interface{} {
	return func(ctx *ExecutionContext, input map[string]interface{}) (map[string]interface{}, error) {
		// Placeholder for JavaScript execution (would use goja or similar in production)
		return map[string]interface{}{
			"result": "JavaScript execution not implemented in demo",
			"code":   config.Code,
			"input":  input,
		}, nil
	}
}

// Middleware creators
func (e *JSONEngine) createAuthMiddleware(config MiddlewareConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Simple auth middleware
		if config.Config["skip_paths"] != nil {
			skipPaths := config.Config["skip_paths"].([]interface{})
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

// Workflow execution
func (e *JSONEngine) executeWorkflow(ctx *ExecutionContext, workflow *Workflow, input map[string]interface{}) (map[string]interface{}, error) {
	ctx.Workflow = workflow

	// Initialize workflow context
	workflowCtx := make(map[string]interface{})
	for k, v := range input {
		workflowCtx[k] = v
	}
	for k, v := range workflow.Runtime.Variables {
		workflowCtx[k] = v
	}

	// Simple sequential execution (in production, would handle parallel execution, conditions, etc.)
	result := make(map[string]interface{})

	for _, node := range workflow.Nodes {
		ctx.Node = node
		nodeResult, err := e.executeNode(ctx, node, workflowCtx)
		if err != nil {
			return nil, fmt.Errorf("node %s failed: %v", node.ID, err)
		}

		// Merge results
		for k, v := range nodeResult {
			result[k] = v
			workflowCtx[k] = v
		}
	}

	return result, nil
}

func (e *JSONEngine) executeNode(ctx *ExecutionContext, node *Node, input map[string]interface{}) (map[string]interface{}, error) {
	switch node.Config.Type {
	case "function":
		if node.Function != nil {
			return e.executeFunction(ctx, node.Function, input)
		}
		return map[string]interface{}{}, nil

	case "http":
		url := node.Config.Config["url"].(string)
		method := "GET"
		if m, ok := node.Config.Config["method"].(string); ok {
			method = m
		}

		client := &http.Client{Timeout: 30 * time.Second}
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			return nil, err
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

		return map[string]interface{}{
			"status": resp.StatusCode,
			"body":   string(body),
		}, nil

	case "template":
		templateID := node.Config.Config["template"].(string)
		template, exists := e.templates[templateID]
		if !exists {
			return nil, fmt.Errorf("template not found: %s", templateID)
		}

		tmpl := template.Compiled.(*htmlTemplate.Template)
		var buf strings.Builder
		if err := tmpl.Execute(&buf, input); err != nil {
			return nil, err
		}

		return map[string]interface{}{
			"output": buf.String(),
		}, nil

	case "validator":
		validatorID := node.Config.Config["validator"].(string)
		validator, exists := e.validators[validatorID]
		if !exists {
			return nil, fmt.Errorf("validator not found: %s", validatorID)
		}

		// Simple validation
		field := validator.Config.Field
		if value, ok := input[field]; ok {
			for _, rule := range validator.Rules {
				if rule.Type == "required" && value == nil {
					return nil, fmt.Errorf("validation failed: %s", rule.Message)
				}
			}
		}

		return map[string]interface{}{
			"valid": true,
		}, nil

	default:
		return map[string]interface{}{
			"node_type": node.Config.Type,
			"input":     input,
		}, nil
	}
}

func (e *JSONEngine) executeFunction(ctx *ExecutionContext, function *Function, input map[string]interface{}) (map[string]interface{}, error) {
	if function.Handler == nil {
		return nil, fmt.Errorf("function handler not compiled")
	}

	switch handler := function.Handler.(type) {
	case func(*ExecutionContext, map[string]interface{}) (map[string]interface{}, error):
		return handler(ctx, input)
	default:
		return nil, fmt.Errorf("unknown function handler type")
	}
}
