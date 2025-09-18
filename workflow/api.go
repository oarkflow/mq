package workflow

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// WorkflowAPI provides HTTP handlers for workflow management
type WorkflowAPI struct {
	engine *WorkflowEngine
}

// NewWorkflowAPI creates a new workflow API handler
func NewWorkflowAPI(engine *WorkflowEngine) *WorkflowAPI {
	return &WorkflowAPI{
		engine: engine,
	}
}

// RegisterRoutes registers all workflow routes with Fiber app
func (api *WorkflowAPI) RegisterRoutes(app *fiber.App) {
	v1 := app.Group("/api/v1/workflows")

	// Workflow definition routes
	v1.Post("/", api.CreateWorkflow)
	v1.Get("/", api.ListWorkflows)
	v1.Get("/:id", api.GetWorkflow)
	v1.Put("/:id", api.UpdateWorkflow)
	v1.Delete("/:id", api.DeleteWorkflow)
	v1.Get("/:id/versions", api.GetWorkflowVersions)

	// Execution routes
	v1.Post("/:id/execute", api.ExecuteWorkflow)
	v1.Get("/:id/executions", api.ListWorkflowExecutions)
	v1.Get("/executions", api.ListAllExecutions)
	v1.Get("/executions/:executionId", api.GetExecution)
	v1.Post("/executions/:executionId/cancel", api.CancelExecution)
	v1.Post("/executions/:executionId/suspend", api.SuspendExecution)
	v1.Post("/executions/:executionId/resume", api.ResumeExecution)

	// Management routes
	v1.Get("/health", api.HealthCheck)
	v1.Get("/metrics", api.GetMetrics)
}

// CreateWorkflow creates a new workflow definition
func (api *WorkflowAPI) CreateWorkflow(c *fiber.Ctx) error {
	var definition WorkflowDefinition
	if err := c.BodyParser(&definition); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Set ID if not provided
	if definition.ID == "" {
		definition.ID = uuid.New().String()
	}

	// Set version if not provided
	if definition.Version == "" {
		definition.Version = "1.0.0"
	}

	if err := api.engine.RegisterWorkflow(c.Context(), &definition); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(definition)
}

// ListWorkflows lists workflow definitions with filtering
func (api *WorkflowAPI) ListWorkflows(c *fiber.Ctx) error {
	filter := &WorkflowFilter{
		Limit:  10,
		Offset: 0,
	}

	// Parse query parameters
	if limit := c.Query("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			filter.Limit = l
		}
	}

	if offset := c.Query("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil {
			filter.Offset = o
		}
	}

	if status := c.Query("status"); status != "" {
		filter.Status = []WorkflowStatus{WorkflowStatus(status)}
	}

	if category := c.Query("category"); category != "" {
		filter.Category = []string{category}
	}

	if owner := c.Query("owner"); owner != "" {
		filter.Owner = []string{owner}
	}

	if search := c.Query("search"); search != "" {
		filter.Search = search
	}

	if sortBy := c.Query("sort_by"); sortBy != "" {
		filter.SortBy = sortBy
	}

	if sortOrder := c.Query("sort_order"); sortOrder != "" {
		filter.SortOrder = sortOrder
	}

	workflows, err := api.engine.ListWorkflows(c.Context(), filter)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"workflows": workflows,
		"total":     len(workflows),
		"limit":     filter.Limit,
		"offset":    filter.Offset,
	})
}

// GetWorkflow retrieves a specific workflow definition
func (api *WorkflowAPI) GetWorkflow(c *fiber.Ctx) error {
	id := c.Params("id")
	version := c.Query("version")

	workflow, err := api.engine.GetWorkflow(c.Context(), id, version)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(workflow)
}

// UpdateWorkflow updates an existing workflow definition
func (api *WorkflowAPI) UpdateWorkflow(c *fiber.Ctx) error {
	id := c.Params("id")

	var definition WorkflowDefinition
	if err := c.BodyParser(&definition); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Ensure ID matches
	definition.ID = id

	if err := api.engine.RegisterWorkflow(c.Context(), &definition); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(definition)
}

// DeleteWorkflow removes a workflow definition
func (api *WorkflowAPI) DeleteWorkflow(c *fiber.Ctx) error {
	id := c.Params("id")

	if err := api.engine.DeleteWorkflow(c.Context(), id); err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusNoContent).Send(nil)
}

// GetWorkflowVersions retrieves all versions of a workflow
func (api *WorkflowAPI) GetWorkflowVersions(c *fiber.Ctx) error {
	id := c.Params("id")

	versions, err := api.engine.registry.GetVersions(c.Context(), id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"workflow_id": id,
		"versions":    versions,
	})
}

// ExecuteWorkflow starts workflow execution
func (api *WorkflowAPI) ExecuteWorkflow(c *fiber.Ctx) error {
	id := c.Params("id")

	var request struct {
		Input           map[string]interface{} `json:"input"`
		Priority        Priority               `json:"priority"`
		Owner           string                 `json:"owner"`
		TriggeredBy     string                 `json:"triggered_by"`
		ParentExecution string                 `json:"parent_execution"`
		Delay           int                    `json:"delay"` // seconds
	}

	if err := c.BodyParser(&request); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	options := &ExecutionOptions{
		Priority:        request.Priority,
		Owner:           request.Owner,
		TriggeredBy:     request.TriggeredBy,
		ParentExecution: request.ParentExecution,
		Delay:           time.Duration(request.Delay) * time.Second,
	}

	execution, err := api.engine.ExecuteWorkflow(c.Context(), id, request.Input, options)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(execution)
}

// ListWorkflowExecutions lists executions for a specific workflow
func (api *WorkflowAPI) ListWorkflowExecutions(c *fiber.Ctx) error {
	workflowID := c.Params("id")

	filter := &ExecutionFilter{
		WorkflowID: []string{workflowID},
		Limit:      10,
		Offset:     0,
	}

	// Parse query parameters
	if limit := c.Query("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			filter.Limit = l
		}
	}

	if offset := c.Query("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil {
			filter.Offset = o
		}
	}

	if status := c.Query("status"); status != "" {
		filter.Status = []ExecutionStatus{ExecutionStatus(status)}
	}

	executions, err := api.engine.ListExecutions(c.Context(), filter)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"executions": executions,
		"total":      len(executions),
		"limit":      filter.Limit,
		"offset":     filter.Offset,
	})
}

// ListAllExecutions lists all executions with filtering
func (api *WorkflowAPI) ListAllExecutions(c *fiber.Ctx) error {
	filter := &ExecutionFilter{
		Limit:  10,
		Offset: 0,
	}

	// Parse query parameters
	if limit := c.Query("limit"); limit != "" {
		if l, err := strconv.Atoi(limit); err == nil {
			filter.Limit = l
		}
	}

	if offset := c.Query("offset"); offset != "" {
		if o, err := strconv.Atoi(offset); err == nil {
			filter.Offset = o
		}
	}

	if status := c.Query("status"); status != "" {
		filter.Status = []ExecutionStatus{ExecutionStatus(status)}
	}

	if owner := c.Query("owner"); owner != "" {
		filter.Owner = []string{owner}
	}

	if priority := c.Query("priority"); priority != "" {
		filter.Priority = []Priority{Priority(priority)}
	}

	executions, err := api.engine.ListExecutions(c.Context(), filter)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"executions": executions,
		"total":      len(executions),
		"limit":      filter.Limit,
		"offset":     filter.Offset,
	})
}

// GetExecution retrieves a specific execution
func (api *WorkflowAPI) GetExecution(c *fiber.Ctx) error {
	executionID := c.Params("executionId")

	execution, err := api.engine.GetExecution(c.Context(), executionID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(execution)
}

// CancelExecution cancels a running execution
func (api *WorkflowAPI) CancelExecution(c *fiber.Ctx) error {
	executionID := c.Params("executionId")

	if err := api.engine.CancelExecution(c.Context(), executionID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "Execution cancelled",
	})
}

// SuspendExecution suspends a running execution
func (api *WorkflowAPI) SuspendExecution(c *fiber.Ctx) error {
	executionID := c.Params("executionId")

	if err := api.engine.SuspendExecution(c.Context(), executionID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "Execution suspended",
	})
}

// ResumeExecution resumes a suspended execution
func (api *WorkflowAPI) ResumeExecution(c *fiber.Ctx) error {
	executionID := c.Params("executionId")

	if err := api.engine.ResumeExecution(c.Context(), executionID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "Execution resumed",
	})
}

// HealthCheck returns the health status of the workflow engine
func (api *WorkflowAPI) HealthCheck(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "healthy",
		"timestamp": time.Now(),
		"version":   "1.0.0",
	})
}

// GetMetrics returns workflow engine metrics
func (api *WorkflowAPI) GetMetrics(c *fiber.Ctx) error {
	// In a real implementation, collect actual metrics
	metrics := map[string]interface{}{
		"total_workflows":        0,
		"total_executions":       0,
		"running_executions":     0,
		"completed_executions":   0,
		"failed_executions":      0,
		"average_execution_time": "0s",
		"uptime":                 "0s",
		"memory_usage":           "0MB",
		"cpu_usage":              "0%",
	}

	return c.JSON(metrics)
}

// Error handling middleware
func ErrorHandler(c *fiber.Ctx, err error) error {
	code := fiber.StatusInternalServerError

	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
	}

	return c.Status(code).JSON(fiber.Map{
		"error":     true,
		"message":   err.Error(),
		"timestamp": time.Now(),
	})
}

// CORS middleware configuration
func CORSConfig() fiber.Config {
	return fiber.Config{
		ErrorHandler: ErrorHandler,
	}
}
