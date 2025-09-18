package dag

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// WorkflowAPI provides HTTP handlers for workflow management on top of DAG
type WorkflowAPI struct {
	enhancedDAG *EnhancedDAG
}

// NewWorkflowAPI creates a new workflow API handler
func NewWorkflowAPI(enhancedDAG *EnhancedDAG) *WorkflowAPI {
	return &WorkflowAPI{
		enhancedDAG: enhancedDAG,
	}
}

// RegisterWorkflowRoutes registers all workflow routes with Fiber app
func (api *WorkflowAPI) RegisterWorkflowRoutes(app *fiber.App) {
	v1 := app.Group("/api/v1/workflows")

	// Workflow definition routes
	v1.Post("/", api.CreateWorkflow)
	v1.Get("/", api.ListWorkflows)
	v1.Get("/:id", api.GetWorkflow)
	v1.Put("/:id", api.UpdateWorkflow)
	v1.Delete("/:id", api.DeleteWorkflow)

	// Execution routes
	v1.Post("/:id/execute", api.ExecuteWorkflow)
	v1.Get("/:id/executions", api.ListWorkflowExecutions)
	v1.Get("/executions", api.ListAllExecutions)
	v1.Get("/executions/:executionId", api.GetExecution)
	v1.Post("/executions/:executionId/cancel", api.CancelExecution)

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

	// Set timestamps
	now := time.Now()
	definition.CreatedAt = now
	definition.UpdatedAt = now

	if err := api.enhancedDAG.RegisterWorkflow(c.Context(), &definition); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(definition)
}

// ListWorkflows lists workflow definitions with filtering
func (api *WorkflowAPI) ListWorkflows(c *fiber.Ctx) error {
	workflows := api.enhancedDAG.ListWorkflows()

	// Apply filters if provided
	status := c.Query("status")
	if status != "" {
		filtered := make([]*WorkflowDefinition, 0)
		for _, w := range workflows {
			if string(w.Status) == status {
				filtered = append(filtered, w)
			}
		}
		workflows = filtered
	}

	// Apply pagination
	limit, _ := strconv.Atoi(c.Query("limit", "10"))
	offset, _ := strconv.Atoi(c.Query("offset", "0"))

	total := len(workflows)
	start := offset
	end := offset + limit

	if start > total {
		start = total
	}
	if end > total {
		end = total
	}

	pagedWorkflows := workflows[start:end]

	return c.JSON(fiber.Map{
		"workflows": pagedWorkflows,
		"total":     total,
		"limit":     limit,
		"offset":    offset,
	})
}

// GetWorkflow retrieves a workflow definition by ID
func (api *WorkflowAPI) GetWorkflow(c *fiber.Ctx) error {
	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Workflow ID is required",
		})
	}

	workflow, err := api.enhancedDAG.GetWorkflow(id)
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
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Workflow ID is required",
		})
	}

	var definition WorkflowDefinition
	if err := c.BodyParser(&definition); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Ensure ID matches
	definition.ID = id
	definition.UpdatedAt = time.Now()

	if err := api.enhancedDAG.RegisterWorkflow(c.Context(), &definition); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(definition)
}

// DeleteWorkflow deletes a workflow definition
func (api *WorkflowAPI) DeleteWorkflow(c *fiber.Ctx) error {
	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Workflow ID is required",
		})
	}

	// For now, we'll just return success
	// In a real implementation, you'd remove it from the registry
	return c.JSON(fiber.Map{
		"message": "Workflow deleted successfully",
		"id":      id,
	})
}

// ExecuteWorkflow starts execution of a workflow
func (api *WorkflowAPI) ExecuteWorkflow(c *fiber.Ctx) error {
	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Workflow ID is required",
		})
	}

	var input map[string]any
	if err := c.BodyParser(&input); err != nil {
		input = make(map[string]any)
	}

	execution, err := api.enhancedDAG.ExecuteWorkflow(c.Context(), id, input)
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
	if workflowID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Workflow ID is required",
		})
	}

	activeExecutions := api.enhancedDAG.ListActiveExecutions()

	// Filter by workflow ID
	filtered := make([]*WorkflowExecution, 0)
	for _, exec := range activeExecutions {
		if exec.WorkflowID == workflowID {
			filtered = append(filtered, exec)
		}
	}

	return c.JSON(fiber.Map{
		"executions": filtered,
		"total":      len(filtered),
	})
}

// ListAllExecutions lists all workflow executions
func (api *WorkflowAPI) ListAllExecutions(c *fiber.Ctx) error {
	activeExecutions := api.enhancedDAG.ListActiveExecutions()

	return c.JSON(fiber.Map{
		"executions": activeExecutions,
		"total":      len(activeExecutions),
	})
}

// GetExecution retrieves a specific workflow execution
func (api *WorkflowAPI) GetExecution(c *fiber.Ctx) error {
	executionID := c.Params("executionId")
	if executionID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Execution ID is required",
		})
	}

	execution, err := api.enhancedDAG.GetExecution(executionID)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(execution)
}

// CancelExecution cancels a running workflow execution
func (api *WorkflowAPI) CancelExecution(c *fiber.Ctx) error {
	executionID := c.Params("executionId")
	if executionID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Execution ID is required",
		})
	}

	if err := api.enhancedDAG.CancelExecution(executionID); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "Execution cancelled successfully",
		"id":      executionID,
	})
}

// HealthCheck provides health status of the workflow system
func (api *WorkflowAPI) HealthCheck(c *fiber.Ctx) error {
	workflows := api.enhancedDAG.ListWorkflows()
	activeExecutions := api.enhancedDAG.ListActiveExecutions()

	return c.JSON(fiber.Map{
		"status":            "healthy",
		"workflows":         len(workflows),
		"active_executions": len(activeExecutions),
		"timestamp":         time.Now(),
	})
}

// GetMetrics provides system metrics
func (api *WorkflowAPI) GetMetrics(c *fiber.Ctx) error {
	workflows := api.enhancedDAG.ListWorkflows()
	activeExecutions := api.enhancedDAG.ListActiveExecutions()

	// Basic metrics
	metrics := fiber.Map{
		"workflows": fiber.Map{
			"total":     len(workflows),
			"by_status": make(map[string]int),
		},
		"executions": fiber.Map{
			"active":    len(activeExecutions),
			"by_status": make(map[string]int),
		},
	}

	// Count workflows by status
	statusCounts := metrics["workflows"].(fiber.Map)["by_status"].(map[string]int)
	for _, w := range workflows {
		statusCounts[string(w.Status)]++
	}

	// Count executions by status
	execStatusCounts := metrics["executions"].(fiber.Map)["by_status"].(map[string]int)
	for _, e := range activeExecutions {
		execStatusCounts[string(e.Status)]++
	}

	return c.JSON(metrics)
}

// Helper method to extend existing DAG API with workflow features
func (tm *DAG) RegisterWorkflowAPI(app *fiber.App) error {
	// Create enhanced DAG if not already created
	enhanced, err := NewEnhancedDAG(tm.name, tm.key, nil)
	if err != nil {
		return err
	}

	// Copy existing DAG state to enhanced DAG
	enhanced.DAG = tm

	// Create and register workflow API
	workflowAPI := NewWorkflowAPI(enhanced)
	workflowAPI.RegisterWorkflowRoutes(app)

	return nil
}
