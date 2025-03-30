package dag

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	
	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/form"
	"github.com/oarkflow/json/jsonparser"
	
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
)

// RenderNotFound handles 404 errors.
func renderFiberNotFound(c *fiber.Ctx) error {
	html := `
<div>
	<h1>task not found</h1>
	<p><a href="/process">Back to home</a></p>
</div>
`
	c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
	return c.Status(fiber.StatusNotFound).SendString(html)
}

// Render handles process and request routes.
func (tm *DAG) renderFiber(c *fiber.Ctx) error {
	ctx, data, err := form.ParseBodyAsJSON(c.UserContext(), c.Get("Content-Type"), c.Body(), c.Queries())
	if err != nil {
		return c.Status(fiber.StatusNotFound).SendString(err.Error())
	}
	accept := c.Get("Accept")
	userCtx := form.UserContext(ctx)
	ctx = context.WithValue(ctx, "method", c.Method())
	
	if c.Method() == fiber.MethodGet && userCtx.Get("task_id") != "" {
		manager, ok := tm.taskManager.Get(userCtx.Get("task_id"))
		if !ok || manager == nil {
			if strings.Contains(accept, fiber.MIMETextHTML) || accept == "" {
				return renderFiberNotFound(c)
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": "task not found"})
		}
	}
	
	result := tm.Process(ctx, data)
	if result.Error != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": result.Error.Error()})
	}
	
	contentType := consts.TypeJson
	if ct, ok := result.Ctx.Value(consts.ContentType).(string); ok {
		contentType = ct
	}
	
	switch contentType {
	case consts.TypeHtml:
		htmlContent, err := jsonparser.GetString(result.Payload, "html_content")
		if err != nil {
			return err
		}
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return c.SendString(htmlContent)
	default:
		if c.Method() != fiber.MethodPost {
			return c.Status(fiber.StatusMethodNotAllowed).JSON(fiber.Map{"message": "not allowed"})
		}
		c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
		return c.JSON(result.Payload)
	}
}

// TaskStatusHandler retrieves task statuses.
func (tm *DAG) fiberTaskStatusHandler(c *fiber.Ctx) error {
	taskID := c.Query("taskID")
	if taskID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "taskID is missing"})
	}
	
	manager, ok := tm.taskManager.Get(taskID)
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"message": "Invalid TaskID"})
	}
	
	result := make(map[string]TaskState)
	manager.taskStates.ForEach(func(key string, value *TaskState) bool {
		key = strings.Split(key, Delimiter)[0]
		nodeID := strings.Split(value.NodeID, Delimiter)[0]
		rs := jsonparser.Delete(value.Result.Payload, "html_content")
		status := value.Status
		if status == mq.Processing {
			status = mq.Completed
		}
		state := TaskState{
			NodeID:    nodeID,
			Status:    status,
			UpdatedAt: value.UpdatedAt,
			Result: mq.Result{
				Payload: rs,
				Error:   value.Result.Error,
				Status:  status,
			},
		}
		result[key] = state
		return true
	})
	return c.Type(fiber.MIMEApplicationJSON).JSON(result)
}

func (tm *DAG) BaseURI() string {
	return tm.httpPrefix
}

// Handlers initializes route handlers.
func (tm *DAG) Handlers(app any, prefix string) {
	if prefix != "" && prefix != "/" {
		tm.httpPrefix = prefix
	}
	switch a := app.(type) {
	case fiber.Router:
		a.All("/process", tm.renderFiber)
		a.Get("/request", tm.renderFiber)
		a.Get("/task/status", tm.fiberTaskStatusHandler)
		a.Get("/dot", func(c *fiber.Ctx) error {
			return c.Type(fiber.MIMETextPlain).SendString(tm.ExportDOT())
		})
		a.Get("/", func(c *fiber.Ctx) error {
			image := fmt.Sprintf("%s.svg", mq.NewID())
			defer os.Remove(image)
			
			if err := tm.SaveSVG(image); err != nil {
				return c.Status(fiber.StatusBadRequest).SendString("Failed to read request body")
			}
			
			svgBytes, err := os.ReadFile(image)
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).SendString("Could not read SVG file")
			}
			c.Set(fiber.HeaderContentType, "image/svg+xml")
			return c.Send(svgBytes)
		})
		// <<< NEW FIBER API ENDPOINTS >>>
		a.Get("/metrics", func(c *fiber.Ctx) error {
			return c.JSON(tm.metrics)
		})
		a.Get("/cancel", func(c *fiber.Ctx) error {
			taskID := c.Query("taskID")
			if taskID == "" {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "taskID is missing"})
			}
			err := tm.CancelTask(taskID)
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
			}
			return c.JSON(fiber.Map{"message": "task cancelled successfully"})
		})
	default:
		http.Handle("/notify", tm.SetupWS())
		http.HandleFunc("/process", tm.render)
		http.HandleFunc("/request", tm.render)
		http.HandleFunc("/task/status", tm.taskStatusHandler)
		http.HandleFunc("/dot", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintln(w, tm.ExportDOT())
		})
		// <<< NEW NET/HTTP API ENDPOINTS >>>
		http.HandleFunc("/metrics", tm.metricsHandler)
		http.HandleFunc("/cancel", tm.cancelTaskHandler)
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			image := fmt.Sprintf("%s.svg", mq.NewID())
			err := tm.SaveSVG(image)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}
			defer os.Remove(image)
			svgBytes, err := os.ReadFile(image)
			if err != nil {
				http.Error(w, "Could not read SVG file", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "image/svg+xml")
			if _, err := w.Write(svgBytes); err != nil {
				http.Error(w, "Could not write SVG response", http.StatusInternalServerError)
				return
			}
		})
	}
}
