package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/errors"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/utils"

	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/jsonparser"
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
	ctx, data, err := parseRequest(c)
	if err != nil {
		return c.Status(fiber.StatusNotFound).SendString(err.Error())
	}
	accept := c.Get("Accept")
	userCtx := UserContext(ctx)
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

// Handlers initializes route handlers.
func (tm *DAG) Handlers(app any) {
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
	default:
		http.Handle("/notify", tm.SetupWS())
		http.HandleFunc("/process", tm.render)
		http.HandleFunc("/request", tm.render)
		http.HandleFunc("/task/status", tm.taskStatusHandler)
		http.HandleFunc("/dot", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintln(w, tm.ExportDOT())
		})
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

// parseRequest handles Fiber requests and extracts context and JSON payload.
func parseRequest(c *fiber.Ctx) (context.Context, []byte, error) {
	ctx := c.UserContext()
	userContext := &Context{Query: make(map[string]any)}
	queryParams := c.Queries()
	for key, value := range queryParams {
		userContext.Query[key] = value
	}
	ctx = context.WithValue(ctx, "UserContext", userContext)

	contentType := c.Get("Content-Type")
	var result any

	switch {
	case strings.Contains(contentType, fiber.MIMEApplicationJSON):
		body := c.Body()
		if len(body) == 0 {
			return ctx, nil, nil
		}
		var temp any
		if err := json.Unmarshal(body, &temp); err != nil {
			return ctx, nil, fmt.Errorf("failed to parse body: %v", err)
		}
		switch v := temp.(type) {
		case map[string]any:
			result = v
		case []any:
			parsedArray := make([]map[string]any, len(v))
			for i, item := range v {
				obj, ok := item.(map[string]any)
				if !ok {
					return ctx, nil, fmt.Errorf("invalid JSON array item at index %d", i)
				}
				parsedArray[i] = obj
			}
			result = parsedArray
		default:
			return ctx, nil, fmt.Errorf("unsupported JSON structure: %T", v)
		}
	case strings.Contains(contentType, fiber.MIMEApplicationForm):
		body := c.BodyRaw()
		if body == nil {
			return ctx, nil, errors.New("empty form body")
		}
		val, err := utils.DecodeForm(body)
		if err != nil {
			return ctx, nil, fmt.Errorf("failed to parse form data: %v", err.Error())
		}
		for key, v := range val {
			userContext.Query[key] = v
		}
		result = userContext.Query
	default:
		return ctx, nil, nil
	}

	bt, err := json.Marshal(result)
	if err != nil {
		return ctx, nil, err
	}

	return ctx, bt, nil
}
