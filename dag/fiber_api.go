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
			err = nil
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).SendString("Could not read SVG file")
			}

			// Create HTML page with SVG and Execute Pipeline link
			htmlContent := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
    <title>DAG Pipeline - %s</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
            color: white;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        .header p {
            font-size: 1.2em;
            opacity: 0.9;
        }
        .svg-container {
            background: white;
            border-radius: 15px;
            padding: 30px;
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.3);
            margin-bottom: 30px;
            max-width: 100%%;
            overflow: auto;
            transform-origin: center;
            transform: scale(1);
            position: relative; /* Added for positioning child elements */
        }
        .zoom-controls {
            position: fixed; /* Changed from absolute to fixed */
            bottom: 10px;
            right: 10px;
            display: flex;
            align-items: center;
            gap: 10px;
            background: rgba(255, 255, 255, 0.8);
            padding: 10px;
            border-radius: 8px;
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
        }
        .zoom-controls button {
            background: none;
            border: none;
            cursor: pointer;
            font-size: 20px;
            color: #333;
        }
        .zoom-controls input[type="range"] {
            width: 100px;
        }
        .actions {
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            justify-content: center;
        }
        .btn {
            background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 18px;
            font-weight: bold;
            text-decoration: none;
            display: inline-block;
            transition: all 0.3s ease;
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .btn:hover {
            transform: translateY(-3px);
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.4);
        }
        .btn-secondary {
            background: linear-gradient(45deg, #4ECDC4, #44A08D);
        }
        .info {
            background: rgba(255, 255, 255, 0.1);
            padding: 20px;
            border-radius: 12px;
            margin-top: 20px;
            color: white;
            text-align: center;
            max-width: 600px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🔄 DAG Pipeline</h1>
        <p>%s - Workflow Visualization</p>
    </div>

    <div class="svg-container">
        %s
        <div class="zoom-controls">
            <button onclick="zoomOut()">➖</button>
            <input type="range" min="0.1" max="2" step="0.1" value="1" onchange="setZoom(this.value)">
            <button onclick="zoomIn()">➕</button>
        </div>
    </div>

    <div class="actions">
        <a href="/process" class="btn">🚀 Execute Pipeline</a>
        <a href="/metrics" class="btn btn-secondary">📊 View Metrics</a>
        <a href="/dot" class="btn btn-secondary">📝 Export DOT</a>
    </div>

    <div class="info">
        <p><strong>💡 Ready to Execute:</strong> Click "Execute Pipeline" to start processing your workflow. The system will guide you through each step of the pipeline.</p>
    </div>

    <script>
        function setZoom(value) {
            const svgElement = document.querySelector('.svg-container svg');
            svgElement.style.transform = 'scale(' + value + ')';
        }

        function zoomIn() {
            const slider = document.querySelector('.zoom-controls input[type="range"]');
            let value = parseFloat(slider.value);
            if (value < 2) {
                value += 0.1;
                slider.value = value.toFixed(1);
                setZoom(value);
            }
        }

        function zoomOut() {
            const slider = document.querySelector('.zoom-controls input[type="range"]');
            let value = parseFloat(slider.value);
            if (value > 0.1) {
                value -= 0.1;
                slider.value = value.toFixed(1);
                setZoom(value);
            }
        }
    </script>
</body>
</html>`, tm.name, tm.name, string(svgBytes))

			c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
			return c.SendString(htmlContent)
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

			// Create HTML page with SVG and Execute Pipeline link
			htmlContent := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
    <title>DAG Pipeline - %s</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
            color: white;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        .header p {
            font-size: 1.2em;
            opacity: 0.9;
        }
        .svg-container {
            background: white;
            border-radius: 15px;
            padding: 30px;
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.3);
            margin-bottom: 30px;
            max-width: 100%%;
            overflow: auto;
            transform-origin: center;
            transform: scale(1);
            position: relative; /* Added for positioning child elements */
        }
        .zoom-controls {
            position: fixed; /* Changed from absolute to fixed */
            bottom: 10px;
            right: 10px;
            display: flex;
            align-items: center;
            gap: 10px;
            background: rgba(255, 255, 255, 0.8);
            padding: 10px;
            border-radius: 8px;
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
        }
        .zoom-controls button {
            background: none;
            border: none;
            cursor: pointer;
            font-size: 20px;
            color: #333;
        }
        .zoom-controls input[type="range"] {
            width: 100px;
        }
        .actions {
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            justify-content: center;
        }
        .btn {
            background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 18px;
            font-weight: bold;
            text-decoration: none;
            display: inline-block;
            transition: all 0.3s ease;
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .btn:hover {
            transform: translateY(-3px);
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.4);
        }
        .btn-secondary {
            background: linear-gradient(45deg, #4ECDC4, #44A08D);
        }
        .info {
            background: rgba(255, 255, 255, 0.1);
            padding: 20px;
            border-radius: 12px;
            margin-top: 20px;
            color: white;
            text-align: center;
            max-width: 600px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🔄 DAG Pipeline</h1>
        <p>%s - Workflow Visualization</p>
    </div>

    <div class="svg-container">
        %s
        <div class="zoom-controls">
            <button onclick="zoomOut()">➖</button>
            <input type="range" min="0.1" max="2" step="0.1" value="1" onchange="setZoom(this.value)">
            <button onclick="zoomIn()">➕</button>
        </div>
    </div>

    <div class="actions">
        <a href="/process" class="btn">🚀 Execute Pipeline</a>
        <a href="/metrics" class="btn btn-secondary">📊 View Metrics</a>
        <a href="/dot" class="btn btn-secondary">📝 Export DOT</a>
    </div>

    <div class="info">
        <p><strong>💡 Ready to Execute:</strong> Click "Execute Pipeline" to start processing your workflow. The system will guide you through each step of the pipeline.</p>
    </div>

    <script>
        function setZoom(value) {
            const svgElement = document.querySelector('.svg-container svg');
            svgElement.style.transform = 'scale(' + value + ')';
        }

        function zoomIn() {
            const slider = document.querySelector('.zoom-controls input[type="range"]');
            let value = parseFloat(slider.value);
            if (value < 2) {
                value += 0.1;
                slider.value = value.toFixed(1);
                setZoom(value);
            }
        }

        function zoomOut() {
            const slider = document.querySelector('.zoom-controls input[type="range"]');
            let value = parseFloat(slider.value);
            if (value > 0.1) {
                value -= 0.1;
                slider.value = value.toFixed(1);
                setZoom(value);
            }
        }
    </script>
</body>
</html>`, tm.name, tm.name, string(svgBytes))

			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			if _, err := w.Write([]byte(htmlContent)); err != nil {
				http.Error(w, "Could not write HTML response", http.StatusInternalServerError)
				return
			}
		})
	}
}
