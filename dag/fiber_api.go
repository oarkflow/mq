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
func (tm *DAG) RenderFiber(c *fiber.Ctx) error {
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
	if result.Ctx == nil {
		result.Ctx = ctx
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
		if strings.Contains(htmlContent, "{{current_uri}}") {
			htmlContent = strings.ReplaceAll(htmlContent, "{{current_uri}}", c.Path())
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

// processSVGContent processes SVG to ensure proper scaling using viewBox
func (tm *DAG) processSVGContent(svgContent string) string {
	// Extract width and height from SVG
	var widthVal, heightVal float64
	var hasWidth, hasHeight bool

	// Find width attribute
	if strings.Contains(svgContent, `width="`) {
		start := strings.Index(svgContent, `width="`) + 7
		end := strings.Index(svgContent[start:], `"`)
		if end > 0 {
			width := svgContent[start : start+end]
			// Remove pt, px, or other units and convert to float
			cleanWidth := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(width, "pt"), "px"), "in")
			if _, err := fmt.Sscanf(cleanWidth, "%f", &widthVal); err == nil {
				hasWidth = true
				// Convert pt to pixels (1pt = 1.33px approximately)
				if strings.HasSuffix(width, "pt") {
					widthVal *= 1.33
				}
			}
		}
	}

	// Find height attribute
	if strings.Contains(svgContent, `height="`) {
		start := strings.Index(svgContent, `height="`) + 8
		end := strings.Index(svgContent[start:], `"`)
		if end > 0 {
			height := svgContent[start : start+end]
			// Remove pt, px, or other units and convert to float
			cleanHeight := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(height, "pt"), "px"), "in")
			if _, err := fmt.Sscanf(cleanHeight, "%f", &heightVal); err == nil {
				hasHeight = true
				// Convert pt to pixels (1pt = 1.33px approximately)
				if strings.HasSuffix(height, "pt") {
					heightVal *= 1.33
				}
			}
		}
	}

	// If we don't have both dimensions, use fallback values
	if !hasWidth || !hasHeight || widthVal <= 0 || heightVal <= 0 {
		// Try to extract from viewBox first
		if strings.Contains(svgContent, `viewBox="`) {
			start := strings.Index(svgContent, `viewBox="`) + 9
			end := strings.Index(svgContent[start:], `"`)
			if end > 0 {
				viewBox := svgContent[start : start+end]
				parts := strings.Fields(viewBox)
				if len(parts) >= 4 {
					if w, err := fmt.Sscanf(parts[2], "%f", &widthVal); err == nil && w == 1 && widthVal > 0 {
						hasWidth = true
					}
					if h, err := fmt.Sscanf(parts[3], "%f", &heightVal); err == nil && h == 1 && heightVal > 0 {
						hasHeight = true
					}
				}
			}
		}

		// Final fallback
		if !hasWidth || widthVal <= 0 {
			widthVal = 800
		}
		if !hasHeight || heightVal <= 0 {
			heightVal = 600
		}
	}

	// Create viewBox
	viewBox := fmt.Sprintf("0 0 %.0f %.0f", widthVal, heightVal)

	// Process the SVG content
	processedSVG := svgContent

	// Add or update viewBox
	if strings.Contains(processedSVG, `viewBox="`) {
		// Replace existing viewBox
		start := strings.Index(processedSVG, `viewBox="`)
		end := strings.Index(processedSVG[start+9:], `"`) + start + 9
		processedSVG = processedSVG[:start] + fmt.Sprintf(`viewBox="%s"`, viewBox) + processedSVG[end+1:]
	} else {
		// Add viewBox to opening svg tag
		svgStart := strings.Index(processedSVG, "<svg")
		if svgStart >= 0 {
			// Find the end of the opening tag
			tagEnd := strings.Index(processedSVG[svgStart:], ">") + svgStart
			// Insert viewBox before the closing >
			processedSVG = processedSVG[:tagEnd] + fmt.Sprintf(` viewBox="%s"`, viewBox) + processedSVG[tagEnd:]
		}
	}

	// Remove or replace width and height with 100% for responsive scaling
	if strings.Contains(processedSVG, `width="`) {
		start := strings.Index(processedSVG, `width="`)
		end := strings.Index(processedSVG[start+7:], `"`) + start + 7
		processedSVG = processedSVG[:start] + `width="100%"` + processedSVG[end+1:]
	} else {
		// Add width if it doesn't exist
		svgStart := strings.Index(processedSVG, "<svg")
		if svgStart >= 0 {
			tagEnd := strings.Index(processedSVG[svgStart:], ">") + svgStart
			processedSVG = processedSVG[:tagEnd] + ` width="100%"` + processedSVG[tagEnd:]
		}
	}

	if strings.Contains(processedSVG, `height="`) {
		start := strings.Index(processedSVG, `height="`)
		end := strings.Index(processedSVG[start+8:], `"`) + start + 8
		processedSVG = processedSVG[:start] + `height="100%"` + processedSVG[end+1:]
	} else {
		// Add height if it doesn't exist
		svgStart := strings.Index(processedSVG, "<svg")
		if svgStart >= 0 {
			tagEnd := strings.Index(processedSVG[svgStart:], ">") + svgStart
			processedSVG = processedSVG[:tagEnd] + ` height="100%"` + processedSVG[tagEnd:]
		}
	}

	return processedSVG
}

// SVGViewerHTML creates the HTML with advanced SVG zoom and pan functionality
func (tm *DAG) SVGViewerHTML(svgContent string) string {
	// Process SVG to ensure proper scaling
	processedSVG := tm.processSVGContent(svgContent)
	return fmt.Sprintf(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DAG Pipeline - %s</title>
    <style>
        * {
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            background: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
        }

        .header {
            text-align: center;
            color: white;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }

        .header h1 {
            font-size: 2.5em;
            margin-top: 10px;
            margin-bottom: 10px;
        }

        .header p {
            font-size: 1.2em;
            opacity: 0.9;
            margin-top: 10px;
            margin-bottom: 10px;
        }

        .svg-viewer-container {
            background: white;
            border-radius: 15px;
            padding: 20px;
            box-shadow: 0 12px 40px rgba(0, 0, 0, 0.3);
            position: relative;
        }

        .viewer-container {
            width: 1000px;
            height: 650px;
            border: 2px solid #ddd;
            background-color: #fafafa;
            position: relative;
            overflow: hidden;
            border-radius: 8px;
        }

        .controls {
            display: flex;
            justify-content: center;
            gap: 5px;
            flex-wrap: wrap;
			bottom: 40px;
			position: absolute;
			right: 45px;
			z-index: 10;
        }

        .control-btn {
            padding: 8px;
            color: rgba(0, 0, 0, 0.6);
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
            transition: background-color 0.3s;
            font-weight: 500;
            background-color: white;
        }

        .control-btn:hover {
            color: rgba(0, 0, 0, 0.8);
        }

        .control-btn:active {
            color: rgba(0, 0, 0, 0.9);
        }

        .svg-container {
            width: 100%%;
            height: 100%%;
            cursor: grab;
            position: relative;
            overflow: hidden;
            display: block;
            align-items: center;
            justify-content: center;
        }

        .svg-container:active {
            cursor: grabbing;
        }

        .svg-wrapper {
			user-select: none;
            transform-origin: center center;
            transition: transform 0.2s ease-out;
            max-width: 100%%;
            max-height: 100%%;
        }

        .svg-wrapper svg {
            display: block;
        }

        .instructions {
            text-align: center;
            margin: 10px 0;
            color: white;
            font-size: 14px;
            opacity: 0.9;
        }

        .actions {
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            justify-content: center;
            margin-top: 20px;
        }

        .btn {
            background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
            color: white;
            padding: 12px 24px;
            border: none;
            border-radius: 25px;
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
            text-decoration: none;
            display: inline-block;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
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

        @media (max-width: 1100px) {
            .viewer-container {
                width: 90vw;
                height: 60vh;
            }
        }

        @media (max-width: 768px) {
            body {
                padding: 10px;
            }

            .viewer-container {
                width: 95vw;
                height: 50vh;
            }

            .controls {
                gap: 5px;
            }

            .control-btn {
                padding: 6px 12px;
                font-size: 12px;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>DAG Pipeline</h1>
        <p>%s - Workflow Visualization</p>
    </div>

    <div class="svg-viewer-container relative">

        <div class="controls">
            <button class="control-btn" onclick="zoomIn()" title="Zoom In">➕</button>
            <button class="control-btn" onclick="zoomOut()" title="Zoom Out">➖</button>
            <button class="control-btn" onclick="resetView()" title="Reset View">🔄</button>
        </div>

        <div class="viewer-container" id="viewerContainer">
            <div class="svg-container" id="svgContainer">
                <div class="svg-wrapper" id="svgWrapper">
                    %s
                </div>
            </div>
        </div>
    </div>

    <script>
        let currentZoom = 1;
        let initialScale = 1;
        let isDragging = false;
        let startX, startY, currentX = 0, currentY = 0;

        const svgWrapper = document.getElementById('svgWrapper');
        const svgContainer = document.getElementById('svgContainer');
        const viewerContainer = document.getElementById('viewerContainer');

        // Initialize the viewer
        function initializeViewer() {
            const mainSvg = document.querySelector('#svgWrapper svg');
            if (!mainSvg) {
                console.error('SVG element not found');
                setTimeout(initializeViewer, 100);
                return;
            }

            // Get container dimensions
            const containerWidth = viewerContainer.clientWidth - 4; // Account for border
            const containerHeight = viewerContainer.clientHeight - 4;

            // Set SVG to container size
            mainSvg.setAttribute('width', containerWidth + 'px');
            mainSvg.setAttribute('height', containerHeight + 'px');

            // Get SVG viewBox dimensions for logging
            let svgWidth, svgHeight;
            const viewBox = mainSvg.getAttribute('viewBox');

            if (viewBox) {
                const viewBoxValues = viewBox.split(' ').map(v => parseFloat(v.trim()));
                if (viewBoxValues.length >= 4) {
                    svgWidth = viewBoxValues[2];
                    svgHeight = viewBoxValues[3];
                }
            }

            // Fallback to getBBox if viewBox is not available or invalid
            if (!svgWidth || !svgHeight || svgWidth <= 0 || svgHeight <= 0) {
                try {
                    const bbox = mainSvg.getBBox();
                    svgWidth = bbox.width;
                    svgHeight = bbox.height;
                } catch (e) {
                    console.warn('Could not get SVG bbox, using default dimensions');
                    svgWidth = 800;
                    svgHeight = 600;
                }
            }

            // Set initial scale to 1 for full-width rendering
            initialScale = 1;

            // Reset position
            currentX = 0;
            currentY = 0;
            currentZoom = initialScale;

            // Apply initial transform
            updateTransform();

            // Set cursor for dragging
            svgContainer.style.cursor = 'grab';

            console.log('SVG initialized:', {
                svgWidth: svgWidth,
                svgHeight: svgHeight,
                containerWidth: containerWidth,
                containerHeight: containerHeight,
                initialScale: initialScale
            });
        }

        function updateTransform() {
            svgWrapper.style.transform = 'translate(' + currentX + 'px, ' + currentY + 'px) scale(' + currentZoom + ')';
        }

        function zoomIn() {
            currentZoom = Math.min(currentZoom * 1.2, 10); // Max 10x zoom
            updateTransform();
        }

        function zoomOut() {
            currentZoom = Math.max(currentZoom / 1.2, initialScale * 0.1); // Don't zoom out too much
            updateTransform();
        }

        function resetView() {
            currentX = 0;
            currentY = 0;
            currentZoom = initialScale;
            updateTransform();
        }

        // Mouse events for dragging
        svgContainer.addEventListener('mousedown', function(e) {
            isDragging = true;
            startX = e.clientX - currentX;
            startY = e.clientY - currentY;
            svgContainer.style.cursor = 'grabbing';
            e.preventDefault();
        });

        document.addEventListener('mousemove', function(e) {
            if (isDragging) {
                currentX = e.clientX - startX;
                currentY = e.clientY - startY;
                updateTransform();
            }
        });

        document.addEventListener('mouseup', function() {
            isDragging = false;
            svgContainer.style.cursor = 'grab';
        });

        // Touch events for mobile
        svgContainer.addEventListener('touchstart', function(e) {
            if (e.touches.length === 1) {
                isDragging = true;
                const touch = e.touches[0];
                startX = touch.clientX - currentX;
                startY = touch.clientY - currentY;
                e.preventDefault();
            }
        });

        document.addEventListener('touchmove', function(e) {
            if (isDragging && e.touches.length === 1) {
                const touch = e.touches[0];
                currentX = touch.clientX - startX;
                currentY = touch.clientY - startY;
                updateTransform();
                e.preventDefault();
            }
        });

        document.addEventListener('touchend', function() {
            isDragging = false;
        });

        // Wheel zoom
        svgContainer.addEventListener('wheel', function(e) {
            e.preventDefault();
            const zoomFactor = e.deltaY > 0 ? 0.9 : 1.1;

            // Get mouse position relative to container center
            const rect = svgContainer.getBoundingClientRect();
            const centerX = rect.width / 2;
            const centerY = rect.height / 2;
            const mouseX = e.clientX - rect.left - centerX;
            const mouseY = e.clientY - rect.top - centerY;

            // Calculate zoom point relative to current transform
            const zoomPointX = (mouseX - currentX) / currentZoom;
            const zoomPointY = (mouseY - currentY) / currentZoom;

            // Apply zoom
            const newZoom = Math.max(Math.min(currentZoom * zoomFactor, 10), initialScale * 0.1);

            if (newZoom !== currentZoom) {
                // Adjust position to zoom around mouse point
                currentX = mouseX - zoomPointX * newZoom;
                currentY = mouseY - zoomPointY * newZoom;
                currentZoom = newZoom;

                updateTransform();
            }
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', function(e) {
            switch(e.key) {
                case '+':
                case '=':
                    e.preventDefault();
                    zoomIn();
                    break;
                case '-':
                    e.preventDefault();
                    zoomOut();
                    break;
                case '0':
                    e.preventDefault();
                    resetView();
                    break;
            }
        });

        // Initialize when page loads
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', initializeViewer);
        } else {
            initializeViewer();
        }

        window.addEventListener('resize', function() {
            setTimeout(initializeViewer, 100);
        });

        // Fallback initialization with timeout
        setTimeout(function() {
            if (currentZoom === 1 && initialScale === 1) {
                initializeViserver();
            }
        }, 500);
    </script>
</body>
</html>`, tm.name, tm.name, processedSVG)
}

// Handlers initializes route handlers.
func (tm *DAG) Handlers(app any, prefix string) {
	if prefix != "" && prefix != "/" {
		tm.httpPrefix = prefix
	}
	switch a := app.(type) {
	case fiber.Router:
		a.All("/process", tm.RenderFiber)
		a.Get("/request", tm.RenderFiber)
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

			// Generate HTML with advanced SVG viewer
			htmlContent := tm.SVGViewerHTML(string(svgBytes))

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

			// Generate HTML with advanced SVG viewer
			htmlContent := tm.SVGViewerHTML(string(svgBytes))

			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			if _, err := w.Write([]byte(htmlContent)); err != nil {
				http.Error(w, "Could not write HTML response", http.StatusInternalServerError)
				return
			}
		})
	}
}
