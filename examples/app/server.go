package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/oarkflow/jsonschema"
	"github.com/oarkflow/mq/renderer"
)

type RequestSchemaTemplate struct {
	Schema   *jsonschema.Schema           `json:"schema"`
	Renderer *renderer.JSONSchemaRenderer `json:"template"`
}

var cache = make(map[string]*RequestSchemaTemplate)
var mu = &sync.Mutex{}

func getCachedRenderer(schemaPath, template string) (*renderer.JSONSchemaRenderer, error) {
	mu.Lock()
	defer mu.Unlock()
	path := fmt.Sprintf("%s:%s", schemaPath, template)
	if cached, exists := cache[path]; exists {
		return cached.Renderer, nil
	}

	schemaContent, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("error reading schema file: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile(schemaContent)
	if err != nil {
		return nil, fmt.Errorf("error compiling schema: %w", err)
	}

	templatePath := fmt.Sprintf("templates/%s.html", template)
	htmlLayout, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load template: %w", err)
	}

	renderer := renderer.NewJSONSchemaRenderer(schema, string(htmlLayout))
	cachedTemplate := &RequestSchemaTemplate{
		Schema:   schema,
		Renderer: renderer,
	}
	cache[path] = cachedTemplate

	return cachedTemplate.Renderer, nil
}

func main() {
	http.Handle("/form.css", http.FileServer(http.Dir("templates")))
	http.HandleFunc("/render", func(w http.ResponseWriter, r *http.Request) {
		templateName := r.URL.Query().Get("template")
		if templateName == "" {
			templateName = "basic"
		}
		schemaHTML := r.URL.Query().Get("schema")
		if schemaHTML == "" {
			http.Error(w, "Schema parameter is required", http.StatusBadRequest)
			return
		}
		if !strings.Contains(schemaHTML, ".json") {
			schemaHTML = fmt.Sprintf("%s.json", schemaHTML)
		}
		renderer, err := getCachedRenderer(schemaHTML, templateName)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get cached template: %v", err), http.StatusInternalServerError)
			return
		}

		// Set template data for dynamic interpolation
		templateData := map[string]interface{}{
			"task_id": r.URL.Query().Get("task_id"), // Get task_id from query params
		}
		// If task_id is not provided, use a default value
		if templateData["task_id"] == "" {
			templateData["task_id"] = "default_task_123"
		}

		renderedHTML, err := renderer.RenderFields(templateData)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to render fields: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(renderedHTML))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("Server running on port %s\n", port)
	http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
}
