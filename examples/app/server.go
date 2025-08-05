package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/oarkflow/mq/renderer"
)

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
		renderer, err := renderer.Get(schemaHTML, templateName)
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
