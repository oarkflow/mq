package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/oarkflow/mq/renderer"

	"github.com/oarkflow/form"
)

func main() {
	http.Handle("/form.css", http.FileServer(http.Dir("templates")))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Serve the main form page
		http.ServeFile(w, r, "templates/form.html")
	})
	http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read request body: %v", err), http.StatusBadRequest)
			return
		}
		data, err := form.DecodeForm(body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to decode form: %v", err), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(data)
	})
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
		renderer, err := renderer.GetFromFile(schemaHTML, templateName)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get cached template: %v", err), http.StatusInternalServerError)
			return
		}

		// Set template data for dynamic interpolation
		templateData := map[string]interface{}{
			"task_id":    r.URL.Query().Get("task_id"), // Get task_id from query params
			"session_id": "test_session_123",           // Example session_id
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

	fmt.Printf("Server running on port http://localhost:%s\n", port)
	http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
}
