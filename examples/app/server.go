package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

func main() {
	schemaContent, err := os.ReadFile("schema.json")
	if err != nil {
		fmt.Printf("Error reading schema file: %v\n", err)
		return
	}
	var schema map[string]interface{}
	if err := json.Unmarshal(schemaContent, &schema); err != nil {
		fmt.Printf("Error parsing schema: %v\n", err)
		return
	}
	http.Handle("/form.css", http.FileServer(http.Dir("templates")))
	http.HandleFunc("/render", func(w http.ResponseWriter, r *http.Request) {
		templateName := r.URL.Query().Get("template")
		if templateName == "" {
			templateName = "basic"
		}

		templatePath := fmt.Sprintf("templates/%s.html", templateName)
		htmlLayout, err := os.ReadFile(templatePath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to load template: %v", err), http.StatusInternalServerError)
			return
		}

		renderer := NewJSONSchemaRenderer(schema, string(htmlLayout))
		renderedHTML, err := renderer.RenderFields()
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
