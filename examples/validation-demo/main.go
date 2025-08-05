package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/oarkflow/jsonschema"
	"github.com/oarkflow/mq/renderer"
)

func main() {
	// Setup routes
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/comprehensive", func(w http.ResponseWriter, r *http.Request) {
		renderFormHandler(w, r, "comprehensive-validation.json", "Comprehensive Validation Demo")
	})
	http.HandleFunc("/features", func(w http.ResponseWriter, r *http.Request) {
		renderFormHandler(w, r, "validation-features.json", "Validation Features Demo")
	})
	http.HandleFunc("/complex", func(w http.ResponseWriter, r *http.Request) {
		renderFormHandler(w, r, "complex.json", "Complex Form Demo")
	})

	fmt.Println("Server starting on :8080")
	fmt.Println("Available examples:")
	fmt.Println("  - http://localhost:8080/comprehensive - Comprehensive validation demo")
	fmt.Println("  - http://localhost:8080/features - Validation features demo")
	fmt.Println("  - http://localhost:8080/complex - Complex form demo")

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func renderFormHandler(w http.ResponseWriter, r *http.Request, schemaFile, title string) {
	// Load and compile schema
	schemaData, err := os.ReadFile(schemaFile)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading schema file: %v", err), http.StatusInternalServerError)
		return
	}

	// Parse JSON schema
	var schemaMap map[string]interface{}
	if err := json.Unmarshal(schemaData, &schemaMap); err != nil {
		http.Error(w, fmt.Sprintf("Error parsing JSON schema: %v", err), http.StatusInternalServerError)
		return
	}

	// Compile schema
	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile(schemaData)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error compiling schema: %v", err), http.StatusInternalServerError)
		return
	}

	// Create renderer with basic template
	basicTemplate := `
<form {{if .Form.Action}}action="{{.Form.Action}}"{{end}} {{if .Form.Method}}method="{{.Form.Method}}"{{end}} {{if .Form.Class}}class="{{.Form.Class}}"{{end}}>
	{{.FieldsHTML}}
	<div class="form-buttons">
		{{.ButtonsHTML}}
	</div>
</form>
	`

	renderer := renderer.NewJSONSchemaRenderer(schema, basicTemplate)

	// Render form with empty data
	html, err := renderer.RenderFields(map[string]any{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Error rendering form: %v", err), http.StatusInternalServerError)
		return
	}

	// Create complete HTML page
	fullHTML := createFullHTMLPage(title, html)
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(fullHTML))
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	html := "<!DOCTYPE html>" +
		"<html>" +
		"<head>" +
		"<title>JSON Schema Form Builder - Validation Examples</title>" +
		"<style>" +
		"body { font-family: Arial, sans-serif; margin: 40px; }" +
		"h1 { color: #333; }" +
		".example-list { list-style: none; padding: 0; }" +
		".example-list li { margin: 15px 0; }" +
		".example-list a { display: inline-block; padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; }" +
		".example-list a:hover { background: #0056b3; }" +
		".description { color: #666; margin-top: 5px; }" +
		"</style>" +
		"</head>" +
		"<body>" +
		"<h1>JSON Schema Form Builder - Validation Examples</h1>" +
		"<p>This demo showcases comprehensive JSON Schema 2020-12 validation support with:</p>" +
		"<ul>" +
		"<li>String validations (minLength, maxLength, pattern, format)</li>" +
		"<li>Numeric validations (minimum, maximum, exclusiveMinimum, exclusiveMaximum, multipleOf)</li>" +
		"<li>Array validations (minItems, maxItems, uniqueItems)</li>" +
		"<li>Conditional validations (if/then/else, allOf, anyOf, oneOf)</li>" +
		"<li>Dependent validations (dependentRequired, dependentSchemas)</li>" +
		"<li>Client-side and server-side validation</li>" +
		"<li>Advanced HTML form generation with validation attributes</li>" +
		"</ul>" +
		"<h2>Available Examples:</h2>" +
		"<ul class=\"example-list\">" +
		"<li>" +
		"<a href=\"/comprehensive\">Comprehensive Validation Demo</a>" +
		"<div class=\"description\">Complete example with personal info, address, preferences, financial data, skills, portfolio, and complex conditional logic</div>" +
		"</li>" +
		"<li>" +
		"<a href=\"/features\">Validation Features Demo</a>" +
		"<div class=\"description\">Focused examples of specific validation features organized by category</div>" +
		"</li>" +
		"<li>" +
		"<a href=\"/complex\">Complex Form Demo</a>" +
		"<div class=\"description\">Original company registration form with nested objects and grouped fields</div>" +
		"</li>" +
		"</ul>" +
		"</body>" +
		"</html>"

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

func createFullHTMLPage(title, formHTML string) string {
	template := "<!DOCTYPE html>" +
		"<html>" +
		"<head>" +
		"<title>" + title + "</title>" +
		"<meta charset=\"utf-8\">" +
		"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">" +
		"<script src=\"https://cdn.tailwindcss.com\"></script>" +
		"<style>" +
		".form-group { margin-bottom: 1rem; }" +
		".form-group label { display: block; margin-bottom: 0.25rem; font-weight: 600; }" +
		".form-group input, .form-group select, .form-group textarea { width: 100%; padding: 0.5rem; border: 1px solid #d1d5db; border-radius: 0.375rem; }" +
		".btn { padding: 0.5rem 1rem; border-radius: 0.375rem; font-weight: 600; cursor: pointer; border: none; margin-right: 0.5rem; }" +
		".btn-primary { background-color: #3b82f6; color: white; }" +
		".validation-error { color: #dc2626; font-size: 0.875rem; margin-top: 0.25rem; }" +
		".back-link { display: inline-block; margin-bottom: 2rem; color: #3b82f6; text-decoration: none; }" +
		"</style>" +
		"</head>" +
		"<body class=\"bg-gray-50 min-h-screen py-8\">" +
		"<div class=\"max-w-4xl mx-auto px-4\">" +
		"<a href=\"/\" class=\"back-link\">← Back to Examples</a>" +
		"<div class=\"bg-white rounded-lg shadow-lg p-8\">" +
		"<h1 class=\"text-3xl font-bold text-gray-900 mb-6\">" + title + "</h1>" +
		"<p class=\"text-gray-600 mb-8\">This form demonstrates comprehensive JSON Schema validation.</p>" +
		formHTML +
		"</div></div>" +
		"<script>" +
		"document.addEventListener('DOMContentLoaded', function() {" +
		"const form = document.querySelector('form');" +
		"if (form) {" +
		"form.addEventListener('submit', function(e) {" +
		"e.preventDefault();" +
		"alert('Form validation demo - would submit data in real application');" +
		"});" +
		"}" +
		"});" +
		"</script>" +
		"</body></html>"

	return template
}
