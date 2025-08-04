package main

import (
	"bytes"
	"fmt"
	"html/template"
	"sort"
)

// FieldInfo represents metadata for a field extracted from JSONSchema
type FieldInfo struct {
	Name       string
	Order      int
	Definition map[string]interface{}
}

// JSONSchemaRenderer is responsible for rendering HTML fields based on JSONSchema
type JSONSchemaRenderer struct {
	Schema     map[string]interface{}
	HTMLLayout string
}

// NewJSONSchemaRenderer creates a new instance of JSONSchemaRenderer
func NewJSONSchemaRenderer(schema map[string]interface{}, htmlLayout string) *JSONSchemaRenderer {
	return &JSONSchemaRenderer{
		Schema:     schema,
		HTMLLayout: htmlLayout,
	}
}

// RenderFields generates HTML for fields based on the JSONSchema
func (r *JSONSchemaRenderer) RenderFields() (string, error) {
	fields := parseFieldsFromSchema(r.Schema)
	requiredFields := make(map[string]bool)
	if requiredList, ok := r.Schema["required"].([]interface{}); ok {
		for _, field := range requiredList {
			if fieldName, ok := field.(string); ok {
				requiredFields[fieldName] = true
			}
		}
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Order < fields[j].Order
	})

	var fieldHTML bytes.Buffer
	for _, field := range fields {
		fieldHTML.WriteString(renderField(field, requiredFields))
	}

	tmpl, err := template.New("layout").Funcs(template.FuncMap{
		"form_fields": func() template.HTML {
			return template.HTML(fieldHTML.String())
		},
	}).Parse(r.HTMLLayout)
	if err != nil {
		return "", fmt.Errorf("failed to parse HTML layout: %w", err)
	}

	var renderedHTML bytes.Buffer
	err = tmpl.Execute(&renderedHTML, nil)
	if err != nil {
		return "", fmt.Errorf("failed to execute HTML template: %w", err)
	}

	return fmt.Sprintf(`<form>%s</form>`, renderedHTML.String()), nil
}

// parseFieldsFromSchema extracts and sorts fields from schema
func parseFieldsFromSchema(schema map[string]interface{}) []FieldInfo {
	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		return nil
	}

	var fields []FieldInfo
	for name, definition := range properties {
		order := 0
		if defMap, ok := definition.(map[string]interface{}); ok {
			if ord, exists := defMap["order"].(float64); exists {
				order = int(ord) // Ensure consistent type for sorting
			}
			fields = append(fields, FieldInfo{
				Name:       name,
				Order:      order,
				Definition: defMap,
			})
		}
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Order < fields[j].Order
	})

	return fields
}

// renderField generates HTML for a single field
func renderField(field FieldInfo, requiredFields map[string]bool) string {
	ui, ok := field.Definition["ui"].(map[string]interface{})
	if !ok {
		return ""
	}

	control, _ := ui["element"].(string)
	class, _ := ui["class"].(string)
	name, _ := ui["name"].(string)
	title, _ := field.Definition["title"].(string)
	placeholder, _ := field.Definition["placeholder"].(string)

	isRequired := requiredFields[name]
	required := ""
	if isRequired {
		required = "required"
		title += " *" // Add asterisk for required fields
	}

	additionalAttributes := ""
	for key, value := range field.Definition {
		if key != "title" && key != "ui" && key != "placeholder" {
			additionalAttributes += fmt.Sprintf(` %s="%v"`, key, value)
		}
	}

	switch control {
	case "input":
		return fmt.Sprintf(`<div class="%s"><label for="%s">%s</label><input type="text" id="%s" name="%s" placeholder="%s" %s %s /></div>`, class, name, title, name, name, placeholder, required, additionalAttributes)
	case "textarea":
		return fmt.Sprintf(`<div class="%s"><label for="%s">%s</label><textarea id="%s" name="%s" placeholder="%s" %s %s></textarea></div>`, class, name, title, name, name, placeholder, required, additionalAttributes)
	case "select":
		options, _ := ui["options"].([]interface{})
		var optionsHTML bytes.Buffer
		for _, option := range options {
			optionsHTML.WriteString(fmt.Sprintf(`<option value="%v">%v</option>`, option, option))
		}
		return fmt.Sprintf(`<div class="%s"><label for="%s">%s</label><select id="%s" name="%s" %s %s>%s</select></div>`, class, name, title, name, name, required, additionalAttributes, optionsHTML.String())
	case "h1", "h2", "h3", "h4", "h5", "h6":
		return fmt.Sprintf(`<%s class="%s" id="%s" %s>%s</%s>`, control, class, name, additionalAttributes, title, control)
	case "p":
		return fmt.Sprintf(`<p class="%s" id="%s" %s>%s</p>`, class, name, additionalAttributes, title)
	case "a":
		href, _ := ui["href"].(string)
		return fmt.Sprintf(`<a class="%s" id="%s" href="%s" %s>%s</a>`, class, name, href, additionalAttributes, title)
	default:
		return ""
	}
}
