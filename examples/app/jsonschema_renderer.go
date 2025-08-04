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

// GroupInfo represents metadata for a group extracted from JSONSchema
type GroupInfo struct {
	Title  string
	Fields []FieldInfo
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
	groups := parseGroupsFromSchema(r.Schema)

	var groupHTML bytes.Buffer
	var fieldHTML bytes.Buffer
	for _, group := range groups {
		groupHTML.WriteString(renderGroup(group))
		for _, field := range group.Fields {
			fieldHTML.WriteString(renderField(field))
		}
	}
	formConfig := r.Schema["form"].(map[string]interface{})
	formAttributes := fmt.Sprintf(
		`class="%s" action="%s" method="%s" enctype="%s"`,
		formConfig["class"],
		formConfig["action"],
		formConfig["method"],
		formConfig["enctype"],
	)

	buttonsHTML := renderButtons(formConfig)
	tmpl, err := template.New("layout").Funcs(template.FuncMap{
		"form_fields": func() template.HTML {
			return template.HTML(fieldHTML.String())
		},
		"form_groups": func() template.HTML {
			return template.HTML(groupHTML.String())
		},
		"form_buttons": func() template.HTML {
			return template.HTML(buttonsHTML)
		},
		"form_attributes": func() template.HTML {
			return template.HTML(formAttributes)
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

	return renderedHTML.String(), nil
}

// parseGroupsFromSchema extracts and sorts groups and fields from schema
func parseGroupsFromSchema(schema map[string]interface{}) []GroupInfo {
	formConfig, ok := schema["form"].(map[string]interface{})
	if !ok {
		return nil
	}

	properties, ok := schema["properties"].(map[string]interface{})
	if !ok {
		return nil
	}

	var groups []GroupInfo
	for _, group := range formConfig["groups"].([]interface{}) {
		groupMap := group.(map[string]interface{})
		var fields []FieldInfo
		for _, fieldName := range groupMap["fields"].([]interface{}) {
			fieldDef := properties[fieldName.(string)].(map[string]interface{})
			order := 0
			if ord, exists := fieldDef["order"].(int); exists {
				order = ord
			}
			fields = append(fields, FieldInfo{
				Name:       fieldName.(string),
				Order:      order,
				Definition: fieldDef,
			})
		}
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].Order < fields[j].Order
		})
		groups = append(groups, GroupInfo{
			Title:  groupMap["title"].(string),
			Fields: fields,
		})
	}

	return groups
}

// renderGroup generates HTML for a single group
func renderGroup(group GroupInfo) string {
	var groupHTML bytes.Buffer
	groupHTML.WriteString(fmt.Sprintf(`<fieldset><legend>%s</legend>`, group.Title))
	for _, field := range group.Fields {
		groupHTML.WriteString(renderField(field))
	}
	groupHTML.WriteString(`</fieldset>`)
	return groupHTML.String()
}

// renderField generates HTML for a single field
func renderField(field FieldInfo) string {
	ui, ok := field.Definition["ui"].(map[string]interface{})
	if !ok {
		return ""
	}

	control, _ := ui["element"].(string)
	class, _ := ui["class"].(string)
	name, _ := ui["name"].(string)
	title, _ := field.Definition["title"].(string)
	placeholder, _ := field.Definition["placeholder"].(string)

	isRequired := false
	if requiredFields, ok := field.Definition["required"].([]interface{}); ok {
		for _, rf := range requiredFields {
			if rf == name {
				isRequired = true
				break
			}
		}
	}

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

// renderButtons generates HTML for form buttons
func renderButtons(formConfig map[string]interface{}) string {
	var buttonsHTML bytes.Buffer
	if submitConfig, ok := formConfig["submit"].(map[string]interface{}); ok {
		buttonsHTML.WriteString(fmt.Sprintf(`<button type="%s" class="%s">%s</button>`, submitConfig["type"], submitConfig["class"], submitConfig["label"]))
	}
	if resetConfig, ok := formConfig["reset"].(map[string]interface{}); ok {
		buttonsHTML.WriteString(fmt.Sprintf(`<button type="%s" class="%s">%s</button>`, resetConfig["type"], resetConfig["class"], resetConfig["label"]))
	}
	return buttonsHTML.String()
}
