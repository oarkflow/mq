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
	Title      GroupTitle
	Fields     []FieldInfo
	GroupClass string
}

// GroupTitle represents the title configuration for a group
type GroupTitle struct {
	Text  string
	Class string
}

// JSONSchemaRenderer is responsible for rendering HTML fields based on JSONSchema
type JSONSchemaRenderer struct {
	Schema       map[string]interface{}
	HTMLLayout   string
	TemplateData map[string]interface{} // Data for template interpolation
}

// NewJSONSchemaRenderer creates a new instance of JSONSchemaRenderer
func NewJSONSchemaRenderer(schema map[string]interface{}, htmlLayout string) *JSONSchemaRenderer {
	return &JSONSchemaRenderer{
		Schema:       schema,
		HTMLLayout:   htmlLayout,
		TemplateData: make(map[string]interface{}),
	}
}

// SetTemplateData sets the data used for template interpolation
func (r *JSONSchemaRenderer) SetTemplateData(data map[string]interface{}) {
	r.TemplateData = data
}

// interpolateTemplate replaces template placeholders with actual values
func (r *JSONSchemaRenderer) interpolateTemplate(templateStr string) string {
	if len(r.TemplateData) == 0 {
		return templateStr
	}

	// Create a template and execute it with the template data
	tmpl, err := template.New("interpolate").Parse(templateStr)
	if err != nil {
		return templateStr // Return original if parsing fails
	}

	var result bytes.Buffer
	err = tmpl.Execute(&result, r.TemplateData)
	if err != nil {
		return templateStr // Return original if execution fails
	}

	return result.String()
}

// RenderFields generates HTML for fields based on the JSONSchema
func (r *JSONSchemaRenderer) RenderFields() (string, error) {
	groups := parseGroupsFromSchema(r.Schema)

	var groupHTML bytes.Buffer
	for _, group := range groups {
		groupHTML.WriteString(renderGroup(group))
	}

	formConfig := r.Schema["form"].(map[string]interface{})

	// Extract individual form attributes and interpolate template values
	formClass, _ := formConfig["class"].(string)
	formAction, _ := formConfig["action"].(string)
	formMethod, _ := formConfig["method"].(string)
	formEnctype, _ := formConfig["enctype"].(string)

	// Interpolate template placeholders in form action
	formAction = r.interpolateTemplate(formAction)

	buttonsHTML := renderButtons(formConfig)
	tmpl, err := template.New("layout").Funcs(template.FuncMap{
		"form_groups": func() template.HTML {
			return template.HTML(groupHTML.String())
		},
		"form_buttons": func() template.HTML {
			return template.HTML(buttonsHTML)
		},
		"form_attributes": func() template.HTMLAttr {
			attrs := fmt.Sprintf(`class="%s" action="%s" method="%s" enctype="%s"`,
				formClass, formAction, formMethod, formEnctype)
			return template.HTMLAttr(attrs)
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

	// Get required fields from schema root
	var requiredFields map[string]bool = make(map[string]bool)
	if reqFields, ok := schema["required"].([]interface{}); ok {
		for _, field := range reqFields {
			if fieldName, ok := field.(string); ok {
				requiredFields[fieldName] = true
			}
		}
	}

	var groups []GroupInfo
	for _, group := range formConfig["groups"].([]interface{}) {
		groupMap := group.(map[string]interface{})

		// Parse group title
		var groupTitle GroupTitle
		if titleMap, ok := groupMap["title"].(map[string]interface{}); ok {
			if text, ok := titleMap["text"].(string); ok {
				groupTitle.Text = text
			}
			if class, ok := titleMap["class"].(string); ok {
				groupTitle.Class = class
			}
		}

		// Get group class
		groupClass, _ := groupMap["class"].(string)

		var fields []FieldInfo
		for _, fieldName := range groupMap["fields"].([]interface{}) {
			fieldDef := properties[fieldName.(string)].(map[string]interface{})
			order := 0
			if ord, exists := fieldDef["order"].(int); exists {
				order = ord
			}

			// Add required field info to field definition
			fieldDefCopy := make(map[string]interface{})
			for k, v := range fieldDef {
				fieldDefCopy[k] = v
			}
			fieldDefCopy["isRequired"] = requiredFields[fieldName.(string)]

			fields = append(fields, FieldInfo{
				Name:       fieldName.(string),
				Order:      order,
				Definition: fieldDefCopy,
			})
		}
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].Order < fields[j].Order
		})
		groups = append(groups, GroupInfo{
			Title:      groupTitle,
			Fields:     fields,
			GroupClass: groupClass,
		})
	}

	return groups
}

// renderGroup generates HTML for a single group
func renderGroup(group GroupInfo) string {
	var groupHTML bytes.Buffer

	groupHTML.WriteString(`<div class="form-group-container">`)

	// Render group title if present
	if group.Title.Text != "" {
		if group.Title.Class != "" {
			groupHTML.WriteString(fmt.Sprintf(`<div class="%s">%s</div>`, group.Title.Class, group.Title.Text))
		} else {
			groupHTML.WriteString(fmt.Sprintf(`<h3 class="group-title">%s</h3>`, group.Title.Text))
		}
	}

	if group.GroupClass != "" {
		groupHTML.WriteString(fmt.Sprintf(`<div class="%s">`, group.GroupClass))
	} else {
		groupHTML.WriteString(`<div class="form-group-fields">`)
	}
	// Render fields
	for _, field := range group.Fields {
		groupHTML.WriteString(renderField(field))
	}

	// Close group container div
	groupHTML.WriteString(`</div>`)
	groupHTML.WriteString(`</div>`)
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

	// Check if field is required
	isRequired, _ := field.Definition["isRequired"].(bool)

	required := ""
	if isRequired {
		required = "required"
		title += ` <span class="required">*</span>` // Add asterisk for required fields
	}

	// Handle input type from ui config
	inputType := "text"
	if uiType, ok := ui["type"].(string); ok {
		inputType = uiType
	} else if fieldType, ok := field.Definition["type"].(string); ok && fieldType == "email" {
		inputType = "email"
	}

	additionalAttributes := ""
	for key, value := range field.Definition {
		if key != "title" && key != "ui" && key != "placeholder" && key != "type" && key != "order" && key != "isRequired" {
			additionalAttributes += fmt.Sprintf(` %s="%v"`, key, value)
		}
	}

	switch control {
	case "input":
		return fmt.Sprintf(`<div class="%s"><label for="%s">%s</label><input type="%s" id="%s" name="%s" placeholder="%s" %s %s /></div>`, class, name, title, inputType, name, name, placeholder, required, additionalAttributes)
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
