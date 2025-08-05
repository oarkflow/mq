package renderer

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
	Definition map[string]any
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
	Schema       map[string]any
	HTMLLayout   string
	TemplateData map[string]any // Data for template interpolation
	cachedHTML   string         // Cached rendered HTML
}

// NewJSONSchemaRenderer creates a new instance of JSONSchemaRenderer
func NewJSONSchemaRenderer(schema map[string]any, htmlLayout string) *JSONSchemaRenderer {
	return &JSONSchemaRenderer{
		Schema:       schema,
		HTMLLayout:   htmlLayout,
		TemplateData: make(map[string]any),
	}
}

// SetTemplateData sets the data used for template interpolation
func (r *JSONSchemaRenderer) SetTemplateData(data map[string]any) {
	r.TemplateData = data
}

// interpolateTemplate replaces template placeholders with actual values
func (r *JSONSchemaRenderer) interpolateTemplate(templateStr string) string {
	if len(r.TemplateData) == 0 {
		return templateStr
	}

	tmpl, err := template.New("interpolate").Parse(templateStr)
	if err != nil {
		// Fallback to simple string replacement if template parsing fails
		result := templateStr
		for key, value := range r.TemplateData {
			placeholder := fmt.Sprintf("{{%s}}", key)
			if valueStr, ok := value.(string); ok {
				result = fmt.Sprintf("%s", bytes.ReplaceAll([]byte(result), []byte(placeholder), []byte(valueStr)))
			}
		}
		return result
	}

	var templateResult bytes.Buffer
	err = tmpl.Execute(&templateResult, r.TemplateData)
	if err != nil {
		return templateStr // Return original string if execution fails
	}

	return templateResult.String()
}

// RenderFields generates HTML for fields based on the JSONSchema
func (r *JSONSchemaRenderer) RenderFields() (string, error) {
	// Return cached HTML if available
	if r.cachedHTML != "" {
		return r.cachedHTML, nil
	}

	groups := parseGroupsFromSchema(r.Schema)
	var groupHTML bytes.Buffer
	for _, group := range groups {
		groupHTML.WriteString(renderGroup(group))
	}

	formConfig := r.Schema["form"].(map[string]any)
	formClass, _ := formConfig["class"].(string)
	formAction, _ := formConfig["action"].(string)
	formMethod, _ := formConfig["method"].(string)
	formEnctype, _ := formConfig["enctype"].(string)

	// Interpolate template data into form action
	formAction = r.interpolateTemplate(formAction)
	buttonsHTML := renderButtons(formConfig)

	// Create a new template with the layout and functions
	tmpl, err := template.New("layout").Funcs(template.FuncMap{
		"form_groups": func() template.HTML {
			return template.HTML(groupHTML.String())
		},
		"form_buttons": func() template.HTML {
			return template.HTML(buttonsHTML)
		},
		"form_attributes": func() template.HTMLAttr {
			return template.HTMLAttr(fmt.Sprintf(`class="%s" action="%s" method="%s" enctype="%s"`,
				formClass, formAction, formMethod, formEnctype))
		},
	}).Parse(r.HTMLLayout)
	if err != nil {
		return "", fmt.Errorf("failed to parse HTML layout: %w", err)
	}

	var renderedHTML bytes.Buffer
	if err := tmpl.Execute(&renderedHTML, nil); err != nil {
		return "", fmt.Errorf("failed to execute HTML template: %w", err)
	}

	r.cachedHTML = renderedHTML.String()
	return r.cachedHTML, nil
}

// parseGroupsFromSchema extracts and sorts groups and fields from schema
func parseGroupsFromSchema(schema map[string]any) []GroupInfo {
	formConfig, ok := schema["form"].(map[string]any)
	if !ok {
		return nil
	}

	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		return nil
	}

	var requiredFields map[string]bool = make(map[string]bool)
	if reqFields, ok := schema["required"].([]any); ok {
		for _, field := range reqFields {
			if fieldName, ok := field.(string); ok {
				requiredFields[fieldName] = true
			}
		}
	}

	var groups []GroupInfo
	for _, group := range formConfig["groups"].([]any) {
		groupMap := group.(map[string]any)

		var groupTitle GroupTitle
		if titleMap, ok := groupMap["title"].(map[string]any); ok {
			if text, ok := titleMap["text"].(string); ok {
				groupTitle.Text = text
			}
			if class, ok := titleMap["class"].(string); ok {
				groupTitle.Class = class
			}
		}

		groupClass, _ := groupMap["class"].(string)

		var fields []FieldInfo
		for _, fieldName := range groupMap["fields"].([]any) {
			fieldDef := properties[fieldName.(string)].(map[string]any)
			order := 0
			if ord, exists := fieldDef["order"].(int); exists {
				order = ord
			}

			fieldDefCopy := make(map[string]any)
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

	// A single template for the entire group structure
	const groupTemplateStr = `
<div class="form-group-container">
    {{if .Title.Text}}
        {{if .Title.Class}}
            <div class="{{.Title.Class}}">{{.Title.Text}}</div>
        {{else}}
            <h3 class="group-title">{{.Title.Text}}</h3>
        {{end}}
    {{end}}
    <div class="{{.GroupClass}}">{{.FieldsHTML}}</div>
</div>
`

	// Render fields
	var fieldsHTML bytes.Buffer
	for _, field := range group.Fields {
		fieldsHTML.WriteString(renderField(field))
	}

	tmpl := template.Must(template.New("group").Parse(groupTemplateStr))
	data := map[string]any{
		"Title":      group.Title,
		"GroupClass": group.GroupClass,
		"FieldsHTML": template.HTML(fieldsHTML.String()),
	}
	if group.GroupClass == "" {
		data["GroupClass"] = "form-group-fields"
	}

	if err := tmpl.Execute(&groupHTML, data); err != nil {
		return "" // Return empty string on error
	}

	return groupHTML.String()
}

// Templates for field rendering
var fieldTemplates = map[string]string{
	"input":    `<div class="{{.Class}}"><label for="{{.Name}}">{{.Title}}</label><input type="{{.InputType}}" id="{{.Name}}" name="{{.Name}}" placeholder="{{.Placeholder}}" {{.Required}} {{.AdditionalAttributes}} /></div>`,
	"textarea": `<div class="{{.Class}}"><label for="{{.Name}}">{{.Title}}</label><textarea id="{{.Name}}" name="{{.Name}}" placeholder="{{.Placeholder}}" {{.Required}} {{.AdditionalAttributes}}></textarea></div>`,
	"select":   `<div class="{{.Class}}"><label for="{{.Name}}">{{.Title}}</label><select id="{{.Name}}" name="{{.Name}}" {{.Required}} {{.AdditionalAttributes}}>{{.OptionsHTML}}</select></div>`,
	"h":        `<{{.Control}} class="{{.Class}}" id="{{.Name}}" {{.AdditionalAttributes}}>{{.Title}}</{{.Control}}>`,
	"p":        `<p class="{{.Class}}" id="{{.Name}}" {{.AdditionalAttributes}}>{{.Title}}</p>`,
	"a":        `<a class="{{.Class}}" id="{{.Name}}" href="{{.Href}}" {{.AdditionalAttributes}}>{{.Title}}</a>`,
	"button":   `<button type="{{.Type}}" class="{{.Class}}">{{.Label}}</button>`,
}

func renderField(field FieldInfo) string {
	ui, ok := field.Definition["ui"].(map[string]any)
	if !ok {
		return ""
	}

	control, _ := ui["element"].(string)
	class, _ := ui["class"].(string)
	name, _ := ui["name"].(string)
	title, _ := field.Definition["title"].(string)
	placeholder, _ := field.Definition["placeholder"].(string)

	isRequired, _ := field.Definition["isRequired"].(bool)
	required := ""
	titleHTML := title
	if isRequired {
		required = "required"
		titleHTML += ` <span class="required">*</span>`
	}

	inputType := "text"
	if uiType, ok := ui["type"].(string); ok {
		inputType = uiType
	} else if fieldType, ok := field.Definition["type"].(string); ok && fieldType == "email" {
		inputType = "email"
	}

	var additionalAttributes bytes.Buffer
	for key, value := range field.Definition {
		switch key {
		case "title", "ui", "placeholder", "type", "order", "isRequired":
			continue
		default:
			additionalAttributes.WriteString(fmt.Sprintf(` %s="%v"`, key, value))
		}
	}

	data := map[string]any{
		"Class":                class,
		"Name":                 name,
		"Title":                template.HTML(titleHTML),
		"Placeholder":          placeholder,
		"Required":             required,
		"AdditionalAttributes": template.HTML(additionalAttributes.String()),
		"InputType":            inputType,
		"Control":              control,
		"Href":                 "",
	}

	// Handle options for select
	if control == "select" {
		options, _ := ui["options"].([]any)
		var optionsHTML bytes.Buffer
		for _, option := range options {
			optionsHTML.WriteString(fmt.Sprintf(`<option value="%v">%v</option>`, option, option))
		}
		data["OptionsHTML"] = template.HTML(optionsHTML.String())
	}

	// Handle href for links
	if control == "a" {
		if href, ok := ui["href"].(string); ok {
			data["Href"] = href
		}
	}

	if tmplStr, ok := fieldTemplates[control]; ok {
		tmpl := template.Must(template.New(control).Parse(tmplStr))
		var buf bytes.Buffer
		tmpl.Execute(&buf, data)
		return buf.String()
	}
	// Handle generic 'h' template for h1-h6
	if control[0] == 'h' && len(control) == 2 && control[1] >= '1' && control[1] <= '6' {
		data["Control"] = control
		tmpl := template.Must(template.New("h").Parse(fieldTemplates["h"]))
		var buf bytes.Buffer
		tmpl.Execute(&buf, data)
		return buf.String()
	}

	return ""
}

// renderButtons generates HTML for form buttons
func renderButtons(formConfig map[string]any) string {
	var buttonsHTML bytes.Buffer
	tmpl := template.Must(template.New("button").Parse(fieldTemplates["button"]))

	if submitConfig, ok := formConfig["submit"].(map[string]any); ok {
		data := map[string]any{
			"Type":  submitConfig["type"],
			"Class": submitConfig["class"],
			"Label": submitConfig["label"],
		}
		tmpl.Execute(&buttonsHTML, data)
	}

	if resetConfig, ok := formConfig["reset"].(map[string]any); ok {
		data := map[string]any{
			"Type":  resetConfig["type"],
			"Class": resetConfig["class"],
			"Label": resetConfig["label"],
		}
		tmpl.Execute(&buttonsHTML, data)
	}

	return buttonsHTML.String()
}
