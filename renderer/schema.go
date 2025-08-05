package renderer

import (
	"bytes"
	"fmt"
	"html/template"
	"sort"
	"strings"
)

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

// Templates for field rendering - now supports all HTML DOM elements
var fieldTemplates = map[string]string{
	// Form elements
	"input":    `<div class="{{.Class}}">{{.LabelHTML}}<input {{.AllAttributes}} />{{.ContentHTML}}</div>`,
	"textarea": `<div class="{{.Class}}">{{.LabelHTML}}<textarea {{.AllAttributes}}>{{.Content}}</textarea>{{.ContentHTML}}</div>`,
	"select":   `<div class="{{.Class}}">{{.LabelHTML}}<select {{.AllAttributes}}>{{.OptionsHTML}}</select>{{.ContentHTML}}</div>`,
	"button":   `<button {{.AllAttributes}}>{{.Content}}</button>`,
	"option":   `<option {{.AllAttributes}}>{{.Content}}</option>`,
	"optgroup": `<optgroup {{.AllAttributes}}>{{.OptionsHTML}}</optgroup>`,
	"label":    `<label {{.AllAttributes}}>{{.Content}}</label>`,
	"fieldset": `<fieldset {{.AllAttributes}}>{{.ContentHTML}}</fieldset>`,
	"legend":   `<legend {{.AllAttributes}}>{{.Content}}</legend>`,
	"datalist": `<datalist {{.AllAttributes}}>{{.OptionsHTML}}</datalist>`,
	"output":   `<output {{.AllAttributes}}>{{.Content}}</output>`,
	"progress": `<progress {{.AllAttributes}}>{{.Content}}</progress>`,
	"meter":    `<meter {{.AllAttributes}}>{{.Content}}</meter>`,

	// Text content elements
	"h1":         `<h1 {{.AllAttributes}}>{{.Content}}</h1>`,
	"h2":         `<h2 {{.AllAttributes}}>{{.Content}}</h2>`,
	"h3":         `<h3 {{.AllAttributes}}>{{.Content}}</h3>`,
	"h4":         `<h4 {{.AllAttributes}}>{{.Content}}</h4>`,
	"h5":         `<h5 {{.AllAttributes}}>{{.Content}}</h5>`,
	"h6":         `<h6 {{.AllAttributes}}>{{.Content}}</h6>`,
	"p":          `<p {{.AllAttributes}}>{{.Content}}</p>`,
	"div":        `<div {{.AllAttributes}}>{{.ContentHTML}}</div>`,
	"span":       `<span {{.AllAttributes}}>{{.Content}}</span>`,
	"pre":        `<pre {{.AllAttributes}}>{{.Content}}</pre>`,
	"code":       `<code {{.AllAttributes}}>{{.Content}}</code>`,
	"blockquote": `<blockquote {{.AllAttributes}}>{{.ContentHTML}}</blockquote>`,
	"cite":       `<cite {{.AllAttributes}}>{{.Content}}</cite>`,
	"strong":     `<strong {{.AllAttributes}}>{{.Content}}</strong>`,
	"em":         `<em {{.AllAttributes}}>{{.Content}}</em>`,
	"small":      `<small {{.AllAttributes}}>{{.Content}}</small>`,
	"mark":       `<mark {{.AllAttributes}}>{{.Content}}</mark>`,
	"del":        `<del {{.AllAttributes}}>{{.Content}}</del>`,
	"ins":        `<ins {{.AllAttributes}}>{{.Content}}</ins>`,
	"sub":        `<sub {{.AllAttributes}}>{{.Content}}</sub>`,
	"sup":        `<sup {{.AllAttributes}}>{{.Content}}</sup>`,
	"abbr":       `<abbr {{.AllAttributes}}>{{.Content}}</abbr>`,
	"address":    `<address {{.AllAttributes}}>{{.ContentHTML}}</address>`,
	"time":       `<time {{.AllAttributes}}>{{.Content}}</time>`,

	// List elements
	"ul": `<ul {{.AllAttributes}}>{{.ContentHTML}}</ul>`,
	"ol": `<ol {{.AllAttributes}}>{{.ContentHTML}}</ol>`,
	"li": `<li {{.AllAttributes}}>{{.Content}}</li>`,
	"dl": `<dl {{.AllAttributes}}>{{.ContentHTML}}</dl>`,
	"dt": `<dt {{.AllAttributes}}>{{.Content}}</dt>`,
	"dd": `<dd {{.AllAttributes}}>{{.Content}}</dd>`,

	// Links and media
	"a":          `<a {{.AllAttributes}}>{{.Content}}</a>`,
	"img":        `<img {{.AllAttributes}} />`,
	"figure":     `<figure {{.AllAttributes}}>{{.ContentHTML}}</figure>`,
	"figcaption": `<figcaption {{.AllAttributes}}>{{.Content}}</figcaption>`,
	"audio":      `<audio {{.AllAttributes}}>{{.ContentHTML}}</audio>`,
	"video":      `<video {{.AllAttributes}}>{{.ContentHTML}}</video>`,
	"source":     `<source {{.AllAttributes}} />`,
	"track":      `<track {{.AllAttributes}} />`,

	// Table elements
	"table":    `<table {{.AllAttributes}}>{{.ContentHTML}}</table>`,
	"caption":  `<caption {{.AllAttributes}}>{{.Content}}</caption>`,
	"thead":    `<thead {{.AllAttributes}}>{{.ContentHTML}}</thead>`,
	"tbody":    `<tbody {{.AllAttributes}}>{{.ContentHTML}}</tbody>`,
	"tfoot":    `<tfoot {{.AllAttributes}}>{{.ContentHTML}}</tfoot>`,
	"tr":       `<tr {{.AllAttributes}}>{{.ContentHTML}}</tr>`,
	"th":       `<th {{.AllAttributes}}>{{.Content}}</th>`,
	"td":       `<td {{.AllAttributes}}>{{.Content}}</td>`,
	"colgroup": `<colgroup {{.AllAttributes}}>{{.ContentHTML}}</colgroup>`,
	"col":      `<col {{.AllAttributes}} />`,

	// Sectioning elements
	"article": `<article {{.AllAttributes}}>{{.ContentHTML}}</article>`,
	"section": `<section {{.AllAttributes}}>{{.ContentHTML}}</section>`,
	"nav":     `<nav {{.AllAttributes}}>{{.ContentHTML}}</nav>`,
	"aside":   `<aside {{.AllAttributes}}>{{.ContentHTML}}</aside>`,
	"header":  `<header {{.AllAttributes}}>{{.ContentHTML}}</header>`,
	"footer":  `<footer {{.AllAttributes}}>{{.ContentHTML}}</footer>`,
	"main":    `<main {{.AllAttributes}}>{{.ContentHTML}}</main>`,

	// Interactive elements
	"details": `<details {{.AllAttributes}}>{{.ContentHTML}}</details>`,
	"summary": `<summary {{.AllAttributes}}>{{.Content}}</summary>`,
	"dialog":  `<dialog {{.AllAttributes}}>{{.ContentHTML}}</dialog>`,

	// Embedded content
	"iframe":  `<iframe {{.AllAttributes}}>{{.Content}}</iframe>`,
	"embed":   `<embed {{.AllAttributes}} />`,
	"object":  `<object {{.AllAttributes}}>{{.ContentHTML}}</object>`,
	"param":   `<param {{.AllAttributes}} />`,
	"picture": `<picture {{.AllAttributes}}>{{.ContentHTML}}</picture>`,
	"canvas":  `<canvas {{.AllAttributes}}>{{.Content}}</canvas>`,
	"svg":     `<svg {{.AllAttributes}}>{{.ContentHTML}}</svg>`,

	// Meta elements
	"br":  `<br {{.AllAttributes}} />`,
	"hr":  `<hr {{.AllAttributes}} />`,
	"wbr": `<wbr {{.AllAttributes}} />`,

	// Generic template for any unlisted element
	"generic": `<{{.Element}} {{.AllAttributes}}>{{.ContentHTML}}</{{.Element}}>`,
	"void":    `<{{.Element}} {{.AllAttributes}} />`,
}

// Void elements that don't have closing tags
var voidElements = map[string]bool{
	"area": true, "base": true, "br": true, "col": true, "embed": true,
	"hr": true, "img": true, "input": true, "link": true, "meta": true,
	"param": true, "source": true, "track": true, "wbr": true,
}

var standardAttrs = []string{
	"id", "class", "name", "type", "value", "placeholder", "href", "src",
	"alt", "title", "target", "rel", "role", "tabindex", "accesskey",
	"contenteditable", "draggable", "hidden", "spellcheck", "translate",
	"autocomplete", "autofocus", "disabled", "readonly", "required",
	"multiple", "checked", "selected", "defer", "async", "loop", "muted",
	"controls", "autoplay", "preload", "poster", "width", "height",
	"rows", "cols", "size", "maxlength", "minlength", "min", "max",
	"step", "pattern", "accept", "capture", "form", "formaction",
	"formenctype", "formmethod", "formnovalidate", "formtarget",
	"colspan", "rowspan", "headers", "scope", "start", "reversed",
	"datetime", "open", "label", "high", "low", "optimum", "span",
}

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

func renderField(field FieldInfo) string {
	ui, ok := field.Definition["ui"].(map[string]any)
	if !ok {
		return ""
	}

	element, _ := ui["element"].(string)
	if element == "" {
		return ""
	}

	// Build all attributes
	allAttributes := buildAllAttributes(field, ui)

	// Get content
	content := getFieldContent(field.Definition, ui)
	contentHTML := getFieldContentHTML(field.Definition, ui)

	// Generate label if needed
	labelHTML := generateLabel(field, ui)

	data := map[string]any{
		"Element":       element,
		"AllAttributes": template.HTMLAttr(allAttributes),
		"Content":       content,
		"ContentHTML":   template.HTML(contentHTML),
		"LabelHTML":     template.HTML(labelHTML),
		"Class":         getUIValue(ui, "class"),
		"OptionsHTML":   template.HTML(generateOptions(ui)),
	}

	// Use specific template if available, otherwise use generic
	var tmplStr string
	if template, exists := fieldTemplates[element]; exists {
		tmplStr = template
	} else if voidElements[element] {
		tmplStr = fieldTemplates["void"]
	} else {
		tmplStr = fieldTemplates["generic"]
	}

	tmpl := template.Must(template.New(element).Parse(tmplStr))
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return ""
	}
	return buf.String()
}

func buildAllAttributes(field FieldInfo, ui map[string]any) string {
	var attributes []string

	// Add standard attributes from ui
	for _, attr := range standardAttrs {
		if value, exists := ui[attr]; exists {
			if attr == "class" && value == "" {
				continue // Skip empty class
			}
			attributes = append(attributes, fmt.Sprintf(`%s="%v"`, attr, value))
		}
	}

	// Handle required field
	if isRequired, ok := field.Definition["isRequired"].(bool); ok && isRequired {
		if !contains(attributes, "required=") {
			attributes = append(attributes, `required="required"`)
		}
	}

	// Handle input type based on field type
	element, _ := ui["element"].(string)
	if element == "input" {
		if inputType := getInputType(field.Definition, ui); inputType != "" {
			if !contains(attributes, "type=") {
				attributes = append(attributes, fmt.Sprintf(`type="%s"`, inputType))
			}
		}
	}

	// Add data-* and aria-* attributes
	for key, value := range ui {
		if strings.HasPrefix(key, "data-") || strings.HasPrefix(key, "aria-") {
			attributes = append(attributes, fmt.Sprintf(`%s="%v"`, key, value))
		}
	}

	// Add custom attributes from field definition (excluding known schema properties)
	excludeFields := map[string]bool{
		"type": true, "title": true, "ui": true, "placeholder": true,
		"order": true, "isRequired": true, "content": true, "children": true,
	}

	for key, value := range field.Definition {
		if !excludeFields[key] && !strings.HasPrefix(key, "ui") {
			attributes = append(attributes, fmt.Sprintf(`%s="%v"`, key, value))
		}
	}

	return strings.Join(attributes, " ")
}

func getInputType(fieldDef map[string]any, ui map[string]any) string {
	// Check ui type first
	if uiType, ok := ui["type"].(string); ok {
		return uiType
	}

	// Map schema types to input types
	if fieldType, ok := fieldDef["type"].(string); ok {
		switch fieldType {
		case "email":
			return "email"
		case "password":
			return "password"
		case "number", "integer":
			return "number"
		case "boolean":
			return "checkbox"
		case "date":
			return "date"
		case "time":
			return "time"
		case "datetime":
			return "datetime-local"
		case "url":
			return "url"
		case "tel":
			return "tel"
		case "color":
			return "color"
		case "range":
			return "range"
		case "file":
			return "file"
		case "hidden":
			return "hidden"
		default:
			return "text"
		}
	}

	return "text"
}

func getFieldContent(fieldDef map[string]any, ui map[string]any) string {
	// Check for content in ui first
	if content, ok := ui["content"].(string); ok {
		return content
	}

	// Check for content in field definition
	if content, ok := fieldDef["content"].(string); ok {
		return content
	}

	// Use title as fallback for some elements
	if title, ok := fieldDef["title"].(string); ok {
		return title
	}

	return ""
}

func getFieldContentHTML(fieldDef map[string]any, ui map[string]any) string {
	// Check for HTML content in ui
	if contentHTML, ok := ui["contentHTML"].(string); ok {
		return contentHTML
	}

	// Check for children elements
	if children, ok := ui["children"].([]any); ok {
		return renderChildren(children)
	}

	return ""
}

func renderChildren(children []any) string {
	var result strings.Builder
	for _, child := range children {
		if childMap, ok := child.(map[string]any); ok {
			// Create a temporary field info for the child
			childField := FieldInfo{
				Name:       getMapValue(childMap, "name", ""),
				Definition: childMap,
			}
			result.WriteString(renderField(childField))
		}
	}
	return result.String()
}

func generateLabel(field FieldInfo, ui map[string]any) string {
	// Check if label should be generated
	if showLabel, ok := ui["showLabel"].(bool); !showLabel && ok {
		return ""
	}

	title, _ := field.Definition["title"].(string)
	if title == "" {
		return ""
	}

	name := getUIValue(ui, "name")
	if name == "" {
		name = field.Name
	}

	// Check if field is required
	isRequired, _ := field.Definition["isRequired"].(bool)
	requiredSpan := ""
	if isRequired {
		requiredSpan = ` <span class="required">*</span>`
	}

	return fmt.Sprintf(`<label for="%s">%s%s</label>`, name, title, requiredSpan)
}

func generateOptions(ui map[string]any) string {
	options, ok := ui["options"].([]any)
	if !ok {
		return ""
	}

	var optionsHTML strings.Builder
	for _, option := range options {
		if optionMap, ok := option.(map[string]any); ok {
			// Complex option with attributes
			value := getMapValue(optionMap, "value", "")
			text := getMapValue(optionMap, "text", value)
			selected := ""
			if isSelected, ok := optionMap["selected"].(bool); ok && isSelected {
				selected = ` selected="selected"`
			}
			disabled := ""
			if isDisabled, ok := optionMap["disabled"].(bool); ok && isDisabled {
				disabled = ` disabled="disabled"`
			}
			optionsHTML.WriteString(fmt.Sprintf(`<option value="%s"%s%s>%s</option>`,
				value, selected, disabled, text))
		} else {
			// Simple option (just value)
			optionsHTML.WriteString(fmt.Sprintf(`<option value="%v">%v</option>`, option, option))
		}
	}
	return optionsHTML.String()
}

func getUIValue(ui map[string]any, key string) string {
	if value, ok := ui[key].(string); ok {
		return value
	}
	return ""
}

func getMapValue(m map[string]any, key, defaultValue string) string {
	if value, ok := m[key].(string); ok {
		return value
	}
	return defaultValue
}

func contains(slice []string, substr string) bool {
	for _, item := range slice {
		if strings.Contains(item, substr) {
			return true
		}
	}
	return false
}

// renderButtons generates HTML for form buttons
func renderButtons(formConfig map[string]any) string {
	var buttonsHTML bytes.Buffer

	if submitConfig, ok := formConfig["submit"].(map[string]any); ok {
		buttonHTML := renderButtonFromConfig(submitConfig, "submit")
		buttonsHTML.WriteString(buttonHTML)
	}

	if resetConfig, ok := formConfig["reset"].(map[string]any); ok {
		buttonHTML := renderButtonFromConfig(resetConfig, "reset")
		buttonsHTML.WriteString(buttonHTML)
	}

	// Support for additional custom buttons
	if buttons, ok := formConfig["buttons"].([]any); ok {
		for _, button := range buttons {
			if buttonMap, ok := button.(map[string]any); ok {
				buttonType := getMapValue(buttonMap, "type", "button")
				buttonHTML := renderButtonFromConfig(buttonMap, buttonType)
				buttonsHTML.WriteString(buttonHTML)
			}
		}
	}

	return buttonsHTML.String()
}

func renderButtonFromConfig(config map[string]any, defaultType string) string {
	var attributes []string

	buttonType := getMapValue(config, "type", defaultType)
	attributes = append(attributes, fmt.Sprintf(`type="%s"`, buttonType))

	if class := getMapValue(config, "class", ""); class != "" {
		attributes = append(attributes, fmt.Sprintf(`class="%s"`, class))
	}

	// Add other button attributes
	for key, value := range config {
		switch key {
		case "type", "class", "label", "content":
			continue // Already handled
		default:
			attributes = append(attributes, fmt.Sprintf(`%s="%v"`, key, value))
		}
	}

	content := getMapValue(config, "label", getMapValue(config, "content", "Button"))

	return fmt.Sprintf(`<button %s>%s</button>`,
		strings.Join(attributes, " "), content)
}
