package renderer

import (
	"bytes"
	"fmt"
	"html/template"
	"sort"
	"strings"

	"github.com/oarkflow/jsonschema"
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
	FieldPath  string // Full path for nested fields (e.g., "user.address.street")
	Order      int
	Schema     *jsonschema.Schema
	IsRequired bool
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
	Schema     *jsonschema.Schema
	HTMLLayout string
	cachedHTML string // Cached rendered HTML
}

// NewJSONSchemaRenderer creates a new instance of JSONSchemaRenderer
func NewJSONSchemaRenderer(schema *jsonschema.Schema, htmlLayout string) *JSONSchemaRenderer {
	return &JSONSchemaRenderer{
		Schema:     schema,
		HTMLLayout: htmlLayout,
	}
}

// interpolateTemplate replaces template placeholders with actual values
func (r *JSONSchemaRenderer) interpolateTemplate(templateStr string, data map[string]any) string {
	if len(data) == 0 {
		return templateStr
	}

	tmpl, err := template.New("interpolate").Parse(templateStr)
	if err != nil {
		// Fallback to simple string replacement if template parsing fails
		result := templateStr
		for key, value := range data {
			placeholder := fmt.Sprintf("{{%s}}", key)
			if valueStr, ok := value.(string); ok {
				result = strings.ReplaceAll(result, placeholder, valueStr)
			}
		}
		return result
	}

	var templateResult bytes.Buffer
	err = tmpl.Execute(&templateResult, data)
	if err != nil {
		return templateStr // Return original string if execution fails
	}

	return templateResult.String()
}

// RenderFields generates HTML for fields based on the JSONSchema
func (r *JSONSchemaRenderer) RenderFields(data map[string]any) (string, error) {
	// Return cached HTML if available
	if r.cachedHTML != "" {
		return r.cachedHTML, nil
	}

	groups := r.parseGroupsFromSchema()
	var groupHTML bytes.Buffer
	for _, group := range groups {
		groupHTML.WriteString(renderGroup(group))
	}

	// Extract form configuration
	var formClass, formAction, formMethod, formEnctype string
	if r.Schema.Form != nil {
		if class, ok := r.Schema.Form["class"].(string); ok {
			formClass = class
		}
		if action, ok := r.Schema.Form["action"].(string); ok {
			formAction = r.interpolateTemplate(action, data)
		}
		if method, ok := r.Schema.Form["method"].(string); ok {
			formMethod = method
		}
		if enctype, ok := r.Schema.Form["enctype"].(string); ok {
			formEnctype = enctype
		}
	}

	buttonsHTML := r.renderButtons()

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
func (r *JSONSchemaRenderer) parseGroupsFromSchema() []GroupInfo {
	if r.Schema.Form == nil {
		return nil
	}

	groupsData, ok := r.Schema.Form["groups"]
	if !ok {
		return nil
	}

	groups, ok := groupsData.([]interface{})
	if !ok {
		return nil
	}

	var result []GroupInfo
	for _, group := range groups {
		groupMap, ok := group.(map[string]interface{})
		if !ok {
			continue
		}

		var groupTitle GroupTitle
		if titleMap, ok := groupMap["title"].(map[string]interface{}); ok {
			if text, ok := titleMap["text"].(string); ok {
				groupTitle.Text = text
			}
			if class, ok := titleMap["class"].(string); ok {
				groupTitle.Class = class
			}
		}

		groupClass, _ := groupMap["class"].(string)
		if groupClass == "" {
			groupClass = "form-group-fields"
		}

		var fields []FieldInfo
		if fieldsData, ok := groupMap["fields"].([]interface{}); ok {
			for _, fieldName := range fieldsData {
				if fieldNameStr, ok := fieldName.(string); ok {
					// Handle nested field paths
					fieldInfos := r.extractFieldsFromPath(fieldNameStr, "")
					fields = append(fields, fieldInfos...)
				}
			}
		}

		// Sort fields by order
		sort.Slice(fields, func(i, j int) bool {
			orderI := 0
			orderJ := 0
			if fields[i].Schema.Order != nil {
				orderI = *fields[i].Schema.Order
			}
			if fields[j].Schema.Order != nil {
				orderJ = *fields[j].Schema.Order
			}
			return orderI < orderJ
		})

		result = append(result, GroupInfo{
			Title:      groupTitle,
			Fields:     fields,
			GroupClass: groupClass,
		})
	}
	return result
}

// extractFieldsFromPath recursively extracts fields from a path, handling nested properties
func (r *JSONSchemaRenderer) extractFieldsFromPath(fieldPath, parentPath string) []FieldInfo {
	var fields []FieldInfo

	// Build the full path
	fullPath := fieldPath
	if parentPath != "" {
		fullPath = parentPath + "." + fieldPath
	}

	// Navigate to the schema at this path
	schema := r.getSchemaAtPath(fieldPath)
	if schema == nil {
		return fields
	}

	// Check if this field is required
	isRequired := r.isFieldRequired(fieldPath)

	// If this schema has properties, it's a nested object
	if schema.Properties != nil && len(*schema.Properties) > 0 {
		// Recursively process nested properties
		for propName, propSchema := range *schema.Properties {
			nestedFields := r.extractFieldsFromNestedSchema(propName, fullPath, propSchema, isRequired)
			fields = append(fields, nestedFields...)
		}
	} else {
		// This is a leaf field
		order := 0
		if schema.Order != nil {
			order = *schema.Order
		}

		fields = append(fields, FieldInfo{
			Name:       fieldPath,
			FieldPath:  fullPath,
			Order:      order,
			Schema:     schema,
			IsRequired: isRequired,
		})
	}

	return fields
}

// extractFieldsFromNestedSchema processes nested schema properties
func (r *JSONSchemaRenderer) extractFieldsFromNestedSchema(propName, parentPath string, propSchema *jsonschema.Schema, parentRequired bool) []FieldInfo {
	var fields []FieldInfo

	fullPath := propName
	if parentPath != "" {
		fullPath = parentPath + "." + propName
	}

	// Check if this nested field is required
	isRequired := parentRequired || contains(r.Schema.Required, propName)

	// If this property has nested properties, recurse
	if propSchema.Properties != nil && len(*propSchema.Properties) > 0 {
		for nestedPropName, nestedPropSchema := range *propSchema.Properties {
			nestedFields := r.extractFieldsFromNestedSchema(nestedPropName, fullPath, nestedPropSchema, isRequired)
			fields = append(fields, nestedFields...)
		}
	} else {
		// This is a leaf field
		order := 0
		if propSchema.Order != nil {
			order = *propSchema.Order
		}

		fields = append(fields, FieldInfo{
			Name:       propName,
			FieldPath:  fullPath,
			Order:      order,
			Schema:     propSchema,
			IsRequired: isRequired,
		})
	}

	return fields
}

// getSchemaAtPath navigates to a schema at a given path
func (r *JSONSchemaRenderer) getSchemaAtPath(path string) *jsonschema.Schema {
	if r.Schema.Properties == nil {
		return nil
	}

	parts := strings.Split(path, ".")
	currentSchema := r.Schema

	for _, part := range parts {
		if currentSchema.Properties == nil {
			return nil
		}

		if propSchema, exists := (*currentSchema.Properties)[part]; exists {
			currentSchema = propSchema
		} else {
			return nil
		}
	}

	return currentSchema
}

// isFieldRequired checks if a field is required at the current schema level
func (r *JSONSchemaRenderer) isFieldRequired(fieldName string) bool {
	return contains(r.Schema.Required, fieldName)
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

	if err := tmpl.Execute(&groupHTML, data); err != nil {
		return "" // Return empty string on error
	}

	return groupHTML.String()
}

func renderField(field FieldInfo) string {
	if field.Schema.UI == nil {
		return ""
	}

	element, ok := field.Schema.UI["element"].(string)
	if !ok || element == "" {
		return ""
	}

	// Build all attributes
	allAttributes := buildAllAttributes(field)

	// Get content
	content := getFieldContent(field)
	contentHTML := getFieldContentHTML(field)

	// Generate label if needed
	labelHTML := generateLabel(field)

	data := map[string]any{
		"Element":       element,
		"AllAttributes": template.HTMLAttr(allAttributes),
		"Content":       content,
		"ContentHTML":   template.HTML(contentHTML),
		"LabelHTML":     template.HTML(labelHTML),
		"Class":         getUIValue(field.Schema.UI, "class"),
		"OptionsHTML":   template.HTML(generateOptions(field.Schema.UI)),
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

func buildAllAttributes(field FieldInfo) string {
	var attributes []string

	// Use the field path as the name attribute for nested fields
	fieldName := field.FieldPath
	if fieldName == "" {
		fieldName = field.Name
	}

	// Add name attribute
	attributes = append(attributes, fmt.Sprintf(`name="%s"`, fieldName))

	// Add standard attributes from UI
	if field.Schema.UI != nil {
		element, _ := field.Schema.UI["element"].(string)
		for _, attr := range standardAttrs {
			if attr == "name" {
				continue // Already handled above
			}
			if value, exists := field.Schema.UI[attr]; exists {
				if attr == "class" && value == "" {
					continue // Skip empty class
				}
				// For select, input, textarea, add class to element itself
				if attr == "class" && (element == "select" || element == "input" || element == "textarea") {
					attributes = append(attributes, fmt.Sprintf(`class="%v"`, value))
					continue
				}
				// For other elements, do not add class here (it will be handled in the wrapper div)
				if attr == "class" {
					continue
				}
				attributes = append(attributes, fmt.Sprintf(`%s="%v"`, attr, value))
			}
		}

		// Add data-* and aria-* attributes
		for key, value := range field.Schema.UI {
			if strings.HasPrefix(key, "data-") || strings.HasPrefix(key, "aria-") {
				attributes = append(attributes, fmt.Sprintf(`%s="%v"`, key, value))
			}
		}
	}

	// Handle required field
	if field.IsRequired {
		if !containsAttribute(attributes, "required=") {
			attributes = append(attributes, `required="required"`)
		}
	}

	// Handle input type based on field type
	element, _ := field.Schema.UI["element"].(string)
	if element == "input" {
		if inputType := getInputType(field.Schema); inputType != "" {
			if !containsAttribute(attributes, "type=") {
				attributes = append(attributes, fmt.Sprintf(`type="%s"`, inputType))
			}
		}
	}

	return strings.Join(attributes, " ")
}

func getInputType(schema *jsonschema.Schema) string {
	// Check UI type first
	if schema.UI != nil {
		if uiType, ok := schema.UI["type"].(string); ok {
			return uiType
		}
	}
	var typeStr string
	if len(schema.Type) > 0 {
		typeStr = schema.Type[0]
	} else {
		typeStr = "string"
	}
	// Map schema types to input types
	switch typeStr {
	case "string", "text":
		return "text"
	case "number", "integer":
		return "number"
	case "boolean":
		return "checkbox"
	default:
		return "text"
	}
}

func getFieldContent(field FieldInfo) string {
	// Check for content in UI first
	if field.Schema.UI != nil {
		if content, ok := field.Schema.UI["content"].(string); ok {
			return content
		}
	}

	// Use title as fallback for some elements
	if field.Schema.Title != nil {
		return *field.Schema.Title
	}

	return ""
}

func getFieldContentHTML(field FieldInfo) string {
	// Check for HTML content in UI
	if field.Schema.UI != nil {
		if contentHTML, ok := field.Schema.UI["contentHTML"].(string); ok {
			return contentHTML
		}

		// Check for children elements
		if children, ok := field.Schema.UI["children"].([]interface{}); ok {
			return renderChildren(children)
		}
	}

	return ""
}

func renderChildren(children []interface{}) string {
	var result strings.Builder
	for _, child := range children {
		if childMap, ok := child.(map[string]interface{}); ok {
			// Create a temporary field info for the child
			childSchema := &jsonschema.Schema{
				UI: childMap,
			}
			if title, ok := childMap["title"].(string); ok {
				childSchema.Title = &title
			}

			childField := FieldInfo{
				Name:   getMapValue(childMap, "name", ""),
				Schema: childSchema,
			}
			result.WriteString(renderField(childField))
		}
	}
	return result.String()
}

func generateLabel(field FieldInfo) string {
	// Check if label should be generated
	if field.Schema.UI != nil {
		if showLabel, ok := field.Schema.UI["showLabel"].(bool); !showLabel && ok {
			return ""
		}
	}

	var title string
	if field.Schema.Title != nil {
		title = *field.Schema.Title
	}
	if title == "" {
		return ""
	}

	fieldName := field.FieldPath
	if fieldName == "" {
		fieldName = field.Name
	}

	// Check if field is required
	requiredSpan := ""
	if field.IsRequired {
		requiredSpan = ` <span class="required">*</span>`
	}

	return fmt.Sprintf(`<label for="%s">%s%s</label>`, fieldName, title, requiredSpan)
}

func generateOptions(ui map[string]interface{}) string {
	if ui == nil {
		return ""
	}

	options, ok := ui["options"].([]interface{})
	if !ok {
		return ""
	}

	var optionsHTML strings.Builder
	for _, option := range options {
		if optionMap, ok := option.(map[string]interface{}); ok {
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

func getUIValue(ui map[string]interface{}, key string) string {
	if ui == nil {
		return ""
	}
	if value, ok := ui[key].(string); ok {
		return value
	}
	return ""
}

func getMapValue(m map[string]interface{}, key, defaultValue string) string {
	if value, ok := m[key].(string); ok {
		return value
	}
	return defaultValue
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func containsAttribute(attributes []string, prefix string) bool {
	for _, attr := range attributes {
		if strings.HasPrefix(attr, prefix) {
			return true
		}
	}
	return false
}

// renderButtons generates HTML for form buttons
func (r *JSONSchemaRenderer) renderButtons() string {
	if r.Schema.Form == nil {
		return ""
	}

	var buttonsHTML bytes.Buffer

	if submitConfig, ok := r.Schema.Form["submit"].(map[string]interface{}); ok {
		buttonHTML := renderButtonFromConfig(submitConfig, "submit")
		buttonsHTML.WriteString(buttonHTML)
	}

	if resetConfig, ok := r.Schema.Form["reset"].(map[string]interface{}); ok {
		buttonHTML := renderButtonFromConfig(resetConfig, "reset")
		buttonsHTML.WriteString(buttonHTML)
	}

	// Support for additional custom buttons
	if buttons, ok := r.Schema.Form["buttons"].([]interface{}); ok {
		for _, button := range buttons {
			if buttonMap, ok := button.(map[string]interface{}); ok {
				buttonType := getMapValue(buttonMap, "type", "button")
				buttonHTML := renderButtonFromConfig(buttonMap, buttonType)
				buttonsHTML.WriteString(buttonHTML)
			}
		}
	}

	return buttonsHTML.String()
}

func renderButtonFromConfig(config map[string]interface{}, defaultType string) string {
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
