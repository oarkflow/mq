package renderer

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"sort"
	"strings"
	"sync"

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
	"input":        `<div class="{{.Class}}">{{.LabelHTML}}<input {{.AllAttributes}} />{{.ContentHTML}}</div>`,
	"input_hidden": `<input {{.AllAttributes}} />`, // Special template for hidden inputs without wrapper
	"textarea":     `<div class="{{.Class}}">{{.LabelHTML}}<textarea {{.AllAttributes}}>{{.Content}}</textarea>{{.ContentHTML}}</div>`,
	"select":       `<div class="{{.Class}}">{{.LabelHTML}}<select {{.AllAttributes}}>{{.OptionsHTML}}</select>{{.ContentHTML}}</div>`,
	"button":       `<button {{.AllAttributes}}>{{.Content}}</button>`,
	"option":       `<option {{.AllAttributes}}>{{.Content}}</option>`,
	"optgroup":     `<optgroup {{.AllAttributes}}>{{.OptionsHTML}}</optgroup>`,
	"label":        `<label {{.AllAttributes}}>{{.Content}}</label>`,
	"fieldset":     `<fieldset {{.AllAttributes}}>{{.ContentHTML}}</fieldset>`,
	"legend":       `<legend {{.AllAttributes}}>{{.Content}}</legend>`,
	"datalist":     `<datalist {{.AllAttributes}}>{{.OptionsHTML}}</datalist>`,
	"output":       `<output {{.AllAttributes}}>{{.Content}}</output>`,
	"progress":     `<progress {{.AllAttributes}}>{{.Content}}</progress>`,
	"meter":        `<meter {{.AllAttributes}}>{{.Content}}</meter>`,

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

// Template cache for compiled templates
var (
	compiledFieldTemplates = make(map[string]*template.Template)
	compiledGroupTemplate  *template.Template
	templateCacheMutex     sync.RWMutex
)

// Initialize compiled templates once
func init() {
	var err error
	compiledGroupTemplate, err = template.New("group").Parse(groupTemplateStr)
	if err != nil {
		panic(fmt.Sprintf("Failed to compile group template: %v", err))
	}

	templateCacheMutex.Lock()
	defer templateCacheMutex.Unlock()

	for element, tmplStr := range fieldTemplates {
		compiled, err := template.New(element).Parse(tmplStr)
		if err == nil {
			compiledFieldTemplates[element] = compiled
		}
	}
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
	Validation ValidationInfo // Add validation information
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
	Schema         *jsonschema.Schema
	HTMLLayout     string
	compiledLayout *template.Template
	cachedGroups   []GroupInfo
	cachedButtons  string
	formConfig     FormConfig
	cacheMutex     sync.RWMutex
}

// FormConfig holds cached form configuration
type FormConfig struct {
	Class   string
	Action  string
	Method  string
	Enctype string
}

// NewJSONSchemaRenderer creates a new instance of JSONSchemaRenderer
func NewJSONSchemaRenderer(schema *jsonschema.Schema, htmlLayout string) *JSONSchemaRenderer {
	renderer := &JSONSchemaRenderer{
		Schema:     schema,
		HTMLLayout: htmlLayout,
	}

	// Pre-compile layout template
	renderer.compileLayoutTemplate()

	// Pre-parse and cache groups and form config
	renderer.precomputeStaticData()

	return renderer
}

// compileLayoutTemplate pre-compiles the layout template
func (r *JSONSchemaRenderer) compileLayoutTemplate() {
	tmpl, err := template.New("layout").Funcs(template.FuncMap{
		"form_groups": func(groupsHTML string) template.HTML {
			return template.HTML(groupsHTML)
		},
		"form_buttons": func() template.HTML {
			return template.HTML(r.cachedButtons)
		},
		"form_attributes": func(formAction string) template.HTMLAttr {
			return template.HTMLAttr(fmt.Sprintf(`class="%s" action="%s" method="%s" enctype="%s"`,
				r.formConfig.Class, formAction, r.formConfig.Method, r.formConfig.Enctype))
		},
	}).Parse(r.HTMLLayout)

	if err == nil {
		r.compiledLayout = tmpl
	}
}

// precomputeStaticData caches groups and form configuration
func (r *JSONSchemaRenderer) precomputeStaticData() {
	r.cachedGroups = r.parseGroupsFromSchema()
	r.cachedButtons = r.renderButtons()

	// Cache form configuration
	if r.Schema.Form != nil {
		if class, ok := r.Schema.Form["class"].(string); ok {
			r.formConfig.Class = class
		}
		if action, ok := r.Schema.Form["action"].(string); ok {
			r.formConfig.Action = action
		}
		if method, ok := r.Schema.Form["method"].(string); ok {
			r.formConfig.Method = method
		}
		if enctype, ok := r.Schema.Form["enctype"].(string); ok {
			r.formConfig.Enctype = enctype
		}
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
	r.cacheMutex.RLock()
	defer r.cacheMutex.RUnlock()

	// Use a string builder for efficient string concatenation
	var groupHTML strings.Builder
	groupHTML.Grow(1024) // Pre-allocate reasonable capacity

	for _, group := range r.cachedGroups {
		groupHTML.WriteString(renderGroupWithData(group, data))
	}

	// Interpolate dynamic form action
	formAction := r.interpolateTemplate(r.formConfig.Action, data)

	// Use pre-compiled template if available
	if r.compiledLayout != nil {
		var renderedHTML bytes.Buffer

		templateData := struct {
			GroupsHTML string
			FormAction string
		}{
			GroupsHTML: groupHTML.String(),
			FormAction: formAction,
		}

		// Update the template functions with current data
		updatedTemplate := r.compiledLayout.Funcs(template.FuncMap{
			"form_groups": func() template.HTML {
				return template.HTML(templateData.GroupsHTML)
			},
			"form_buttons": func() template.HTML {
				return template.HTML(r.cachedButtons)
			},
			"form_attributes": func() template.HTMLAttr {
				return template.HTMLAttr(fmt.Sprintf(`class="%s" action="%s" method="%s" enctype="%s"`,
					r.formConfig.Class, templateData.FormAction, r.formConfig.Method, r.formConfig.Enctype))
			},
		})

		if err := updatedTemplate.Execute(&renderedHTML, nil); err != nil {
			return "", fmt.Errorf("failed to execute compiled template: %w", err)
		}

		return renderedHTML.String(), nil
	}

	// Fallback to original method if compilation failed
	return r.renderFieldsFallback(data)
}

// renderFieldsFallback provides fallback rendering when template compilation fails
func (r *JSONSchemaRenderer) renderFieldsFallback(data map[string]any) (string, error) {
	var groupHTML strings.Builder
	for _, group := range r.cachedGroups {
		groupHTML.WriteString(renderGroupWithData(group, data))
	}

	formAction := r.interpolateTemplate(r.formConfig.Action, data)

	// Create a new template with the layout and functions
	tmpl, err := template.New("layout").Funcs(template.FuncMap{
		"form_groups": func() template.HTML {
			return template.HTML(groupHTML.String())
		},
		"form_buttons": func() template.HTML {
			return template.HTML(r.cachedButtons)
		},
		"form_attributes": func() template.HTMLAttr {
			return template.HTMLAttr(fmt.Sprintf(`class="%s" action="%s" method="%s" enctype="%s"`,
				r.formConfig.Class, formAction, r.formConfig.Method, r.formConfig.Enctype))
		},
	}).Parse(r.HTMLLayout)
	if err != nil {
		return "", fmt.Errorf("failed to parse HTML layout: %w", err)
	}

	var renderedHTML bytes.Buffer
	if err := tmpl.Execute(&renderedHTML, nil); err != nil {
		return "", fmt.Errorf("failed to execute HTML template: %w", err)
	}

	return renderedHTML.String(), nil
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
	var groupedFields = make(map[string]bool) // Track fields that are already in groups

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
					// Mark these fields as grouped
					for _, field := range fieldInfos {
						groupedFields[field.FieldPath] = true
					}
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

	// Add ungrouped hidden fields to the first group or create a hidden group
	if r.Schema.Properties != nil {
		var hiddenFields []FieldInfo
		for propName, propSchema := range *r.Schema.Properties {
			if !groupedFields[propName] {
				// Check if this is a hidden input
				if propSchema.UI != nil {
					if element, ok := propSchema.UI["element"].(string); ok && element == "input" {
						if inputType, ok := propSchema.UI["type"].(string); ok && inputType == "hidden" {
							// This is an ungrouped hidden field, add it
							validation := extractValidationInfo(propSchema, false)
							hiddenFields = append(hiddenFields, FieldInfo{
								Name:       propName,
								FieldPath:  propName,
								Order:      0,
								Schema:     propSchema,
								IsRequired: false,
								Validation: validation,
							})
						}
					}
				}
			}
		}

		// If we have hidden fields, add them to the first group or create a new hidden group
		if len(hiddenFields) > 0 {
			if len(result) > 0 {
				// Prepend hidden fields to the first group
				result[0].Fields = append(hiddenFields, result[0].Fields...)
			} else {
				// Create a hidden group
				result = append(result, GroupInfo{
					Title:      GroupTitle{Text: "", Class: ""},
					Fields:     hiddenFields,
					GroupClass: "hidden-fields",
				})
			}
		}
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

	// For nested paths like "company.address.street", we need to check if the final part
	// is required in its immediate parent's schema
	var fieldName string
	var parentSchemaPath string

	pathParts := strings.Split(fieldPath, ".")
	if len(pathParts) > 1 {
		// Extract the field name (last part) and parent path (all but last)
		fieldName = pathParts[len(pathParts)-1]
		parentSchemaPath = strings.Join(pathParts[:len(pathParts)-1], ".")
	} else {
		// Single level field
		fieldName = fieldPath
		parentSchemaPath = parentPath
	}

	// Check if this field is required at the parent level
	isRequired := r.isFieldRequiredAtPath(fieldName, parentSchemaPath)

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
			Name:       fieldName,
			FieldPath:  fullPath,
			Order:      order,
			Schema:     schema,
			IsRequired: isRequired,
			Validation: extractValidationInfo(schema, isRequired),
		})
	}

	return fields
}

// extractFieldsFromNestedSchema processes nested schema properties
func (r *JSONSchemaRenderer) extractFieldsFromNestedSchema(propName, parentPath string, propSchema *jsonschema.Schema, _ bool) []FieldInfo {
	var fields []FieldInfo

	fullPath := propName
	if parentPath != "" {
		fullPath = parentPath + "." + propName
	}

	// Check if this field is required at its immediate parent level
	// The parent schema path is the current parentPath, not one level up
	isRequired := r.isFieldRequiredAtPath(propName, parentPath)

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
			Validation: extractValidationInfo(propSchema, isRequired),
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

// isFieldRequiredAtPath checks if a field is required at a specific schema path
func (r *JSONSchemaRenderer) isFieldRequiredAtPath(fieldName, schemaPath string) bool {
	var schema *jsonschema.Schema

	if schemaPath == "" {
		// Check at root level
		schema = r.Schema
	} else {
		// Navigate to the schema at the given path
		schema = r.getSchemaAtPath(schemaPath)
	}

	if schema == nil {
		return false
	}

	return contains(schema.Required, fieldName)
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

// renderGroupWithData uses pre-compiled templates and string builder for better performance with data interpolation
func renderGroupWithData(group GroupInfo, data map[string]any) string {
	// Collect all validations for dependency checking
	allValidations := make(map[string]ValidationInfo)
	for _, field := range group.Fields {
		allValidations[field.FieldPath] = field.Validation
	}

	// Use string builder for better performance
	var fieldsHTML strings.Builder
	fieldsHTML.Grow(512) // Pre-allocate reasonable capacity

	for _, field := range group.Fields {
		fieldsHTML.WriteString(renderFieldWithContextAndData(field, allValidations, data))
	}

	// Use pre-compiled group template
	templateCacheMutex.RLock()
	groupTemplate := compiledGroupTemplate
	templateCacheMutex.RUnlock()

	if groupTemplate != nil {
		var groupHTML bytes.Buffer
		templateData := map[string]any{
			"Title":      group.Title,
			"GroupClass": group.GroupClass,
			"FieldsHTML": template.HTML(fieldsHTML.String()),
		}

		if err := groupTemplate.Execute(&groupHTML, templateData); err == nil {
			return groupHTML.String()
		}
	}

	// Fallback to original method
	return renderGroup(group)
}

func renderField(field FieldInfo) string {
	return renderFieldWithContext(field, make(map[string]ValidationInfo))
}

// renderFieldWithContext renders a field with access to all field validations for dependency checking
func renderFieldWithContext(field FieldInfo, allValidations map[string]ValidationInfo) string {
	return renderFieldWithContextAndData(field, allValidations, nil)
}

// renderFieldWithContextAndData renders a field with access to all field validations and template data for interpolation
func renderFieldWithContextAndData(field FieldInfo, allValidations map[string]ValidationInfo, data map[string]any) string {
	element := determineFieldElement(field.Schema)
	if element == "" {
		return ""
	}
	allAttributes := buildAttributesWithValidation(field, data)

	content := getFieldContent(field)
	contentHTML := getFieldContentHTML(field)

	var labelHTML string
	if element != "input" || getInputTypeFromSchema(field.Schema) != "hidden" {
		labelHTML = generateLabel(field)
	}

	optionsHTML := generateOptionsFromSchema(field.Schema)
	validationJS := generateClientSideValidation(field.FieldPath, field.Validation, allValidations)

	templateData := map[string]any{
		"Element":       element,
		"AllAttributes": template.HTMLAttr(allAttributes),
		"Content":       content,
		"ContentHTML":   template.HTML(contentHTML + validationJS),
		"LabelHTML":     template.HTML(labelHTML),
		"Class":         getFieldWrapperClass(field.Schema),
		"OptionsHTML":   template.HTML(optionsHTML),
	}

	var tmplStr string
	if element == "input" && getInputTypeFromSchema(field.Schema) == "hidden" {
		if template, exists := fieldTemplates["input_hidden"]; exists {
			tmplStr = template
		} else {
			tmplStr = fieldTemplates["input"]
		}
	} else if template, exists := fieldTemplates[element]; exists {
		tmplStr = template
	} else if voidElements[element] {
		tmplStr = fieldTemplates["void"]
	} else {
		tmplStr = fieldTemplates["generic"]
	}

	tmpl := template.Must(template.New(element).Parse(tmplStr))
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateData); err != nil {
		return ""
	}
	return buf.String()
}

// determineFieldElement determines the appropriate HTML element based on JSON Schema
func determineFieldElement(schema *jsonschema.Schema) string {
	if schema.UI != nil {
		if element, ok := schema.UI["element"].(string); ok {
			return element
		}
	}

	if shouldUseSelect(schema) {
		return "select"
	}

	if shouldUseTextarea(schema) {
		return "textarea"
	}

	var typeStr string
	if len(schema.Type) > 0 {
		typeStr = schema.Type[0]
	}
	switch typeStr {
	case "boolean":
		return "input" // will be type="checkbox"
	case "array":
		return "input" // could be enhanced for array inputs
	case "object":
		return "fieldset" // for nested objects
	default:
		return "input"
	}
}

// buildAttributesWithValidation creates all HTML attributes including validation with data interpolation
func buildAttributesWithValidation(field FieldInfo, data map[string]any) string {
	var builder strings.Builder
	builder.Grow(512) // Pre-allocate capacity

	// Check if UI specifies a custom name, otherwise use field path for nested fields
	var fieldName string
	if field.Schema.UI != nil {
		if customName, exists := field.Schema.UI["name"].(string); exists && customName != "" {
			fieldName = customName
		}
	}

	// Fallback to field path or field name
	if fieldName == "" {
		fieldName = field.FieldPath
		if fieldName == "" {
			fieldName = field.Name
		}
	}

	// Add name attribute
	builder.WriteString(`name="`)
	builder.WriteString(fieldName)
	builder.WriteString(`"`)

	// Add id attribute for accessibility
	fieldId := strings.ReplaceAll(fieldName, ".", "_")
	builder.WriteString(` id="`)
	builder.WriteString(fieldId)
	builder.WriteString(`"`)

	// Determine and add type attribute for input elements
	element := determineFieldElement(field.Schema)
	if element == "input" {
		inputType := getInputTypeFromSchema(field.Schema)
		builder.WriteString(` type="`)
		builder.WriteString(inputType)
		builder.WriteString(`"`)
	}

	// Add validation attributes
	validationAttrs := generateValidationAttributes(field.Validation)
	for _, attr := range validationAttrs {
		builder.WriteString(` `)
		builder.WriteString(attr)
	}

	// Add default value with interpolation
	defaultValue := getDefaultValue(field.Schema)
	if field.Schema.UI != nil {
		if uiValue, exists := field.Schema.UI["value"].(string); exists {
			// Interpolate template values in UI value
			if data != nil {
				defaultValue = interpolateString(uiValue, data)
			} else {
				defaultValue = uiValue
			}
		}
	}
	if defaultValue != "" {
		builder.WriteString(` value="`)
		builder.WriteString(defaultValue)
		builder.WriteString(`"`)
	}

	// Add placeholder with interpolation
	placeholder := getPlaceholder(field.Schema)
	if field.Schema.UI != nil {
		if uiPlaceholder, exists := field.Schema.UI["placeholder"].(string); exists {
			if data != nil {
				placeholder = interpolateString(uiPlaceholder, data)
			} else {
				placeholder = uiPlaceholder
			}
		}
	}
	if placeholder != "" {
		builder.WriteString(` placeholder="`)
		builder.WriteString(placeholder)
		builder.WriteString(`"`)
	}

	// Add standard attributes from UI with interpolation
	if field.Schema.UI != nil {
		for _, attr := range standardAttrs {
			if attr == "name" || attr == "id" || attr == "type" || attr == "required" ||
				attr == "pattern" || attr == "min" || attr == "max" || attr == "minlength" ||
				attr == "maxlength" || attr == "step" || attr == "value" || attr == "placeholder" {
				continue // Already handled above or in validation
			}
			if value, exists := field.Schema.UI[attr]; exists {
				if attr == "class" && value == "" {
					continue // Skip empty class
				}

				// Apply interpolation to string values if data is available
				valueStr := fmt.Sprintf("%v", value)
				if data != nil && strings.Contains(valueStr, "{{") {
					valueStr = interpolateString(valueStr, data)
				}

				builder.WriteString(` `)
				builder.WriteString(attr)
				builder.WriteString(`="`)
				builder.WriteString(valueStr)
				builder.WriteString(`"`)
			}
		}

		// Add data-* and aria-* attributes with interpolation
		for key, value := range field.Schema.UI {
			if strings.HasPrefix(key, "data-") || strings.HasPrefix(key, "aria-") {
				valueStr := fmt.Sprintf("%v", value)
				if data != nil && strings.Contains(valueStr, "{{") {
					valueStr = interpolateString(valueStr, data)
				}

				builder.WriteString(` `)
				builder.WriteString(key)
				builder.WriteString(`="`)
				builder.WriteString(valueStr)
				builder.WriteString(`"`)
			}
		}
	}

	return builder.String()
}

// generateOptionsFromSchema generates option HTML from schema enum or UI options
func generateOptionsFromSchema(schema *jsonschema.Schema) string {
	var optionsHTML strings.Builder

	// Check UI options first
	if schema.UI != nil {
		if options, ok := schema.UI["options"].([]interface{}); ok {
			for _, option := range options {
				if optionMap, ok := option.(map[string]interface{}); ok {
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
					optionsHTML.WriteString(fmt.Sprintf(`<option value="%v">%v</option>`, option, option))
				}
			}
			return optionsHTML.String()
		}
	}

	// Generate options from enum
	if len(schema.Enum) > 0 {
		for _, enumValue := range schema.Enum {
			optionsHTML.WriteString(fmt.Sprintf(`<option value="%v">%v</option>`, enumValue, enumValue))
		}
	}

	return optionsHTML.String()
}

// getFieldWrapperClass determines the CSS class for the field wrapper
func getFieldWrapperClass(schema *jsonschema.Schema) string {
	if schema.UI != nil {
		if class, ok := schema.UI["class"].(string); ok {
			return class
		}
	}
	return "form-group" // default class
}

// getDefaultValue extracts default value from schema
func getDefaultValue(schema *jsonschema.Schema) string {
	if schema.Default != nil {
		return fmt.Sprintf("%v", schema.Default)
	}
	return ""
}

// getPlaceholder extracts placeholder from schema
func getPlaceholder(schema *jsonschema.Schema) string {
	if schema.UI != nil {
		if placeholder, ok := schema.UI["placeholder"].(string); ok {
			return placeholder
		}
	}

	// Auto-generate placeholder from title or description
	if schema.Title != nil {
		return fmt.Sprintf("Enter %s", strings.ToLower(*schema.Title))
	}

	if schema.Description != nil {
		return *schema.Description
	}

	return ""
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

type RequestSchemaTemplate struct {
	Schema   *jsonschema.Schema  `json:"schema"`
	Renderer *JSONSchemaRenderer `json:"template"`
}

var (
	cache           = make(map[string]*RequestSchemaTemplate)
	mu              = &sync.RWMutex{}
	BaseTemplateDir = "templates"
)

// ClearCache clears the template cache
func ClearCache() {
	mu.Lock()
	defer mu.Unlock()
	cache = make(map[string]*RequestSchemaTemplate)
}

// GetCacheSize returns the current cache size
func GetCacheSize() int {
	mu.RLock()
	defer mu.RUnlock()
	return len(cache)
}

func GetFromBytes(schemaContent []byte, template string) (*RequestSchemaTemplate, error) {
	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile(schemaContent)
	if err != nil {
		return nil, fmt.Errorf("error compiling schema: %w", err)
	}

	templatePath := fmt.Sprintf("%s/%s.html", BaseTemplateDir, template)
	htmlLayout, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load template: %w", err)
	}

	renderer := NewJSONSchemaRenderer(schema, string(htmlLayout))
	cachedTemplate := &RequestSchemaTemplate{
		Schema:   schema,
		Renderer: renderer,
	}
	return cachedTemplate, nil
}

func GetFromFile(schemaPath, template string) (*JSONSchemaRenderer, error) {
	path := fmt.Sprintf("%s:%s", schemaPath, template)
	mu.RLock()
	if cached, exists := cache[path]; exists {
		mu.RUnlock()
		return cached.Renderer, nil
	}
	mu.RUnlock()
	mu.Lock()
	defer mu.Unlock()
	if cached, exists := cache[path]; exists {
		return cached.Renderer, nil
	}
	schemaContent, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("error reading schema file: %w", err)
	}
	schemaRenderer, err := GetFromBytes(schemaContent, template)
	if err != nil {
		return nil, fmt.Errorf("error creating renderer from bytes: %w", err)
	}
	cache[path] = schemaRenderer
	return schemaRenderer.Renderer, nil
}

// interpolateString replaces template placeholders in a string with actual values
func interpolateString(templateStr string, data map[string]any) string {
	if len(data) == 0 {
		return templateStr
	}

	// First try Go template interpolation
	tmpl, err := template.New("interpolate").Parse(templateStr)
	if err == nil {
		var templateResult bytes.Buffer
		err = tmpl.Execute(&templateResult, data)
		if err == nil {
			return templateResult.String()
		}
	}

	// Fallback to simple string replacement if template parsing/execution fails
	result := templateStr
	for key, value := range data {
		placeholder := fmt.Sprintf("{{%s}}", key)
		if valueStr, ok := value.(string); ok {
			result = strings.ReplaceAll(result, placeholder, valueStr)
		} else {
			result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", value))
		}
	}
	return result
}
