package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"os"

	"github.com/oarkflow/jet"
	"github.com/oarkflow/jsonschema"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/renderer"
)

type RenderHTMLNode struct {
	dag.Operation
	renderer *renderer.JSONSchemaRenderer
}

func (c *RenderHTMLNode) prepareRenderer(schemaFile string, data, templateData map[string]any, template string, templateFiles ...string) (string, error) {
	if c.renderer == nil {
		var templateFile string
		if len(templateFiles) > 0 {
			templateFile = templateFiles[0]
		}
		schema, ok := data["__schema"].(*jsonschema.Schema)
		if !ok {
			return "", fmt.Errorf("schema file %s not found in context", schemaFile)
		}
		if schema == nil {
			return "", fmt.Errorf("schema file %s not found", schemaFile)
		}
		renderer, err := renderer.GetFromSchema(schema, template, templateFile)
		if err != nil {
			return "", fmt.Errorf("failed to get renderer from file %s: %v", schemaFile, err)
		}
		c.renderer = renderer.Renderer
	}
	return c.renderer.RenderFields(templateData)
}

func (c *RenderHTMLNode) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data := c.Payload.Data
	var (
		schemaFile, _   = data["schema_file"].(string)
		templateStr, _  = data["template"].(string)
		templateFile, _ = data["template_file"].(string)
	)
	var templateData map[string]any
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &templateData); err != nil {
			return mq.Result{Payload: task.Payload, Error: err, Ctx: ctx}
		}
	}
	if templateData == nil {
		templateData = make(map[string]any)
	}
	delete(templateData, "html_content")
	if c.Payload.Mapping != nil {
		for k, v := range c.Payload.Mapping {
			_, val := dag.GetVal(ctx, v, templateData)
			templateData[k] = val
		}
	}
	templateData["task_id"] = ctx.Value("task_id")
	var renderedHTML string
	var err error
	parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
	switch {
	// 1. JSONSchema + HTML Template
	case schemaFile != "" && templateStr != "" && templateFile == "":
		fmt.Println("Using JSONSchema and inline HTML template", c.ID)
		renderedHTML, err = c.prepareRenderer(schemaFile, data, templateData, templateStr)
	// 2. JSONSchema + HTML File
	case schemaFile != "" && templateFile != "" && templateStr == "":
		fmt.Println("Using JSONSchema and HTML file", c.ID)
		renderedHTML, err = c.prepareRenderer(schemaFile, data, templateData, "", templateFile)
	// 3. Only JSONSchema
	case (schemaFile != "" || c.renderer != nil) && templateStr == "" && templateFile == "":
		fmt.Println("Using only JSONSchema", c.ID)
		renderedHTML, err = c.prepareRenderer(schemaFile, data, templateData, "")
	// 4. Only HTML Template
	case templateStr != "" && templateFile == "" && schemaFile == "":
		fmt.Println("Using inline HTML template", c.ID)
		tmpl, err := template.New("inline").Parse(templateStr)
		if err != nil {
			return mq.Result{Error: fmt.Errorf("failed to parse template: %v", err), Ctx: ctx}
		}
		var buf bytes.Buffer
		err = tmpl.Execute(&buf, templateData)
		if err != nil {
			return mq.Result{Error: fmt.Errorf("failed to execute template: %v", err), Ctx: ctx}
		}
		renderedHTML = buf.String()
	// 5. Only HTML File
	case templateFile != "" && templateStr == "" && schemaFile == "":
		fmt.Println("Using HTML file", c.ID)
		fileContent, err := os.ReadFile(templateFile)
		if err != nil {
			return mq.Result{Error: fmt.Errorf("failed to read template file: %v", err), Ctx: ctx}
		}
		renderedHTML, err = parser.ParseTemplate(string(fileContent), templateData)
		if err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
	default:
		return mq.Result{Error: fmt.Errorf("no valid rendering approach found"), Ctx: ctx}
	}

	if err != nil {
		return mq.Result{Payload: task.Payload, Error: err, Ctx: ctx, ConditionStatus: "invalid"}
	}

	ctx = context.WithValue(ctx, consts.ContentType, consts.TypeHtml)
	resultData := map[string]any{
		"html_content": renderedHTML,
		"step":         "form",
	}
	bt, _ := json.Marshal(resultData)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func NewRenderHTMLNode(id string) *RenderHTMLNode {
	return &RenderHTMLNode{Operation: dag.Operation{Key: "render-html", ID: id, Type: dag.Page, Tags: []string{"built-in"}}}
}
