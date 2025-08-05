package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"os"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/renderer"
)

type RenderHTMLNode struct {
	dag.Operation
	renderer *renderer.JSONSchemaRenderer
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
			return mq.Result{Payload: task.Payload, Error: err, Ctx: ctx, ConditionStatus: "invalid"}
		}
	}
	if templateData == nil {
		templateData = make(map[string]any)
	}
	templateData["task_id"] = ctx.Value("task_id")

	var renderedHTML string
	var err error

	switch {
	// 1. JSONSchema + HTML Template
	case schemaFile != "" && templateStr != "":
		if c.renderer == nil {
			renderer, err := renderer.GetFromFile(schemaFile, templateStr)
			if err != nil {
				return mq.Result{Error: fmt.Errorf("failed to get renderer from file %s: %v", schemaFile, err), Ctx: ctx}
			}
			c.renderer = renderer
		}
		renderedHTML, err = c.renderer.RenderFields(templateData)
	// 2. JSONSchema + HTML File
	case schemaFile != "" && templateFile != "":
		if c.renderer == nil {
			renderer, err := renderer.GetFromFile(schemaFile, "", templateFile)
			if err != nil {
				return mq.Result{Error: fmt.Errorf("failed to get renderer from file %s: %v", schemaFile, err), Ctx: ctx}
			}
			c.renderer = renderer
		}
		renderedHTML, err = c.renderer.RenderFields(templateData)
	// 3. Only JSONSchema
	case schemaFile != "" || c.renderer != nil:
		if c.renderer == nil {
			renderer, err := renderer.GetFromFile(schemaFile, "")
			if err != nil {
				return mq.Result{Error: fmt.Errorf("failed to get renderer from file %s: %v", schemaFile, err), Ctx: ctx}
			}
			c.renderer = renderer
		}
		renderedHTML, err = c.renderer.RenderFields(templateData)
	// 4. Only HTML Template
	case templateStr != "":
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
	case templateFile != "":
		fileContent, err := os.ReadFile(templateFile)
		if err != nil {
			return mq.Result{Error: fmt.Errorf("failed to read template file: %v", err), Ctx: ctx}
		}
		tmpl, err := template.New("file").Parse(string(fileContent))
		if err != nil {
			return mq.Result{Error: fmt.Errorf("failed to parse template file: %v", err), Ctx: ctx}
		}
		var buf bytes.Buffer
		err = tmpl.Execute(&buf, templateData)
		if err != nil {
			return mq.Result{Error: fmt.Errorf("failed to execute template file: %v", err), Ctx: ctx}
		}
		renderedHTML = buf.String()
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
