package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/utils"
)

type OutputHandler struct {
	dag.Operation
}

func (c *OutputHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var templateData map[string]any
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &templateData); err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
	}
	if templateData == nil {
		templateData = make(map[string]any)
	}
	if c.Payload.Mapping != nil {
		for k, v := range c.Payload.Mapping {
			_, val := dag.GetVal(ctx, v, templateData)
			templateData[k] = val
		}
	}
	except, ok := c.Payload.Data["except_fields"].([]string)
	if !ok {
		exceptAny, ok := c.Payload.Data["except_fields"].([]any)
		if ok {
			except = make([]string, len(exceptAny))
			for i, v := range exceptAny {
				except[i], _ = v.(string)
			}
		}
	}

	outputType, _ := c.Payload.Data["output_type"].(string)
	switch outputType {
	case "stdout":
		fmt.Println(templateData)
		templateData["result"] = "Data written to STDOUT"
	case "stderr":
		fmt.Fprintln(os.Stderr, templateData)
		templateData["result"] = "Data written to STDERR"
	case "json":
		encoder := json.NewEncoder(os.Stdout)
		if err := encoder.Encode(templateData); err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		templateData["result"] = "Data written as JSON to STDOUT"
	case "file":
		filePath, _ := c.Payload.Data["file_path"].(string)
		if filePath == "" {
			return mq.Result{Error: fmt.Errorf("file_path not specified"), Ctx: ctx}
		}
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		defer file.Close()
		encoder := json.NewEncoder(file)
		if err := encoder.Encode(templateData); err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		templateData["result"] = map[string]any{
			"file_path": filePath,
			"message":   "Data written to file",
		}
	case "api":
		apiURL, _ := c.Payload.Data["api_url"].(string)
		if apiURL == "" {
			return mq.Result{Error: fmt.Errorf("api_url not specified"), Ctx: ctx}
		}
		jsonData, err := json.Marshal(templateData)
		if err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}

		req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(jsonData))
		if err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		authType, _ := c.Payload.Data["auth_type"].(string)
		switch authType {
		case "basic":
			username, _ := c.Payload.Data["username"].(string)
			password, _ := c.Payload.Data["password"].(string)
			req.SetBasicAuth(username, password)
		case "bearer":
			token, _ := c.Payload.Data["token"].(string)
			req.Header.Set("Authorization", "Bearer "+token)
		case "api_key":
			apiKey, _ := c.Payload.Data["api_key"].(string)
			headerName, _ := c.Payload.Data["api_key_header"].(string)
			if headerName == "" {
				headerName = "X-API-Key"
			}
			req.Header.Set(headerName, apiKey)
		}

		if headers, ok := c.Payload.Data["headers"].(map[string]string); ok {
			for key, value := range headers {
				req.Header.Set(key, value)
			}
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return mq.Result{Error: err, Ctx: ctx}
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			return mq.Result{Error: fmt.Errorf("API request failed with status: %d", resp.StatusCode), Ctx: ctx}
		}
		templateData["result"] = map[string]any{
			"status_code": resp.StatusCode,
			"message":     "Data sent to API successfully",
		}
	}

	bt, _ := json.Marshal(templateData)
	for _, field := range except {
		utils.RemoveRecursiveFromJSON(bt, field)
	}
	return mq.Result{Payload: bt, Ctx: ctx}
}

func NewOutputHandler(id string) *OutputHandler {
	return &OutputHandler{
		Operation: dag.Operation{ID: id, Key: "output", Type: dag.Function, Tags: []string{"built-in"}},
	}
}
