package handlers

import (
	"context"

	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

type OutputHandler struct {
	dag.Operation
}

func (c *OutputHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var templateData map[string]any
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, &templateData); err != nil {
			return mq.Result{Payload: task.Payload, Error: err, Ctx: ctx}
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
	bt, _ := json.Marshal(templateData)
	return mq.Result{Payload: bt, Ctx: ctx}
}

func NewOutputHandler(id string) *OutputHandler {
	return &OutputHandler{
		Operation: dag.Operation{ID: id, Key: "output", Type: dag.Function, Tags: []string{"built-in"}},
	}
}
