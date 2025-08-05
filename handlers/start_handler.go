package handlers

import (
	"context"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

type StartHandler struct {
	dag.Operation
}

func (e *StartHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

func NewStartHandler(id string) *StartHandler {
	return &StartHandler{
		Operation: dag.Operation{ID: id, Key: "start", Type: dag.Function, Tags: []string{"built-in"}},
	}
}
