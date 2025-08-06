package handlers

import (
	"context"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

type OutputHandler struct {
	dag.Operation
}

func (e *OutputHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

func NewOutputHandler(id string) *OutputHandler {
	return &OutputHandler{
		Operation: dag.Operation{ID: id, Key: "output", Type: dag.Function, Tags: []string{"built-in"}},
	}
}
