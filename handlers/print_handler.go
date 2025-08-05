package handlers

import (
	"context"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

type PrintHandler struct {
	dag.Operation
}

func (e *PrintHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	e.Debug(ctx, task)
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

func NewPrintHandler(id string) *PrintHandler {
	return &PrintHandler{
		Operation: dag.Operation{ID: id, Key: "print", Type: dag.Function},
	}
}
