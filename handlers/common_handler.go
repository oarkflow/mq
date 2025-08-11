package handlers

import (
	"context"
	"fmt"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

type Loop struct {
	dag.Operation
}

func (e *Loop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("Loop Data")
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

func NewLoop(id string) *Loop {
	return &Loop{
		Operation: dag.Operation{ID: id, Key: "loop", Type: dag.Function, Tags: []string{"built-in"}},
	}
}

var defaultKey = "default"

type Condition struct {
	dag.Operation
	conditions map[string]dag.Condition
}

func (e *Condition) SetConditions(conditions map[string]dag.Condition) {
	e.conditions = conditions
}

func (e *Condition) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	data, err := dag.UnmarshalPayload[map[string]any](ctx, task.Payload)
	if err != nil {
		return mq.Result{Error: err, Ctx: ctx}
	}
	var conditionStatus string
	_, ok := e.conditions[defaultKey]
	for status, condition := range e.conditions {
		if status != defaultKey && condition != nil {
			if condition.Match(data) {
				conditionStatus = status
			}
		}
	}
	if conditionStatus == "" && ok {
		conditionStatus = defaultKey
	}
	return mq.Result{Payload: task.Payload, ConditionStatus: conditionStatus, Ctx: ctx}
}

func NewCondition(id string) *Condition {
	return &Condition{
		Operation: dag.Operation{ID: id, Key: "condition", Type: dag.Function, Tags: []string{"built-in"}},
	}
}
