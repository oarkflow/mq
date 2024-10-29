package tasks

import (
	"context"

	"github.com/oarkflow/json"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

type GetData struct {
	dag.Operation
}

func (e *GetData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type Loop struct {
	dag.Operation
}

func (e *Loop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type Condition struct {
	dag.Operation
}

func (e *Condition) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	switch email := data["email"].(type) {
	case string:
		if email == "abc.xyz@gmail.com" {
			return mq.Result{Payload: task.Payload, ConditionStatus: "pass", Ctx: ctx}
		}
		return mq.Result{Payload: task.Payload, ConditionStatus: "fail", Ctx: ctx}
	default:
		return mq.Result{Payload: task.Payload, ConditionStatus: "fail", Ctx: ctx}
	}
}

type PrepareEmail struct {
	dag.Operation
}

func (e *PrepareEmail) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	data["email_valid"] = true
	d, _ := json.Marshal(data)
	return mq.Result{Payload: d, Ctx: ctx}
}

type EmailDelivery struct {
	dag.Operation
}

func (e *EmailDelivery) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	data["email_sent"] = true
	d, _ := json.Marshal(data)
	return mq.Result{Payload: d, Ctx: ctx}
}

type SendSms struct {
	dag.Operation
}

func (e *SendSms) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	return mq.Result{Payload: task.Payload, Error: nil, Ctx: ctx}
}

type StoreData struct {
	dag.Operation
}

func (e *StoreData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type InAppNotification struct {
	dag.Operation
}

func (e *InAppNotification) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}
