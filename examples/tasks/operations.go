package tasks

import (
	"context"

	"github.com/oarkflow/json"

	v2 "github.com/oarkflow/mq/dag"

	"github.com/oarkflow/mq"
)

type GetData struct {
	v2.Operation
}

func (e *GetData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type Loop struct {
	v2.Operation
}

func (e *Loop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type Condition struct {
	v2.Operation
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
	v2.Operation
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
	v2.Operation
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
	v2.Operation
}

func (e *SendSms) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	data["sms_sent"] = true
	d, _ := json.Marshal(data)
	return mq.Result{Payload: d, Ctx: ctx}
}

type StoreData struct {
	v2.Operation
}

func (e *StoreData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	data["stored"] = true
	d, _ := json.Marshal(data)
	return mq.Result{Payload: d, Ctx: ctx}
}

type InAppNotification struct {
	v2.Operation
}

func (e *InAppNotification) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	data["notified"] = true
	d, _ := json.Marshal(data)
	return mq.Result{Payload: d, Ctx: ctx}
}

type Final struct {
	v2.Operation
}

func (e *Final) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	rs := map[string]any{
		"html_content": `<strong>Processed successfully!</strong>`,
	}
	bt, _ := json.Marshal(rs)
	return mq.Result{Payload: bt, Ctx: ctx}
}
