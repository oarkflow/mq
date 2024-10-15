package tasks

import (
	"context"
	"fmt"

	"github.com/oarkflow/json"

	"github.com/oarkflow/dipper"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/services"
)

type GetData struct {
	services.Operation
}

func (e *GetData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println("Getting Input", string(task.Payload))
	ctx = context.WithValue(ctx, "extra_params", map[string]any{"iphone": true})
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type Loop struct {
	services.Operation
}

func (e *Loop) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	fmt.Println(ctx.Value("extra_params"), "LOOOOOP")
	fmt.Println("Looping...", string(task.Payload))
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type Condition struct {
	services.Operation
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
			fmt.Println("Checking...", data, "Pass...")
			return mq.Result{Payload: task.Payload, Status: "pass", Ctx: ctx}
		}
		return mq.Result{Payload: task.Payload, Status: "fail", Ctx: ctx}
	default:
		return mq.Result{Payload: task.Payload, Status: "fail", Ctx: ctx}
	}
}

type PrepareEmail struct {
	services.Operation
}

func (e *PrepareEmail) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		fmt.Println("Prepare Email")
		panic(err)
	}
	data["email_valid"] = true
	d, _ := json.Marshal(data)
	fmt.Println("Preparing Email...", string(d))
	return mq.Result{Payload: d, Ctx: ctx}
}

type EmailDelivery struct {
	services.Operation
}

func (e *EmailDelivery) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		fmt.Println("Email Delivery")
		panic(err)
	}
	fmt.Println("Sending Email...", data)
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type SendSms struct {
	services.Operation
}

func (e *SendSms) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	fmt.Println("Sending Sms...", data)
	return mq.Result{Payload: task.Payload, Error: nil, Ctx: ctx}
}

type StoreData struct {
	services.Operation
}

func (e *StoreData) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {

	fmt.Println("Storing Data...", string(task.Payload))
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type InAppNotification struct {
	services.Operation
}

func (e *InAppNotification) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		panic(err)
	}
	fmt.Println("In App notification...", data)
	return mq.Result{Payload: task.Payload, Ctx: ctx}
}

type DataBranchHandler struct{ services.Operation }

func (v *DataBranchHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	ctx = context.WithValue(ctx, "extra_params", map[string]any{"iphone": true})
	var row map[string]any
	var result mq.Result
	result.Payload = task.Payload
	err := json.Unmarshal(result.Payload, &row)
	if err != nil {
		result.Error = err
		return result
	}
	fmt.Println("Data Branch...")
	b := make(map[string]any)
	switch branches := row["data_branch"].(type) {
	case map[string]any:
		for field, handler := range branches {
			data, err := dipper.Get(row, field)
			if err != nil {
				break
			}
			b[handler.(string)] = data
		}
		break
	}
	br, err := json.Marshal(b)
	if err != nil {
		result.Error = err
		return result
	}
	result.Status = "branches"
	result.Payload = br
	result.Ctx = ctx
	return result
}
