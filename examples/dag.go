package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/oarkflow/mq/examples/tasks"
	"github.com/oarkflow/mq/services"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

func main() {
	aSync()
}

func setup(f *dag.DAG) {
	f.
		AddNode("Email Delivery", "email:deliver", &tasks.EmailDelivery{Operation: services.Operation{Type: "process"}}).
		AddNode("Prepare Email", "prepare:email", &tasks.PrepareEmail{Operation: services.Operation{Type: "process"}}).
		AddNode("Get Input", "get:input", &tasks.GetData{Operation: services.Operation{Type: "input"}}, true).
		AddNode("Iterator Processor", "loop", &tasks.Loop{Operation: services.Operation{Type: "loop"}}).
		AddNode("Condition", "condition", &tasks.Condition{Operation: services.Operation{Type: "condition"}}).
		AddNode("Store data", "store:data", &tasks.StoreData{Operation: services.Operation{Type: "process"}}).
		AddNode("Send SMS", "send:sms", &tasks.SendSms{Operation: services.Operation{Type: "process"}}).
		AddNode("Notification", "notification", &tasks.InAppNotification{Operation: services.Operation{Type: "process"}}).
		AddCondition("condition", map[dag.When]dag.Then{"pass": "email:deliver", "fail": "store:data"}).
		AddEdge("Get input to loop", "get:input", "loop").
		AddIterator("Loop to prepare email", "loop", "prepare:email").
		AddEdge("Prepare Email to condition", "prepare:email", "condition").
		AddEdge("Store Data to send sms and notification", "store:data", "send:sms", "notification")
}

func sendData(f *dag.DAG) {
	data := []map[string]any{
		{"phone": "+123456789", "email": "abc.xyz@gmail.com"}, {"phone": "+98765412", "email": "xyz.abc@gmail.com"},
	}
	bt, _ := json.Marshal(data)
	result := f.Process(context.Background(), bt)
	fmt.Println(string(result.Payload))
}

func aSync() {
	f := dag.NewDAG("Sample DAG", "sample-dag", mq.WithCleanTaskOnComplete())

	f.SetNotifyResponse(func(ctx context.Context, result mq.Result) error {
		if f.Notifier != nil {
			f.Notifier.ToRoom("global", "final-message", result)
		}
		return nil
	})
	f.ReportNodeResult(func(result mq.Result) {
		if f.Notifier != nil {
			f.Notifier.ToRoom("global", "message", result)
		}
		log.Printf("DAG - FINAL_RESPONSE ~> TaskID: %s, Payload: %s, Topic: %s, Error: %v, Latency: %s", result.TaskID, result.Payload, result.Topic, result.Error, result.Latency)
	})
	setup(f)
	err := f.Validate()
	if err != nil {
		panic(err)
	}

	err = f.Start(context.TODO(), ":8083")
	if err != nil {
		panic(err)
	}
}
