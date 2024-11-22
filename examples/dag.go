package main

import (
	"context"
	"encoding/json"
	"fmt"
	v2 "github.com/oarkflow/mq/dag/v2"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	f := v2.NewDAG("Sample DAG", "sample-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))
	f.SetNotifyResponse(func(ctx context.Context, result mq.Result) error {
		if f.Notifier != nil {
			f.Notifier.ToRoom("global", "final-message", result)
		}
		return nil
	})
	setup(f)
	err := f.Validate()
	if err != nil {
		panic(err)
	}
	f.Start(context.Background(), ":8083")
	sendData(f)
}

func subDAG() *v2.DAG {
	f := v2.NewDAG("Sub DAG", "sub-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))
	f.
		AddNode(v2.Function, "Store data", "store:data", &tasks.StoreData{Operation: v2.Operation{Type: "process"}}, true).
		AddNode(v2.Function, "Send SMS", "send:sms", &tasks.SendSms{Operation: v2.Operation{Type: "process"}}).
		AddNode(v2.Function, "Notification", "notification", &tasks.InAppNotification{Operation: v2.Operation{Type: "process"}}).
		AddEdge(v2.Simple, "Store Payload to send sms", "store:data", "send:sms").
		AddEdge(v2.Simple, "Store Payload to notification", "send:sms", "notification")
	return f
}

func setup(f *v2.DAG) {
	f.
		AddNode(v2.Function, "Email Delivery", "email:deliver", &tasks.EmailDelivery{Operation: v2.Operation{Type: "process"}}).
		AddNode(v2.Function, "Prepare Email", "prepare:email", &tasks.PrepareEmail{Operation: v2.Operation{Type: "process"}}).
		AddNode(v2.Function, "Get Input", "get:input", &tasks.GetData{Operation: v2.Operation{Type: "input"}}, true).
		AddNode(v2.Function, "Final Payload", "final", &tasks.Final{Operation: v2.Operation{Type: "page"}}).
		AddNode(v2.Function, "Iterator Processor", "loop", &tasks.Loop{Operation: v2.Operation{Type: "loop"}}).
		AddNode(v2.Function, "Condition", "condition", &tasks.Condition{Operation: v2.Operation{Type: "condition"}}).
		AddDAGNode("Persistent", "persistent", subDAG()).
		AddEdge(v2.Simple, "Get input to loop", "get:input", "loop").
		AddEdge(v2.Iterator, "Loop to prepare email", "loop", "prepare:email").
		AddEdge(v2.Simple, "Prepare Email to condition", "prepare:email", "condition").
		AddCondition("condition", map[string]string{"pass": "email:deliver", "fail": "persistent"})
}

func sendData(f *v2.DAG) {
	data := []map[string]any{
		{"phone": "+123456789", "email": "abc.xyz@gmail.com"}, {"phone": "+98765412", "email": "xyz.abc@gmail.com"},
	}
	bt, _ := json.Marshal(data)
	result := f.Process(context.Background(), bt)
	fmt.Println(string(result.Payload))
}
