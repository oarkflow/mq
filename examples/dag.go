package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	f := dag.NewDAG("Sample DAG", "sample-dag", func(taskID string, result mq.Result) {
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
	f.Start(context.Background(), ":8082")
	sendData(f)
}

func subDAG() *dag.DAG {
	f := dag.NewDAG("Sub DAG", "sub-dag", func(taskID string, result mq.Result) {
		fmt.Printf("Final result for task %s: %s\n", taskID, string(result.Payload))
	}, mq.WithSyncMode(true))
	f.
		AddNode(dag.Function, "Store data", "store:data", &tasks.StoreData{Operation: dag.Operation{Type: "process"}}, true).
		AddNode(dag.Function, "Send SMS", "send:sms", &tasks.SendSms{Operation: dag.Operation{Type: "process"}}).
		AddNode(dag.Function, "Notification", "notification", &tasks.InAppNotification{Operation: dag.Operation{Type: "process"}}).
		AddEdge(dag.Simple, "Store Payload to send sms", "store:data", "send:sms").
		AddEdge(dag.Simple, "Store Payload to notification", "send:sms", "notification")
	return f
}

func setup(f *dag.DAG) {
	f.
		AddNode(dag.Function, "Email Delivery", "email:deliver", &tasks.EmailDelivery{Operation: dag.Operation{Type: "process"}}).
		AddNode(dag.Function, "Prepare Email", "prepare:email", &tasks.PrepareEmail{Operation: dag.Operation{Type: "process"}}).
		AddNode(dag.Function, "Get Input", "get:input", &tasks.GetData{Operation: dag.Operation{Type: "input"}}, true).
		AddNode(dag.Function, "Iterator Processor", "loop", &tasks.Loop{Operation: dag.Operation{Type: "loop"}}).
		AddNode(dag.Function, "Condition", "condition", &tasks.Condition{Operation: dag.Operation{Type: "condition"}}).
		AddDAGNode("Persistent", "persistent", subDAG()).
		AddEdge(dag.Simple, "Get input to loop", "get:input", "loop").
		AddEdge(dag.Iterator, "Loop to prepare email", "loop", "prepare:email").
		AddEdge(dag.Simple, "Prepare Email to condition", "prepare:email", "condition").
		AddCondition("condition", map[string]string{"pass": "email:deliver", "fail": "persistent"})
}

func sendData(f *dag.DAG) {
	data := []map[string]any{
		{"phone": "+123456789", "email": "abc.xyz@gmail.com"}, {"phone": "+98765412", "email": "xyz.abc@gmail.com"},
	}
	bt, _ := json.Marshal(data)
	result := f.Process(context.Background(), bt)
	fmt.Println(string(result.Payload))
}
