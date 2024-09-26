package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/mq/broker"
)

func handleNode1(_ context.Context, task broker.Task) broker.Result {
	result := []map[string]string{
		{"field": "facility", "item": "item1"},
		{"field": "facility", "item": "item2"},
		{"field": "facility", "item": "item3"},
	}
	var payload string
	err := json.Unmarshal(task.Payload, &payload)
	if err != nil {
		return broker.Result{Status: "fail", Payload: json.RawMessage(`{"field": "node1", "item": "error"}`)}
	}
	fmt.Printf("Processing task at node1: %s\n", string(task.Payload))
	bt, _ := json.Marshal(result)
	return broker.Result{Status: "completed", Payload: bt}
}

func handleNode2(_ context.Context, task broker.Task) broker.Result {
	var payload map[string]string
	err := json.Unmarshal(task.Payload, &payload)
	if err != nil {
		return broker.Result{Status: "fail", Payload: json.RawMessage(`{"field": "node2", "item": "error"}`)}
	}
	status := "fail"
	if payload["item"] == "item2" {
		status = "pass"
	}
	fmt.Printf("Processing task at node2: %s %s\n", payload, status)
	bt, _ := json.Marshal(payload)
	return broker.Result{Status: status, Payload: bt}
}

func handleNode3(_ context.Context, task broker.Task) broker.Result {
	result := `{"field": "node3", "item": %s}`
	fmt.Printf("Processing task at node3: %s\n", string(task.Payload))
	return broker.Result{Status: "completed", Payload: json.RawMessage(fmt.Sprintf(result, string(task.Payload)))}
}

func handleNode4(_ context.Context, task broker.Task) broker.Result {
	result := `{"field": "node4", "item": %s}`
	fmt.Printf("Processing task at node4: %s\n", string(task.Payload))
	return broker.Result{Status: "completed", Payload: json.RawMessage(fmt.Sprintf(result, string(task.Payload)))}
}

func main() {
	ctx := context.Background()
	d := broker.NewDAG(":8082")

	d.AddNode("node1", handleNode1, true)
	d.AddNode("node2", handleNode2)
	d.AddNode("node3", handleNode3)
	d.AddNode("node4", handleNode4)
	d.AddCondition("node2", map[string]string{"pass": "node3", "fail": "node4"})
	err := d.AddLoop("node1", "node2")
	if err != nil {
		panic(err)
	}
	err = d.Prepare(ctx)
	if err != nil {
		panic(err)
	}
	// Start the DAG and process the task
	go func() {
		if err := d.Start(ctx); err != nil {
			fmt.Println("Error starting DAG:", err)
		}
	}()
	d.ProcessTask(ctx, broker.Task{Payload: []byte(`"Start processing"`)})

	// Keep the program running to allow task processing
	time.Sleep(50 * time.Second)
}
