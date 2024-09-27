package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oarkflow/mq"
)

func handleNode1(_ context.Context, task mq.Task) mq.Result {
	result := []map[string]string{
		{"field": "facility", "item": "item1"},
		{"field": "facility", "item": "item2"},
		{"field": "facility", "item": "item3"},
	}
	var payload string
	err := json.Unmarshal(task.Payload, &payload)
	if err != nil {
		return mq.Result{Status: "fail", Payload: json.RawMessage(`{"field": "node1", "item": "error"}`)}
	}
	fmt.Printf("Processing task at node1: %s\n", string(task.Payload))
	bt, _ := json.Marshal(result)
	return mq.Result{Status: "completed", Payload: bt}
}

func handleNode2(_ context.Context, task mq.Task) mq.Result {
	var payload map[string]string
	err := json.Unmarshal(task.Payload, &payload)
	if err != nil {
		return mq.Result{Status: "fail", Payload: json.RawMessage(`{"field": "node2", "item": "error"}`)}
	}
	status := "fail"
	if payload["item"] == "item2" {
		status = "pass"
	}
	fmt.Printf("Processing task at node2: %s %s\n", payload, status)
	bt, _ := json.Marshal(payload)
	return mq.Result{Status: status, Payload: bt}
}

func handleNode3(_ context.Context, task mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	data["item"] = "Item processed in node3"
	bt, _ := json.Marshal(data)
	return mq.Result{Status: "completed", Payload: bt}
}

func handleNode4(_ context.Context, task mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	data["item"] = "An Item processed in node4"
	bt, _ := json.Marshal(data)
	return mq.Result{Status: "completed", Payload: bt}
}

func main() {
	ctx := context.Background()
	d := mq.NewDAG(false)

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
	result := d.ProcessTask(ctx, mq.Task{Payload: []byte(`"Start processing"`)})
	fmt.Println(string(result.Payload))
	time.Sleep(50 * time.Second)
}
