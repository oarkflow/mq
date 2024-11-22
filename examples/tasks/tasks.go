package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	v2 "github.com/oarkflow/mq/dag/v2"
	"log"

	"github.com/oarkflow/mq"
)

type Node1 struct{ v2.Operation }

func (t *Node1) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	fmt.Println("Node 1", string(task.Payload))
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

type Node2 struct{ v2.Operation }

func (t *Node2) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	fmt.Println("Node 2", string(task.Payload))
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

type Node3 struct{ v2.Operation }

func (t *Node3) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	fmt.Println(string(task.Payload))
	err := json.Unmarshal(task.Payload, &user)
	if err != nil {
		panic(err)
	}
	age := int(user["age"].(float64))
	status := "FAIL"
	if age > 20 {
		status = "PASS"
	}
	user["status"] = status
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload, ConditionStatus: status}
}

type Node4 struct{ v2.Operation }

func (t *Node4) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	user["node"] = "D"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

type Node5 struct{ v2.Operation }

func (t *Node5) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	user["node"] = "E"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

type Node6 struct{ v2.Operation }

func (t *Node6) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	resultPayload, _ := json.Marshal(map[string]any{"storage": user})
	return mq.Result{Payload: resultPayload}
}

type Node7 struct{ v2.Operation }

func (t *Node7) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	user["node"] = "G"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

type Node8 struct{ v2.Operation }

func (t *Node8) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	_ = json.Unmarshal(task.Payload, &user)
	user["node"] = "H"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Callback(_ context.Context, task mq.Result) mq.Result {
	fmt.Println("Received task", task.TaskID, "Payload", string(task.Payload), task.Error, task.Topic)
	return mq.Result{}
}

func NotifyResponse(_ context.Context, result mq.Result) error {
	log.Printf("DAG - FINAL_RESPONSE ~> TaskID: %s, Payload: %s, Topic: %s, Error: %v, Latency: %s", result.TaskID, result.Payload, result.Topic, result.Error, result.Latency)
	return nil
}

func NotifySubDAGResponse(_ context.Context, result mq.Result) error {
	log.Printf("SUB DAG - FINAL_RESPONSE ~> TaskID: %s, Payload: %s, Topic: %s, Error: %v, Latency: %s", result.TaskID, result.Payload, result.Topic, result.Error, result.Latency)
	return nil
}
