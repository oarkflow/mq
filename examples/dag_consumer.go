package main

import (
	"context"
	"fmt"
	v2 "github.com/oarkflow/mq/dag/v2"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	d := v2.NewDAG("Sample DAG", "sample-dag", func(taskID string, result mq.Result) {
		fmt.Println("Final", string(result.Payload))
	},
		mq.WithSyncMode(true),
		mq.WithNotifyResponse(tasks.NotifyResponse),
	)
	d.AddNode(v2.Function, "C", "C", &tasks.Node3{}, true)
	d.AddNode(v2.Function, "D", "D", &tasks.Node4{})
	d.AddNode(v2.Function, "E", "E", &tasks.Node5{})
	d.AddNode(v2.Function, "F", "F", &tasks.Node6{})
	d.AddNode(v2.Function, "G", "G", &tasks.Node7{})
	d.AddNode(v2.Function, "H", "H", &tasks.Node8{})

	d.AddCondition("C", map[string]string{"PASS": "D", "FAIL": "E"})
	d.AddEdge(v2.Simple, "Label 1", "B", "C")
	d.AddEdge(v2.Simple, "Label 2", "D", "F")
	d.AddEdge(v2.Simple, "Label 3", "E", "F")
	d.AddEdge(v2.Simple, "Label 4", "F", "G", "H")
	d.AssignTopic("queue")
	err := d.Consume(context.Background())
	if err != nil {
		panic(err)
	}
}
