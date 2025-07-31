package main

import (
	"context"
	"fmt"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	d := dag.NewDAG("Sample DAG", "sample-dag", func(taskID string, result mq.Result) {
		fmt.Println("Final", string(result.Payload))
	},
		mq.WithSyncMode(true),
		mq.WithNotifyResponse(tasks.NotifyResponse),
	)
	d.AddNode(dag.Function, "C", "C", &tasks.Node3{}, true)
	d.AddNode(dag.Function, "D", "D", &tasks.Node4{})
	d.AddNode(dag.Function, "E", "E", &tasks.Node5{})
	d.AddNode(dag.Function, "F", "F", &tasks.Node6{})
	d.AddNode(dag.Function, "G", "G", &tasks.Node7{})
	d.AddNode(dag.Function, "H", "H", &tasks.Node8{})

	d.AddCondition("C", map[string]string{"PASS": "D", "FAIL": "E"})
	d.AddEdge(dag.Simple, "Label 1", "B", "C")
	d.AddEdge(dag.Simple, "Label 2", "D", "F")
	d.AddEdge(dag.Simple, "Label 3", "E", "F")
	d.AddEdge(dag.Simple, "Label 4", "F", "G", "H")
	d.AssignTopic("queue")
	err := d.Consume(context.Background())
	if err != nil {
		panic(err)
	}
}
