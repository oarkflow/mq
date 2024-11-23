package main

import (
	"context"
	"fmt"
	"github.com/oarkflow/mq/dag/v1"

	"github.com/oarkflow/mq/examples/tasks"

	"github.com/oarkflow/mq"
)

func main() {
	d := v1.NewDAG(
		"Sample DAG",
		"sample-dag",
		mq.WithSyncMode(true),
		mq.WithNotifyResponse(tasks.NotifyResponse),
	)
	subDag := v1.NewDAG(
		"Sub DAG",
		"D",
		mq.WithNotifyResponse(tasks.NotifySubDAGResponse),
	)
	subDag.AddNode("I", "I", &tasks.Node4{}, true)
	subDag.AddNode("F", "F", &tasks.Node6{})
	subDag.AddNode("G", "G", &tasks.Node7{})
	subDag.AddNode("H", "H", &tasks.Node8{})
	subDag.AddEdge("Label 2", "I", "F")
	subDag.AddEdge("Label 4", "F", "G", "H")

	d.AddNode("A", "A", &tasks.Node1{}, true)
	d.AddNode("B", "B", &tasks.Node2{})
	d.AddNode("C", "C", &tasks.Node3{})
	d.AddDAGNode("D", "D", subDag)
	d.AddNode("E", "E", &tasks.Node5{})
	d.AddIterator("Send each item", "A", "B")
	d.AddCondition("C", map[v1.When]v1.Then{"PASS": "D", "FAIL": "E"})
	d.AddEdge("Label 1", "B", "C")

	fmt.Println(d.ExportDOT())

	err := d.Start(context.TODO(), ":8083")
	if err != nil {
		panic(err)
	}
}
