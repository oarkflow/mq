package main

import (
	"context"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"

	"github.com/oarkflow/mq/dag"
)

func main() {
	d := dag.NewDAG("Sample DAG", "sample-dag",
		mq.WithSyncMode(true),
		mq.WithNotifyResponse(tasks.NotifyResponse),
		mq.WithSecretKey([]byte("wKWa6GKdBd0njDKNQoInBbh6P0KTjmob")),
	)
	d.AddNode("C", "C", tasks.Node3, true)
	d.AddNode("D", "D", tasks.Node4)
	d.AddNode("E", "E", tasks.Node5)
	d.AddNode("F", "F", tasks.Node6)
	d.AddNode("G", "G", tasks.Node7)
	d.AddNode("H", "H", tasks.Node8)

	d.AddCondition("C", map[dag.When]dag.Then{"PASS": "D", "FAIL": "E"})
	d.AddEdge("Label 1", "B", "C")
	d.AddEdge("Label 2", "D", "F")
	d.AddEdge("Label 3", "E", "F")
	d.AddEdge("Label 4", "F", "G", "H")
	// d.AssignTopic("queue1")
	d.Consume(context.Background())
}
