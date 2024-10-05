package main

import (
	"context"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
	"time"
)

var d *dag.DAG

func main() {
	d = dag.New()
	d.AddNode("queue1", tasks.Node1)
	d.AddNode("queue2", tasks.Node2)
	d.AddNode("queue3", tasks.Node3)
	d.AddNode("queue4", tasks.Node4)

	d.AddEdge("queue1", "queue2")
	d.AddLoop("queue2", "queue3")
	d.AddEdge("queue2", "queue4")
	d.Prepare()
	go func() {
		d.Start(context.Background(), ":8081")
	}()
	time.Sleep(5 * time.Second)
	err := d.PublishTask(context.Background(), []byte(`{"tast": 123}`))
	if err != nil {
		panic(err)
	}

	time.Sleep(10 * time.Second)
}
