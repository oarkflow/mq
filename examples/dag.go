package main

import (
	"context"
	"fmt"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
	"time"
)

var d *dag.DAG

func main() {
	d = dag.New()
	d.AddNode("queue1", tasks.CheckCondition, true)
	d.AddNode("queue2", tasks.Pass)
	d.AddNode("queue3", tasks.Fail)

	d.AddCondition("queue1", map[string]string{"pass": "queue2", "fail": "queue3"})
	d.Prepare()
	go func() {
		d.Start(context.Background(), ":8081")
	}()
	go func() {
		time.Sleep(3 * time.Second)
		result := d.Send(context.Background(), []byte(`{"user_id": 1}`))
		if result.Error != nil {
			panic(result.Error)
		}
		fmt.Println("Response", string(result.Payload))
	}()

	time.Sleep(10 * time.Second)
}
