# Introduction MQ (Message Queue Broker)

A simple Pub/Sub system memory based task processing. It uses centralized server to manage consumers and publishers.


## Examples:

### Run server

> `go run server.go`

### Run consumer

> `go run consumer.go`

### Run publisher

> `go run publisher.go`



[tasks.go](./examples/tasks/tasks.go)

```go
package tasks

import (
	"context"
	"github.com/oarkflow/json"
	"fmt"
	"log"

	"github.com/oarkflow/mq"
)

func Node1(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

func Node2(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, TaskID: task.ID}
}

func Node3(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	age := int(user["age"].(float64))
	status := "FAIL"
	if age > 20 {
		status = "PASS"
	}
	user["status"] = status
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload, Status: status}
}

func Node4(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	user["final"] = "D"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Node5(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	user["salary"] = "E"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func Node6(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	resultPayload, _ := json.Marshal(map[string]any{"storage": user})
	return mq.Result{Payload: resultPayload}
}

func Callback(ctx context.Context, task mq.Result) mq.Result {
	fmt.Println("Received task", task.TaskID, "Payload", string(task.Payload), task.Error, task.Topic)
	return mq.Result{}
}

func NotifyResponse(ctx context.Context, result mq.Result) {
	log.Printf("DAG Final response: TaskID: %s, Payload: %s, Topic: %s", result.TaskID, result.Payload, result.Topic)
}
```

## Start Server

[server.go](./examples/server.go)

```go
package main

import (
	"context"
	
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	b := mq.NewBroker(mq.WithCallback(tasks.Callback))
	b.NewQueue("queue1")
	b.NewQueue("queue2")
	b.Start(context.Background())
}
```

## Start Consumer

[consumer.go](./examples/consumer.go)

```go
package main

import (
	"context"

	"github.com/oarkflow/mq"

	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	consumer1 := mq.NewConsumer("consumer-1", "queue1", tasks.Node1)
	consumer2 := mq.NewConsumer("consumer-2", "queue2", tasks.Node2)
	// consumer := mq.NewConsumer("consumer-1", mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"))
	go consumer1.Consume(context.Background())
	consumer2.Consume(context.Background())
}
```

## Publish tasks

[publisher.go](./examples/publisher.go)

```go
package main

import (
	"context"
	"fmt"
	
	"github.com/oarkflow/mq"
)

func main() {
	payload := []byte(`{"message":"Message Publisher \n Task"}`)
	task := mq.Task{
		Payload: payload,
	}
	publisher := mq.NewPublisher("publish-1")
	err := publisher.Publish(context.Background(), "queue1", task)
	if err != nil {
		panic(err)
	}
	fmt.Println("Async task published successfully")
	payload = []byte(`{"message":"Fire-and-Forget \n Task"}`)
	task = mq.Task{
		Payload: payload,
	}
	result, err := publisher.Request(context.Background(), "queue1", task)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Sync task published. Result: %v\n", string(result.Payload))
}
```

# DAG (Directed Acyclic Graph)

In this package, you can use the `DAG` feature to create a directed acyclic graph of tasks. The `DAG` feature allows you to define a sequence of tasks that need to be executed in a specific order.

## Example

[dag.go](./examples/dag.go)

```go
package main

import (
	"context"
	"github.com/oarkflow/json"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/examples/tasks"
	"io"
	"net/http"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

var (
	d = dag.NewDAG(mq.WithSyncMode(false), mq.WithNotifyResponse(tasks.NotifyResponse))
	// d = dag.NewDAG(mq.WithSyncMode(true), mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"), mq.WithCAPath("./certs/ca.cert"))
)

func main() {
	d.AddNode("A", tasks.Node1, true)
	d.AddNode("B", tasks.Node2)
	d.AddNode("C", tasks.Node3)
	d.AddNode("D", tasks.Node4)
	d.AddNode("E", tasks.Node5)
	d.AddNode("F", tasks.Node6)
	d.AddEdge("A", "B", dag.LoopEdge)
	d.AddCondition("C", map[string]string{"PASS": "D", "FAIL": "E"})
	d.AddEdge("B", "C")
	d.AddEdge("D", "F")
	d.AddEdge("E", "F")
	http.HandleFunc("POST /publish", requestHandler("publish"))
	http.HandleFunc("POST /request", requestHandler("request"))
	err := d.Start(context.TODO(), ":8083")
	if err != nil {
		panic(err)
	}
}

func requestHandler(requestType string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}
		var payload []byte
		if r.Body != nil {
			defer r.Body.Close()
			var err error
			payload, err = io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}
		} else {
			http.Error(w, "Empty request body", http.StatusBadRequest)
			return
		}
		ctx := context.Background()
		if requestType == "request" {
			ctx = mq.SetHeaders(ctx, map[string]string{consts.AwaitResponseKey: "true"})
		}
		// ctx = context.WithValue(ctx, "initial_node", "E")
		rs := d.ProcessTask(ctx, payload)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rs)
	}
}
```


## TODOS

- Backend for task persistence
- Task scheduling