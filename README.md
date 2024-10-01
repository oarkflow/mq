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
	"encoding/json"
	"fmt"
	
	"github.com/oarkflow/mq"
)

func Node1(ctx context.Context, task mq.Task) mq.Result {
	fmt.Println("Processing queue1")
	return mq.Result{Payload: task.Payload, MessageID: task.ID}
}

func Node2(ctx context.Context, task mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload, MessageID: task.ID}
}

func Node3(ctx context.Context, task mq.Task) mq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	data["salary"] = fmt.Sprintf("12000%v", data["user_id"])
	bt, _ := json.Marshal(data)
	return mq.Result{Payload: bt, MessageID: task.ID}
}

func Node4(ctx context.Context, task mq.Task) mq.Result {
	var data []map[string]any
	err := json.Unmarshal(task.Payload, &data)
	if err != nil {
		return mq.Result{Error: err}
	}
	payload := map[string]any{"storage": data}
	bt, _ := json.Marshal(payload)
	return mq.Result{Payload: bt, MessageID: task.ID}
}

func Callback(ctx context.Context, task mq.Result) mq.Result {
	fmt.Println("Received task", task.MessageID, "Payload", string(task.Payload), task.Error, task.Queue)
	return mq.Result{}
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
	consumer := mq.NewConsumer("consumer-1")
	consumer.RegisterHandler("queue1", tasks.Node1)
	consumer.RegisterHandler("queue2", tasks.Node2)
	consumer.Consume(context.Background())
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
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
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
		err := d.Start(context.TODO())
		if err != nil {
			panic(err)
		}
	}()
	http.HandleFunc("/publish", requestHandler("publish"))
	http.HandleFunc("/request", requestHandler("request"))
	log.Println("HTTP server started on http://localhost:8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
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
		var rs mq.Result
		if requestType == "request" {
			rs = d.Request(context.Background(), payload)
		} else {
			rs = d.Send(context.Background(), payload)
		}
		w.Header().Set("Content-Type", "application/json")
		result := map[string]any{
			"message_id": rs.MessageID,
			"payload":    string(rs.Payload),
			"error":      rs.Error,
		}
		json.NewEncoder(w).Encode(result)
	}
}
```


## TODOS

- Backend for task persistence
- Task scheduling
- Conditional nodes for tasks