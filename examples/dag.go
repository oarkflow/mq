package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	err := d.AddDeferredNode("F")
	if err != nil {
		panic(err)
	}
	d.AddEdge("A", "B", dag.LoopEdge)
	d.AddCondition("C", map[string]string{"PASS": "D", "FAIL": "E"})
	d.AddEdge("B", "C")
	d.AddEdge("D", "F")
	d.AddEdge("E", "F")
	http.HandleFunc("POST /publish", requestHandler("publish"))
	http.HandleFunc("POST /request", requestHandler("request"))
	err = d.Start(context.TODO(), ":8083")
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
		if rs.Error != nil {
			http.Error(w, fmt.Sprintf("[DAG Error] - %v", rs.Error), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rs)
	}
}
