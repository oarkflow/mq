package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/examples/tasks"
)

var d *dag.DAG

func main() {
	d = dag.New(mq.WithTLS(true, "server.crt", "server.key"), mq.WithCAPath("ca.crt"))
	d.AddNode("queue1", tasks.Node1)
	d.AddNode("queue2", tasks.Node2)
	d.AddNode("queue3", tasks.Node3)
	d.AddNode("queue4", tasks.Node4)

	d.AddEdge("queue1", "queue2")
	d.AddLoop("queue2", "queue3")
	d.AddEdge("queue2", "queue4")
	d.Prepare()
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
