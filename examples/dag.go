package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/examples/tasks"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

var (
	d = dag.NewDAG(
		"Sample DAG",
		"sample-dag",
		mq.WithSyncMode(true),
		mq.WithNotifyResponse(tasks.NotifyResponse),
	)
	// d = dag.NewDAG(mq.WithSyncMode(true), mq.WithTLS(true, "./certs/server.crt", "./certs/server.key"), mq.WithCAPath("./certs/ca.cert"))
)

func main() {
	subDag := dag.NewDAG(
		"Sub DAG",
		"D",
		mq.WithNotifyResponse(tasks.NotifySubDAGResponse),
	)
	subDag.AddNode("D", "D", tasks.Node4, true)
	subDag.AddNode("F", "F", tasks.Node6)
	subDag.AddNode("G", "G", tasks.Node7)
	subDag.AddNode("H", "H", tasks.Node8)
	subDag.AddEdge("Label 2", "D", "F")
	subDag.AddEdge("Label 4", "F", "G", "H")

	d.AddNode("A", "A", tasks.Node1, true)
	d.AddNode("B", "B", tasks.Node2)
	d.AddNode("C", "C", tasks.Node3)
	d.AddDAGNode("D", "D", subDag)
	d.AddNode("E", "E", tasks.Node5)
	d.AddLoop("Send each item", "A", "B")
	d.AddCondition("C", map[dag.When]dag.Then{"PASS": "D", "FAIL": "E"})
	d.AddEdge("Label 1", "B", "C")
	// Classify edges
	// d.ClassifyEdges()
	// fmt.Println(d.ExportDOT())

	http.HandleFunc("POST /publish", requestHandler("publish"))
	http.HandleFunc("POST /request", requestHandler("request"))
	http.HandleFunc("/pause-consumer/{id}", func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("id")
		if id != "" {
			d.PauseConsumer(request.Context(), id)
		}
	})
	http.HandleFunc("/resume-consumer/{id}", func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("id")
		if id != "" {
			d.ResumeConsumer(request.Context(), id)
		}
	})
	http.HandleFunc("/pause", func(writer http.ResponseWriter, request *http.Request) {
		d.Pause(request.Context())
	})
	http.HandleFunc("/resume", func(writer http.ResponseWriter, request *http.Request) {
		d.Resume(request.Context())
	})
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
		ctx := r.Context()
		if requestType == "request" {
			ctx = mq.SetHeaders(ctx, map[string]string{consts.AwaitResponseKey: "true"})
		}
		// ctx = context.WithValue(ctx, "initial_node", "E")
		rs := d.Process(ctx, payload)
		if rs.Error != nil {
			http.Error(w, fmt.Sprintf("[DAG Error] - %v", rs.Error), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rs)
	}
}
