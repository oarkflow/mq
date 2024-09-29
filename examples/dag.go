package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"io"
	"log"
	"net/http"
)

var d *dag.DAG

func main() {
	d = dag.New()
	d.AddNode("queue1", func(ctx context.Context, task mq.Task) mq.Result {
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})
	d.AddNode("queue2", func(ctx context.Context, task mq.Task) mq.Result {
		return mq.Result{Payload: task.Payload, MessageID: task.ID}
	})
	d.AddNode("queue3", func(ctx context.Context, task mq.Task) mq.Result {
		var data map[string]any
		err := json.Unmarshal(task.Payload, &data)
		if err != nil {
			return mq.Result{Error: err}
		}
		data["salary"] = fmt.Sprintf("12000%v", data["user_id"])
		bt, _ := json.Marshal(data)
		return mq.Result{Payload: bt, MessageID: task.ID}
	})
	d.AddNode("queue4", func(ctx context.Context, task mq.Task) mq.Result {
		var data []map[string]any
		err := json.Unmarshal(task.Payload, &data)
		if err != nil {
			return mq.Result{Error: err}
		}
		payload := map[string]any{"storage": data}
		bt, _ := json.Marshal(payload)
		return mq.Result{Payload: bt, MessageID: task.ID}
	})
	d.AddEdge("queue1", "queue2")
	d.AddLoop("queue2", "queue3")
	d.AddEdge("queue2", "queue4")
	go func() {
		err := d.Start(context.TODO())
		if err != nil {
			panic(err)
		}
	}()
	http.HandleFunc("/send-task", sendTaskHandler)
	log.Println("HTTP server started on http://localhost:8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
}
func sendTaskHandler(w http.ResponseWriter, r *http.Request) {
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
	finalResult := d.Send(payload)
	w.Header().Set("Content-Type", "application/json")
	result := map[string]any{
		"message_id": finalResult.MessageID,
		"payload":    string(finalResult.Payload),
		"error":      finalResult.Error,
	}

	json.NewEncoder(w).Encode(result)
}
