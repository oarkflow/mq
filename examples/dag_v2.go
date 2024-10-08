package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/v2"
)

func handler1(ctx context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload}
}

func handler2(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	return mq.Result{Payload: task.Payload}
}

func handler3(ctx context.Context, task *mq.Task) mq.Result {
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

func handler4(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	user["final"] = "D"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func handler5(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	user["salary"] = "E"
	resultPayload, _ := json.Marshal(user)
	return mq.Result{Payload: resultPayload}
}

func handler6(ctx context.Context, task *mq.Task) mq.Result {
	var user map[string]any
	json.Unmarshal(task.Payload, &user)
	resultPayload, _ := json.Marshal(map[string]any{"storage": user})
	return mq.Result{Payload: resultPayload}
}

var (
	d = v2.NewDAG(mq.WithSyncMode(true))
)

func main() {
	d.AddNode("A", handler1)
	d.AddNode("B", handler2)
	d.AddNode("C", handler3)
	d.AddNode("D", handler4)
	d.AddNode("E", handler5)
	d.AddNode("F", handler6)
	d.AddEdge("A", "B", v2.LoopEdge)
	d.AddCondition("C", map[string]string{"PASS": "D", "FAIL": "E"})
	d.AddEdge("B", "C")
	d.AddEdge("D", "F")
	d.AddEdge("E", "F")

	initialPayload, _ := json.Marshal([]map[string]any{
		{"user_id": 1, "age": 12},
		{"user_id": 2, "age": 34},
	})
	for i := 0; i < 100; i++ {
		rs := d.ProcessTask(context.Background(), "A", initialPayload)
		if rs.Error != nil {
			panic(rs.Error)
		}
		fmt.Println(string(rs.Payload))
	}
	/*http.HandleFunc("POST /publish", requestHandler("publish"))
	http.HandleFunc("POST /request", requestHandler("request"))
	err := d.Start(context.TODO(), ":8083")
	if err != nil {
		panic(err)
	}*/
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
		rs := d.ProcessTask(context.Background(), "A", payload)
		w.Header().Set("Content-Type", "application/json")
		result := map[string]any{
			"message_id": rs.TaskID,
			"payload":    string(rs.Payload),
			"error":      rs.Error,
		}
		json.NewEncoder(w).Encode(result)
	}
}
