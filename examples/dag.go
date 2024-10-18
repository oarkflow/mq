package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/examples/tasks"
	"github.com/oarkflow/mq/services"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

func main() {
	sync()
	async()
}

func setup(f *dag.DAG) {
	f.
		AddNode("Email Delivery", "email:deliver", &tasks.EmailDelivery{Operation: services.Operation{Type: "process"}}).
		AddNode("Prepare Email", "prepare:email", &tasks.PrepareEmail{Operation: services.Operation{Type: "process"}}).
		AddNode("Get Input", "get:input", &tasks.GetData{Operation: services.Operation{Type: "input"}}, true).
		AddNode("Iterator Processor", "loop", &tasks.Loop{Operation: services.Operation{Type: "loop"}}).
		AddNode("Condition", "condition", &tasks.Condition{Operation: services.Operation{Type: "condition"}}).
		AddNode("Store data", "store:data", &tasks.StoreData{Operation: services.Operation{Type: "process"}}).
		AddNode("Send SMS", "send:sms", &tasks.SendSms{Operation: services.Operation{Type: "process"}}).
		AddNode("Notification", "notification", &tasks.InAppNotification{Operation: services.Operation{Type: "process"}}).
		AddCondition("condition", map[dag.When]dag.Then{"pass": "email:deliver", "fail": "store:data"}).
		AddEdge("Get input to loop", "get:input", "loop").
		AddIterator("Loop to prepare email", "loop", "prepare:email").
		AddEdge("Prepare Email to condition", "prepare:email", "condition").
		AddEdge("Store Data to send sms and notification", "store:data", "send:sms", "notification")
}

func sendData(f *dag.DAG) {
	data := []map[string]any{
		{"phone": "+123456789", "email": "abc.xyz@gmail.com"}, {"phone": "+98765412", "email": "xyz.abc@gmail.com"},
	}
	bt, _ := json.Marshal(data)
	result := f.Process(context.Background(), bt)
	fmt.Println(string(result.Payload))
}

func sync() {
	f := dag.NewDAG("Sample DAG", "sample-dag", mq.WithSyncMode(true), mq.WithNotifyResponse(tasks.NotifyResponse))
	setup(f)
	fmt.Println(f.ExportDOT())
	sendData(f)
	fmt.Println(f.SaveSVG("dag.svg"))
}

func async() {
	f := dag.NewDAG("Sample DAG", "sample-dag", mq.WithNotifyResponse(tasks.NotifyResponse))
	setup(f)

	requestHandler := func(requestType string) func(w http.ResponseWriter, r *http.Request) {
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
			rs := f.Process(ctx, payload)
			if rs.Error != nil {
				http.Error(w, fmt.Sprintf("[DAG Error] - %v", rs.Error), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(rs)
		}
	}

	http.HandleFunc("POST /publish", requestHandler("publish"))
	http.HandleFunc("POST /request", requestHandler("request"))
	http.HandleFunc("/pause-consumer/{id}", func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("id")
		if id != "" {
			f.PauseConsumer(request.Context(), id)
		}
	})
	http.HandleFunc("/resume-consumer/{id}", func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("id")
		if id != "" {
			f.ResumeConsumer(request.Context(), id)
		}
	})
	http.HandleFunc("/pause", func(writer http.ResponseWriter, request *http.Request) {
		f.Pause(request.Context())
	})
	http.HandleFunc("/resume", func(writer http.ResponseWriter, request *http.Request) {
		f.Resume(request.Context())
	})
	err := f.Start(context.TODO(), ":8083")
	if err != nil {
		panic(err)
	}
}
