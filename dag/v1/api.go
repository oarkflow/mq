package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/oarkflow/mq/jsonparser"
	"github.com/oarkflow/mq/sio"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/metrics"
)

type Request struct {
	Payload   json.RawMessage `json:"payload"`
	Interval  time.Duration   `json:"interval"`
	Schedule  bool            `json:"schedule"`
	Overlap   bool            `json:"overlap"`
	Recurring bool            `json:"recurring"`
}

func (tm *DAG) SetupWS() *sio.Server {
	ws := sio.New(sio.Config{
		CheckOrigin:       func(r *http.Request) bool { return true },
		EnableCompression: true,
	})
	WsEvents(ws)
	tm.Notifier = ws
	return ws
}

func (tm *DAG) Handlers() {
	metrics.HandleHTTP()
	http.Handle("/", http.FileServer(http.Dir("webroot")))
	http.Handle("/notify", tm.SetupWS())
	http.HandleFunc("GET /render", tm.Render)
	http.HandleFunc("POST /request", tm.Request)
	http.HandleFunc("POST /publish", tm.Publish)
	http.HandleFunc("POST /schedule", tm.Schedule)
	http.HandleFunc("/pause-consumer/{id}", func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("id")
		if id != "" {
			tm.PauseConsumer(request.Context(), id)
		}
	})
	http.HandleFunc("/resume-consumer/{id}", func(writer http.ResponseWriter, request *http.Request) {
		id := request.PathValue("id")
		if id != "" {
			tm.ResumeConsumer(request.Context(), id)
		}
	})
	http.HandleFunc("/pause", func(w http.ResponseWriter, request *http.Request) {
		err := tm.Pause(request.Context())
		if err != nil {
			http.Error(w, "Failed to pause", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "paused"})
	})
	http.HandleFunc("/resume", func(w http.ResponseWriter, request *http.Request) {
		err := tm.Resume(request.Context())
		if err != nil {
			http.Error(w, "Failed to resume", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "resumed"})
	})
	http.HandleFunc("/stop", func(w http.ResponseWriter, request *http.Request) {
		err := tm.Stop(request.Context())
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
	})
	http.HandleFunc("/close", func(w http.ResponseWriter, request *http.Request) {
		err := tm.Close()
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "closed"})
	})
	http.HandleFunc("/dot", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, tm.ExportDOT())
	})
	http.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		image := fmt.Sprintf("%s.svg", mq.NewID())
		err := tm.SaveSVG(image)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		defer os.Remove(image)
		svgBytes, err := os.ReadFile(image)
		if err != nil {
			http.Error(w, "Could not read SVG file", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "image/svg+xml")
		if _, err := w.Write(svgBytes); err != nil {
			http.Error(w, "Could not write SVG response", http.StatusInternalServerError)
			return
		}
	})
}

func (tm *DAG) request(w http.ResponseWriter, r *http.Request, async bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
	var request Request
	if r.Body != nil {
		defer r.Body.Close()
		var err error
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		err = json.Unmarshal(payload, &request)
		if err != nil {
			http.Error(w, "Failed to unmarshal body", http.StatusBadRequest)
			return
		}
	} else {
		http.Error(w, "Empty request body", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	if async {
		ctx = mq.SetHeaders(ctx, map[string]string{consts.AwaitResponseKey: "true"})
	}
	var opts []mq.SchedulerOption
	if request.Interval > 0 {
		opts = append(opts, mq.WithInterval(request.Interval))
	}
	if request.Overlap {
		opts = append(opts, mq.WithOverlap())
	}
	if request.Recurring {
		opts = append(opts, mq.WithRecurring())
	}
	ctx = context.WithValue(ctx, "query_params", r.URL.Query())
	var rs mq.Result
	if request.Schedule {
		rs = tm.ScheduleTask(ctx, request.Payload, opts...)
	} else {
		rs = tm.Process(ctx, request.Payload)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rs)
}

func (tm *DAG) Render(w http.ResponseWriter, r *http.Request) {
	ctx := mq.SetHeaders(r.Context(), map[string]string{consts.AwaitResponseKey: "true", "request_type": "render"})
	ctx = context.WithValue(ctx, "query_params", r.URL.Query())
	rs := tm.Process(ctx, nil)
	content, err := jsonparser.GetString(rs.Payload, "html_content")
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", consts.TypeHtml)
	w.Write([]byte(content))
}

func (tm *DAG) Request(w http.ResponseWriter, r *http.Request) {
	tm.request(w, r, true)
}

func (tm *DAG) Publish(w http.ResponseWriter, r *http.Request) {
	tm.request(w, r, false)
}

func (tm *DAG) Schedule(w http.ResponseWriter, r *http.Request) {
	tm.request(w, r, false)
}

func GetTaskID(ctx context.Context) string {
	if queryParams := ctx.Value("query_params"); queryParams != nil {
		if params, ok := queryParams.(url.Values); ok {
			if id := params.Get("taskID"); id != "" {
				return id
			}
		}
	}
	return ""
}

func CanNextNode(ctx context.Context) string {
	if queryParams := ctx.Value("query_params"); queryParams != nil {
		if params, ok := queryParams.(url.Values); ok {
			if id := params.Get("next"); id != "" {
				return id
			}
		}
	}
	return ""
}
