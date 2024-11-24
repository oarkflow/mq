package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/sio"

	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/jsonparser"
)

func renderNotFound(w http.ResponseWriter) {
	html := []byte(`
<div>
	<h1>task not found</h1>
	<p><a href="/process">Back to home</a></p>
</div>
`)
	w.Header().Set(consts.ContentType, consts.TypeHtml)
	w.Write(html)
}

func (tm *DAG) render(w http.ResponseWriter, r *http.Request) {
	ctx, data, err := parse(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	accept := r.Header.Get("Accept")
	userCtx := UserContext(ctx)
	ctx = context.WithValue(ctx, "method", r.Method)
	if r.Method == "GET" && userCtx.Get("task_id") != "" {
		manager, ok := tm.taskManager.Get(userCtx.Get("task_id"))
		if !ok || manager == nil {
			if strings.Contains(accept, "text/html") || accept == "" {
				renderNotFound(w)
				return
			}
			http.Error(w, fmt.Sprintf(`{"message": "%s"}`, "task not found"), http.StatusInternalServerError)
			return
		}
	}
	result := tm.Process(ctx, data)
	if result.Error != nil {
		http.Error(w, fmt.Sprintf(`{"message": "%s"}`, result.Error.Error()), http.StatusInternalServerError)
		return
	}
	contentType, ok := result.Ctx.Value(consts.ContentType).(string)
	if !ok {
		contentType = consts.TypeJson
	}
	switch contentType {
	case consts.TypeHtml:
		w.Header().Set(consts.ContentType, consts.TypeHtml)
		data, err := jsonparser.GetString(result.Payload, "html_content")
		if err != nil {
			return
		}
		w.Write([]byte(data))
	default:
		if r.Method != "POST" {
			http.Error(w, `{"message": "not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set(consts.ContentType, consts.TypeJson)
		json.NewEncoder(w).Encode(result.Payload)
	}
}

func (tm *DAG) taskStatusHandler(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("taskID")
	if taskID == "" {
		http.Error(w, `{"message": "taskID is missing"}`, http.StatusBadRequest)
		return
	}
	manager, ok := tm.taskManager.Get(taskID)
	if !ok {
		http.Error(w, `{"message": "Invalid TaskID"}`, http.StatusNotFound)
		return
	}
	result := make(map[string]TaskState)
	manager.taskStates.ForEach(func(key string, value *TaskState) bool {
		key = strings.Split(key, Delimiter)[0]
		nodeID := strings.Split(value.NodeID, Delimiter)[0]
		rs := jsonparser.Delete(value.Result.Payload, "html_content")
		status := value.Status
		if status == mq.Processing {
			status = mq.Completed
		}
		state := TaskState{
			NodeID:    nodeID,
			Status:    status,
			UpdatedAt: value.UpdatedAt,
			Result: mq.Result{
				Payload: rs,
				Error:   value.Result.Error,
				Status:  status,
			},
		}
		result[key] = state
		return true
	})
	w.Header().Set(consts.ContentType, consts.TypeJson)
	json.NewEncoder(w).Encode(result)
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
