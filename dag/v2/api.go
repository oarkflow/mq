package v2

import (
	"encoding/json"
	"net/http"

	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/jsonparser"
)

func (tm *DAG) render(w http.ResponseWriter, request *http.Request) {
	ctx, data, err := parse(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	result := tm.ProcessTask(ctx, data)
	contentType, ok := result.Ctx.Value(consts.ContentType).(string)
	if !ok {
		contentType = consts.TypeJson
	}
	switch contentType {
	case consts.TypeHtml:
		w.Header().Set(consts.ContentType, consts.TypeHtml)
		data, err := jsonparser.GetString(result.Data, "html_content")
		if err != nil {
			return
		}
		w.Write([]byte(data))
	default:
		if request.Method != "POST" {
			http.Error(w, `{"message": "not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set(consts.ContentType, consts.TypeJson)
		json.NewEncoder(w).Encode(result.Data)
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
	for key, value := range manager.taskStates {
		rs := jsonparser.Delete(value.Result.Data, "html_content")
		state := TaskState{
			NodeID:    value.NodeID,
			Status:    value.Status,
			UpdatedAt: value.UpdatedAt,
			Result: Result{
				Data:   rs,
				Error:  value.Result.Error,
				Status: value.Result.Status,
			},
		}
		result[key] = state
	}
	w.Header().Set(consts.ContentType, consts.TypeJson)
	json.NewEncoder(w).Encode(result)
}

func (tm *DAG) Start(addr string) {
	http.HandleFunc("/process", tm.render)
	http.HandleFunc("/task/status", tm.taskStatusHandler)
	http.ListenAndServe(addr, nil)
}
