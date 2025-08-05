package handlers

import (
	"context"
	"fmt"
	"github.com/oarkflow/json"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"

	"github.com/oarkflow/log"
)

type LogHandler struct {
	dag.Operation
}

func (p *LogHandler) ProcessTask(ctx context.Context, task *mq.Task) mq.Result {
	var row map[string]any
	data := task.Payload
	if data != nil {
		err := json.Unmarshal(data, &row)
		if err != nil {
			return mq.Result{
				Ctx:   ctx,
				Error: err,
			}
		}
	}
	var msg string
	toReturn := make(map[string]any)
	for k, v := range p.Payload.Mapping {
		_, val := dag.GetVal(ctx, v, row)
		toReturn[k] = val
	}
	if val, exist := p.Payload.Data["message"]; exist {
		_, v := dag.GetVal(ctx, val.(string), toReturn)
		if v != nil {
			msg = v.(string)
		} else {
			msg = val.(string)
		}
	}
	logger := log.Info()
	if len(toReturn) > 0 {
		for k, v := range toReturn {
			logger = logger.Any(k, v)
		}
	}
	logger.Msg(msg)
	if _, exist := p.Payload.Data["print"]; exist {
		fmt.Println(toReturn, msg)
	}
	return mq.Result{
		Ctx:     ctx,
		Payload: task.Payload,
	}
}

func NewLogHandler(id string) *LogHandler {
	return &LogHandler{Operation: dag.Operation{Key: "log", ID: id, Type: dag.Function, Tags: []string{"built-in"}}}
}
