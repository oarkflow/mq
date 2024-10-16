package mq

import (
	"encoding/json"
	"time"
)

func NewTask(id string, payload json.RawMessage, nodeKey string) *Task {
	if id == "" {
		id = NewID()
	}
	return &Task{ID: id, Payload: payload, Topic: nodeKey, CreatedAt: time.Now()}
}
