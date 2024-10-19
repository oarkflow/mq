package mq

import (
	"encoding/json"
	"time"
)

type Task struct {
	CreatedAt   time.Time       `json:"created_at"`
	ProcessedAt time.Time       `json:"processed_at"`
	Expiry      time.Time       `json:"expiry"`
	Error       error           `json:"error"`
	ID          string          `json:"id"`
	Topic       string          `json:"topic"`
	Status      string          `json:"status"`
	Payload     json.RawMessage `json:"payload"`
}

func NewTask(id string, payload json.RawMessage, nodeKey string) *Task {
	if id == "" {
		id = NewID()
	}
	return &Task{ID: id, Payload: payload, Topic: nodeKey, CreatedAt: time.Now()}
}
