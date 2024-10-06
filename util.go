package mq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/mq/codec"
)

func (b *Broker) TLSConfig() TLSConfig {
	return b.opts.tlsConfig
}

func (b *Broker) SyncMode() bool {
	return b.opts.syncMode
}

func (b *Broker) HandleCallback(ctx context.Context, msg *codec.Message) {
	if b.opts.callback != nil {
		var result Result
		err := json.Unmarshal(msg.Payload, &result)
		if err == nil {
			for _, callback := range b.opts.callback {
				callback(ctx, result)
			}
		}
	}
}

// MarshalJSON implements custom marshaling for Result
func (m Result) MarshalJSON() ([]byte, error) {
	type Alias Result // create an alias to avoid recursion
	return json.Marshal(&struct {
		Err *string `json:"error,omitempty"`
		*Alias
	}{
		Err:   marshalError(m.Error),
		Alias: (*Alias)(&m),
	})
}

// UnmarshalJSON implements custom unmarshaling for Result
func (m *Result) UnmarshalJSON(data []byte) error {
	type Alias Result // create an alias to avoid recursion
	aux := &struct {
		Err *string `json:"error,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(m),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	m.Error = unmarshalError(aux.Err)
	return nil
}

// Helper function to marshal an error to a string pointer
func marshalError(err error) *string {
	if err == nil {
		return nil
	}
	msg := err.Error()
	return &msg
}

// Helper function to unmarshal a string pointer to an error
func unmarshalError(errStr *string) error {
	if errStr == nil {
		return nil
	}
	return fmt.Errorf(*errStr)
}
