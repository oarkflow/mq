package mq

import (
	"context"
	"encoding/json"

	"github.com/oarkflow/mq/codec"
)

func (b *Broker) TLSConfig() TLSConfig {
	return b.opts.tlsConfig
}

func (b *Broker) SyncMode() bool {
	return b.opts.syncMode
}

func (b *Broker) NotifyHandler() func(context.Context, Result) error {
	return b.opts.notifyResponse
}

func (b *Broker) SetNotifyHandler(callback Callback) {
	b.opts.notifyResponse = callback
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
