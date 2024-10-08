package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Result struct {
	Ctx     context.Context
	Payload json.RawMessage `json:"payload"`
	Topic   string          `json:"topic"`
	TaskID  string          `json:"task_id"`
	Error   error           `json:"error,omitempty"`
	Status  string          `json:"status"`
}

func (r Result) Unmarshal(data any) error {
	if r.Payload == nil {
		return fmt.Errorf("payload is nil")
	}
	return json.Unmarshal(r.Payload, data)
}

func (r Result) String() string {
	return string(r.Payload)
}

func HandleError(ctx context.Context, err error, status ...string) Result {
	st := "Failed"
	if len(status) > 0 {
		st = status[0]
	}
	if err == nil {
		return Result{}
	}
	return Result{
		Status: st,
		Error:  err,
		Ctx:    ctx,
	}
}

func (r Result) WithData(status string, data []byte) Result {
	if r.Error != nil {
		return r
	}
	return Result{
		Status:  status,
		Payload: data,
		Error:   nil,
		Ctx:     r.Ctx,
	}
}

type TLSConfig struct {
	UseTLS   bool
	CertPath string
	KeyPath  string
	CAPath   string
}

type Options struct {
	syncMode         bool
	brokerAddr       string
	callback         []func(context.Context, Result) Result
	maxRetries       int
	initialDelay     time.Duration
	maxBackoff       time.Duration
	jitterPercent    float64
	tlsConfig        TLSConfig
	aesKey           json.RawMessage
	hmacKey          json.RawMessage
	enableEncryption bool
	queueSize        int
}

func defaultOptions() Options {
	return Options{
		syncMode:      false,
		brokerAddr:    ":8080",
		maxRetries:    5,
		initialDelay:  2 * time.Second,
		maxBackoff:    20 * time.Second,
		jitterPercent: 0.5,
		queueSize:     100,
	}
}

// Option defines a function type for setting options.
type Option func(*Options)

func setupOptions(opts ...Option) Options {
	options := defaultOptions()
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

func WithEncryption(aesKey, hmacKey json.RawMessage, enableEncryption bool) Option {
	return func(opts *Options) {
		opts.aesKey = aesKey
		opts.hmacKey = hmacKey
		opts.enableEncryption = enableEncryption
	}
}

// WithBrokerURL -
func WithBrokerURL(url string) Option {
	return func(opts *Options) {
		opts.brokerAddr = url
	}
}

// WithTLS - Option to enable/disable TLS
func WithTLS(enableTLS bool, certPath, keyPath string) Option {
	return func(o *Options) {
		o.tlsConfig.UseTLS = enableTLS
		o.tlsConfig.CertPath = certPath
		o.tlsConfig.KeyPath = keyPath
	}
}

// WithCAPath - Option to enable/disable TLS
func WithCAPath(caPath string) Option {
	return func(o *Options) {
		o.tlsConfig.CAPath = caPath
	}
}

// WithSyncMode -
func WithSyncMode(mode bool) Option {
	return func(opts *Options) {
		opts.syncMode = mode
	}
}

// WithMaxRetries -
func WithMaxRetries(val int) Option {
	return func(opts *Options) {
		opts.maxRetries = val
	}
}

// WithInitialDelay -
func WithInitialDelay(val time.Duration) Option {
	return func(opts *Options) {
		opts.initialDelay = val
	}
}

// WithMaxBackoff -
func WithMaxBackoff(val time.Duration) Option {
	return func(opts *Options) {
		opts.maxBackoff = val
	}
}

// WithCallback -
func WithCallback(val ...func(context.Context, Result) Result) Option {
	return func(opts *Options) {
		opts.callback = val
	}
}

// WithJitterPercent -
func WithJitterPercent(val float64) Option {
	return func(opts *Options) {
		opts.jitterPercent = val
	}
}
