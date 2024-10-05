package v2

import (
	"context"
	"encoding/json"
	"time"
)

type Result struct {
	Payload   json.RawMessage `json:"payload"`
	Queue     string          `json:"queue"`
	MessageID string          `json:"message_id"`
	Error     error           `json:"error,omitempty"`
	Status    string          `json:"status"`
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
}

func defaultOptions() Options {
	return Options{
		syncMode:      false,
		brokerAddr:    ":8080",
		maxRetries:    5,
		initialDelay:  2 * time.Second,
		maxBackoff:    20 * time.Second,
		jitterPercent: 0.5,
	}
}

// Option defines a function type for setting options.
type Option func(*Options)

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
