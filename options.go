package mq

import (
	"context"
	"time"
)

type Options struct {
	syncMode       bool
	brokerAddr     string
	messageHandler MessageHandler
	closeHandler   CloseHandler
	errorHandler   ErrorHandler
	callback       []func(context.Context, Result) Result
	maxRetries     int
	initialDelay   time.Duration
	maxBackoff     time.Duration
	jitterPercent  float64
	useTLS         bool   // Add a flag to enable/disable TLS
	tlsCertPath    string // Path to TLS certificate
	tlsKeyPath     string // Path to TLS private key
	tlsCAPath      string // Path to the Certificate Authority (optional)
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

// WithBrokerURL -
func WithBrokerURL(url string) Option {
	return func(opts *Options) {
		opts.brokerAddr = url
	}
}

// Option to enable/disable TLS
func WithTLS(enableTLS bool, certPath, keyPath, caPath string) Option {
	return func(o *Options) {
		o.useTLS = enableTLS
		o.tlsCertPath = certPath
		o.tlsKeyPath = keyPath
		o.tlsCAPath = caPath
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

// WithMessageHandler sets a custom MessageHandler.
func WithMessageHandler(handler MessageHandler) Option {
	return func(opts *Options) {
		opts.messageHandler = handler
	}
}

// WithErrorHandler sets a custom ErrorHandler.
func WithErrorHandler(handler ErrorHandler) Option {
	return func(opts *Options) {
		opts.errorHandler = handler
	}
}

// WithCloseHandler sets a custom CloseHandler.
func WithCloseHandler(handler CloseHandler) Option {
	return func(opts *Options) {
		opts.closeHandler = handler
	}
}
