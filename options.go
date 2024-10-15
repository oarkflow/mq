package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/oarkflow/mq/consts"
)

type Result struct {
	CreatedAt   time.Time       `json:"created_at"`
	ProcessedAt time.Time       `json:"processed_at,omitempty"`
	Latency     string          `json:"latency"`
	Error       error           `json:"error,omitempty"`
	Topic       string          `json:"topic"`
	TaskID      string          `json:"task_id"`
	Status      string          `json:"status"`
	Ctx         context.Context `json:"-"`
	Payload     json.RawMessage `json:"payload"`
}

func (r Result) Unmarshal(data any) error {
	if r.Payload == nil {
		return fmt.Errorf("payload is nil")
	}
	return json.Unmarshal(r.Payload, data)
}

func HandleError(ctx context.Context, err error, status ...string) Result {
	st := "Failed"
	if len(status) > 0 {
		st = status[0]
	}
	if err == nil {
		return Result{Ctx: ctx}
	}
	return Result{
		Ctx:    ctx,
		Status: st,
		Error:  err,
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
	CertPath string
	KeyPath  string
	CAPath   string
	UseTLS   bool
}

type Options struct {
	consumerOnSubscribe  func(ctx context.Context, topic, consumerName string)
	consumerOnClose      func(ctx context.Context, topic, consumerName string)
	notifyResponse       func(context.Context, Result)
	tlsConfig            TLSConfig
	brokerAddr           string
	callback             []func(context.Context, Result) Result
	maxRetries           int
	initialDelay         time.Duration
	maxBackoff           time.Duration
	jitterPercent        float64
	queueSize            int
	numOfWorkers         int
	maxMemoryLoad        int64
	syncMode             bool
	enableWorkerPool     bool
	respondPendingResult bool
}

func (o *Options) SetSyncMode(sync bool) {
	o.syncMode = sync
}

func (o *Options) NumOfWorkers() int {
	return o.numOfWorkers
}

func (o *Options) QueueSize() int {
	return o.queueSize
}

func (o *Options) MaxMemoryLoad() int64 {
	return o.maxMemoryLoad
}

func defaultOptions() *Options {
	return &Options{
		brokerAddr:           ":8080",
		maxRetries:           5,
		respondPendingResult: true,
		initialDelay:         2 * time.Second,
		maxBackoff:           20 * time.Second,
		jitterPercent:        0.5,
		queueSize:            100,
		numOfWorkers:         runtime.NumCPU(),
		maxMemoryLoad:        5000000,
	}
}

// Option defines a function type for setting options.
type Option func(*Options)

func SetupOptions(opts ...Option) *Options {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}

func WithNotifyResponse(handler func(ctx context.Context, result Result)) Option {
	return func(opts *Options) {
		opts.notifyResponse = handler
	}
}

func WithWorkerPool(queueSize, numOfWorkers int, maxMemoryLoad int64) Option {
	return func(opts *Options) {
		opts.enableWorkerPool = true
		opts.queueSize = queueSize
		opts.numOfWorkers = numOfWorkers
		opts.maxMemoryLoad = maxMemoryLoad
	}
}

func WithConsumerOnSubscribe(handler func(ctx context.Context, topic, consumerName string)) Option {
	return func(opts *Options) {
		opts.consumerOnSubscribe = handler
	}
}

func WithConsumerOnClose(handler func(ctx context.Context, topic, consumerName string)) Option {
	return func(opts *Options) {
		opts.consumerOnClose = handler
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

// WithRespondPendingResult -
func WithRespondPendingResult(mode bool) Option {
	return func(opts *Options) {
		opts.respondPendingResult = mode
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

func HeadersWithConsumerID(ctx context.Context, id string) map[string]string {
	return WithHeaders(ctx, map[string]string{consts.ConsumerKey: id, consts.ContentType: consts.TypeJson})
}

func HeadersWithConsumerIDAndQueue(ctx context.Context, id, queue string) map[string]string {
	return WithHeaders(ctx, map[string]string{
		consts.ConsumerKey: id,
		consts.ContentType: consts.TypeJson,
		consts.QueueKey:    queue,
	})
}
