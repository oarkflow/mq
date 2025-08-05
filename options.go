package mq

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/oarkflow/mq/logger"
	"github.com/oarkflow/mq/utils"
)

type ThresholdConfig struct {
	HighMemory    int64
	LongExecution time.Duration
}

type MetricsRegistry interface {
	Register(metricName string, value interface{})
	Increment(metricName string)
	Get(metricName string) interface{}
}

type CircuitBreakerConfig struct {
	Enabled          bool
	FailureThreshold int
	ResetTimeout     time.Duration
}

type PoolOption func(*Pool)

func WithTaskQueueSize(size int) PoolOption {
	return func(p *Pool) {
		p.taskQueue = make(PriorityQueue, 0, size)
	}
}

func WithTaskTimeout(t time.Duration) PoolOption {
	return func(p *Pool) {
		p.timeout = t
	}
}

func WithCompletionCallback(callback func()) PoolOption {
	return func(p *Pool) {
		p.completionCallback = callback
	}
}

func WithMaxMemoryLoad(maxMemoryLoad int64) PoolOption {
	return func(p *Pool) {
		p.maxMemoryLoad = maxMemoryLoad
	}
}

func WithBatchSize(batchSize int) PoolOption {
	return func(p *Pool) {
		p.batchSize = batchSize
	}
}

func WithHandler(handler Handler) PoolOption {
	return func(p *Pool) {
		p.handler = handler
	}
}

func WithPoolCallback(callback Callback) PoolOption {
	return func(p *Pool) {
		p.callback = callback
	}
}

func WithTaskStorage(storage TaskStorage) PoolOption {
	return func(p *Pool) {
		p.taskStorage = storage
	}
}

func WithWarningThresholds(thresholds ThresholdConfig) PoolOption {
	return func(p *Pool) {
		p.thresholds = thresholds
	}
}

func WithDiagnostics(enabled bool) PoolOption {
	return func(p *Pool) {
		p.diagnosticsEnabled = enabled
	}
}

func WithMetricsRegistry(registry MetricsRegistry) PoolOption {
	return func(p *Pool) {
		p.metricsRegistry = registry
	}
}

func WithCircuitBreaker(config CircuitBreakerConfig) PoolOption {
	return func(p *Pool) {
		p.circuitBreaker = config
	}
}

func WithGracefulShutdown(timeout time.Duration) PoolOption {
	return func(p *Pool) {
		p.gracefulShutdownTimeout = timeout
	}
}

func WithPlugin(plugin Plugin) PoolOption {
	return func(p *Pool) {
		p.plugins = append(p.plugins, plugin)
	}
}

var BrokerAddr string

func init() {
	if BrokerAddr == "" {
		port, err := utils.GetRandomPort()
		if err != nil {
			BrokerAddr = ":8081"
		} else {
			BrokerAddr = fmt.Sprintf(":%d", port)
		}
	}
}

func defaultOptions() *Options {
	return &Options{
		brokerAddr:           BrokerAddr,
		maxRetries:           5,
		respondPendingResult: true,
		initialDelay:         2 * time.Second,
		maxBackoff:           20 * time.Second,
		jitterPercent:        0.5,
		queueSize:            100,
		numOfWorkers:         runtime.NumCPU(),
		maxMemoryLoad:        5000000,
		storage:              NewMemoryTaskStorage(10 * time.Minute),
		logger:               logger.NewDefaultLogger(),
		consumerTimeout:      30 * time.Second, // default timeout for backward compatibility
	}
}

type Option func(*Options)

func SetupOptions(opts ...Option) *Options {
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	return options
}

func WithNotifyResponse(callback Callback) Option {
	return func(opts *Options) {
		opts.notifyResponse = callback
	}
}

func WithLogger(log logger.Logger) Option {
	return func(opts *Options) {
		if log == nil {
			opts.logger = logger.NewNullLogger()
		} else {
			opts.logger = log
		}
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

// WithHTTPApi - Option to enable/disable TLS
func WithHTTPApi(flag bool) Option {
	return func(o *Options) {
		o.enableHTTPApi = flag
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

// WithCleanTaskOnComplete -
func WithCleanTaskOnComplete() Option {
	return func(opts *Options) {
		opts.cleanTaskOnComplete = true
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

func WithBrokerRateLimiter(rate int, burst int) Option {
	return func(opts *Options) {
		opts.BrokerRateLimiter = NewRateLimiter(rate, burst)
	}
}

func WithConsumerRateLimiter(rate int, burst int) Option {
	return func(opts *Options) {
		opts.ConsumerRateLimiter = NewRateLimiter(rate, burst)
	}
}

func DisableBrokerRateLimit() Option {
	return func(opts *Options) {
		opts.BrokerRateLimiter = nil
	}
}

func DisableConsumerRateLimit() Option {
	return func(opts *Options) {
		opts.ConsumerRateLimiter = nil
	}
}

// TaskOption defines a function type for setting options.
type TaskOption func(*Task)

func WithDAG(dag any) TaskOption {
	return func(opts *Task) {
		opts.dag = dag
	}
}

func WithConsumerTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		opts.consumerTimeout = timeout
	}
}
