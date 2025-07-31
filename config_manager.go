package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
)

// ConfigManager handles dynamic configuration management
type ConfigManager struct {
	config     *ProductionConfig
	watchers   []ConfigWatcher
	mu         sync.RWMutex
	logger     logger.Logger
	configFile string
}

// ProductionConfig contains all production configuration
type ProductionConfig struct {
	Broker      BrokerConfig      `json:"broker"`
	Consumer    ConsumerConfig    `json:"consumer"`
	Publisher   PublisherConfig   `json:"publisher"`
	Pool        PoolConfig        `json:"pool"`
	Security    SecurityConfig    `json:"security"`
	Monitoring  MonitoringConfig  `json:"monitoring"`
	Persistence PersistenceConfig `json:"persistence"`
	Clustering  ClusteringConfig  `json:"clustering"`
	RateLimit   RateLimitConfig   `json:"rate_limit"`
	LastUpdated time.Time         `json:"last_updated"`
}

// BrokerConfig contains broker-specific configuration
type BrokerConfig struct {
	Address              string            `json:"address"`
	Port                 int               `json:"port"`
	MaxConnections       int               `json:"max_connections"`
	ConnectionTimeout    time.Duration     `json:"connection_timeout"`
	ReadTimeout          time.Duration     `json:"read_timeout"`
	WriteTimeout         time.Duration     `json:"write_timeout"`
	IdleTimeout          time.Duration     `json:"idle_timeout"`
	KeepAlive            bool              `json:"keep_alive"`
	KeepAlivePeriod      time.Duration     `json:"keep_alive_period"`
	MaxQueueDepth        int               `json:"max_queue_depth"`
	EnableDeadLetter     bool              `json:"enable_dead_letter"`
	DeadLetterMaxRetries int               `json:"dead_letter_max_retries"`
	EnableMetrics        bool              `json:"enable_metrics"`
	MetricsInterval      time.Duration     `json:"metrics_interval"`
	GracefulShutdown     time.Duration     `json:"graceful_shutdown"`
	MessageTTL           time.Duration     `json:"message_ttl"`
	Headers              map[string]string `json:"headers"`
}

// ConsumerConfig contains consumer-specific configuration
type ConsumerConfig struct {
	MaxRetries              int           `json:"max_retries"`
	InitialDelay            time.Duration `json:"initial_delay"`
	MaxBackoff              time.Duration `json:"max_backoff"`
	JitterPercent           float64       `json:"jitter_percent"`
	EnableReconnect         bool          `json:"enable_reconnect"`
	ReconnectInterval       time.Duration `json:"reconnect_interval"`
	HealthCheckInterval     time.Duration `json:"health_check_interval"`
	MaxConcurrentTasks      int           `json:"max_concurrent_tasks"`
	TaskTimeout             time.Duration `json:"task_timeout"`
	EnableDeduplication     bool          `json:"enable_deduplication"`
	DeduplicationWindow     time.Duration `json:"deduplication_window"`
	EnablePriorityQueue     bool          `json:"enable_priority_queue"`
	EnableHTTPAPI           bool          `json:"enable_http_api"`
	HTTPAPIPort             int           `json:"http_api_port"`
	EnableCircuitBreaker    bool          `json:"enable_circuit_breaker"`
	CircuitBreakerThreshold int           `json:"circuit_breaker_threshold"`
	CircuitBreakerTimeout   time.Duration `json:"circuit_breaker_timeout"`
}

// PublisherConfig contains publisher-specific configuration
type PublisherConfig struct {
	MaxRetries            int           `json:"max_retries"`
	InitialDelay          time.Duration `json:"initial_delay"`
	MaxBackoff            time.Duration `json:"max_backoff"`
	JitterPercent         float64       `json:"jitter_percent"`
	ConnectionPoolSize    int           `json:"connection_pool_size"`
	PublishTimeout        time.Duration `json:"publish_timeout"`
	EnableBatching        bool          `json:"enable_batching"`
	BatchSize             int           `json:"batch_size"`
	BatchTimeout          time.Duration `json:"batch_timeout"`
	EnableCompression     bool          `json:"enable_compression"`
	CompressionLevel      int           `json:"compression_level"`
	EnableAsync           bool          `json:"enable_async"`
	AsyncBufferSize       int           `json:"async_buffer_size"`
	EnableOrderedDelivery bool          `json:"enable_ordered_delivery"`
}

// PoolConfig contains worker pool configuration
type PoolConfig struct {
	MinWorkers               int           `json:"min_workers"`
	MaxWorkers               int           `json:"max_workers"`
	QueueSize                int           `json:"queue_size"`
	MaxMemoryLoad            int64         `json:"max_memory_load"`
	TaskTimeout              time.Duration `json:"task_timeout"`
	IdleWorkerTimeout        time.Duration `json:"idle_worker_timeout"`
	EnableDynamicScaling     bool          `json:"enable_dynamic_scaling"`
	ScalingFactor            float64       `json:"scaling_factor"`
	ScalingInterval          time.Duration `json:"scaling_interval"`
	MaxQueueWaitTime         time.Duration `json:"max_queue_wait_time"`
	EnableWorkStealing       bool          `json:"enable_work_stealing"`
	EnablePriorityScheduling bool          `json:"enable_priority_scheduling"`
	GracefulShutdownTimeout  time.Duration `json:"graceful_shutdown_timeout"`
}

// SecurityConfig contains security-related configuration
type SecurityConfig struct {
	EnableTLS             bool          `json:"enable_tls"`
	TLSCertPath           string        `json:"tls_cert_path"`
	TLSKeyPath            string        `json:"tls_key_path"`
	TLSCAPath             string        `json:"tls_ca_path"`
	TLSInsecureSkipVerify bool          `json:"tls_insecure_skip_verify"`
	EnableAuthentication  bool          `json:"enable_authentication"`
	AuthenticationMethod  string        `json:"authentication_method"` // "basic", "jwt", "oauth"
	EnableAuthorization   bool          `json:"enable_authorization"`
	EnableEncryption      bool          `json:"enable_encryption"`
	EncryptionKey         string        `json:"encryption_key"`
	EnableAuditLog        bool          `json:"enable_audit_log"`
	AuditLogPath          string        `json:"audit_log_path"`
	SessionTimeout        time.Duration `json:"session_timeout"`
	MaxLoginAttempts      int           `json:"max_login_attempts"`
	LockoutDuration       time.Duration `json:"lockout_duration"`
}

// MonitoringConfig contains monitoring and observability configuration
type MonitoringConfig struct {
	EnableMetrics       bool          `json:"enable_metrics"`
	MetricsPort         int           `json:"metrics_port"`
	MetricsPath         string        `json:"metrics_path"`
	EnableHealthCheck   bool          `json:"enable_health_check"`
	HealthCheckPort     int           `json:"health_check_port"`
	HealthCheckPath     string        `json:"health_check_path"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	EnableTracing       bool          `json:"enable_tracing"`
	TracingEndpoint     string        `json:"tracing_endpoint"`
	TracingSampleRate   float64       `json:"tracing_sample_rate"`
	EnableLogging       bool          `json:"enable_logging"`
	LogLevel            string        `json:"log_level"`
	LogFormat           string        `json:"log_format"` // "json", "text"
	LogOutput           string        `json:"log_output"` // "stdout", "file", "syslog"
	LogFilePath         string        `json:"log_file_path"`
	LogMaxSize          int           `json:"log_max_size"` // MB
	LogMaxBackups       int           `json:"log_max_backups"`
	LogMaxAge           int           `json:"log_max_age"` // days
	EnableProfiling     bool          `json:"enable_profiling"`
	ProfilingPort       int           `json:"profiling_port"`
}

// PersistenceConfig contains data persistence configuration
type PersistenceConfig struct {
	EnablePersistence  bool          `json:"enable_persistence"`
	StorageType        string        `json:"storage_type"` // "memory", "file", "redis", "postgres", "mysql"
	ConnectionString   string        `json:"connection_string"`
	MaxConnections     int           `json:"max_connections"`
	ConnectionTimeout  time.Duration `json:"connection_timeout"`
	RetentionPeriod    time.Duration `json:"retention_period"`
	CleanupInterval    time.Duration `json:"cleanup_interval"`
	BackupEnabled      bool          `json:"backup_enabled"`
	BackupInterval     time.Duration `json:"backup_interval"`
	BackupPath         string        `json:"backup_path"`
	CompressionEnabled bool          `json:"compression_enabled"`
	EncryptionEnabled  bool          `json:"encryption_enabled"`
	ReplicationEnabled bool          `json:"replication_enabled"`
	ReplicationNodes   []string      `json:"replication_nodes"`
}

// ClusteringConfig contains clustering configuration
type ClusteringConfig struct {
	EnableClustering      bool          `json:"enable_clustering"`
	NodeID                string        `json:"node_id"`
	ClusterNodes          []string      `json:"cluster_nodes"`
	DiscoveryMethod       string        `json:"discovery_method"` // "static", "consul", "etcd", "k8s"
	DiscoveryEndpoint     string        `json:"discovery_endpoint"`
	HeartbeatInterval     time.Duration `json:"heartbeat_interval"`
	ElectionTimeout       time.Duration `json:"election_timeout"`
	EnableLoadBalancing   bool          `json:"enable_load_balancing"`
	LoadBalancingStrategy string        `json:"load_balancing_strategy"` // "round_robin", "least_connections", "hash"
	EnableFailover        bool          `json:"enable_failover"`
	FailoverTimeout       time.Duration `json:"failover_timeout"`
	EnableReplication     bool          `json:"enable_replication"`
	ReplicationFactor     int           `json:"replication_factor"`
	ConsistencyLevel      string        `json:"consistency_level"` // "weak", "strong", "eventual"
}

// RateLimitConfig contains rate limiting configuration
type RateLimitConfig struct {
	EnableBrokerRateLimit    bool `json:"enable_broker_rate_limit"`
	BrokerRate               int  `json:"broker_rate"` // requests per second
	BrokerBurst              int  `json:"broker_burst"`
	EnableConsumerRateLimit  bool `json:"enable_consumer_rate_limit"`
	ConsumerRate             int  `json:"consumer_rate"`
	ConsumerBurst            int  `json:"consumer_burst"`
	EnablePublisherRateLimit bool `json:"enable_publisher_rate_limit"`
	PublisherRate            int  `json:"publisher_rate"`
	PublisherBurst           int  `json:"publisher_burst"`
	EnablePerQueueRateLimit  bool `json:"enable_per_queue_rate_limit"`
	PerQueueRate             int  `json:"per_queue_rate"`
	PerQueueBurst            int  `json:"per_queue_burst"`
}

// Custom unmarshaling to handle duration strings
func (c *ProductionConfig) UnmarshalJSON(data []byte) error {
	type Alias ProductionConfig
	aux := &struct {
		*Alias
		LastUpdated string `json:"last_updated"`
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.LastUpdated != "" {
		if t, err := time.Parse(time.RFC3339, aux.LastUpdated); err == nil {
			c.LastUpdated = t
		}
	}

	return nil
}

func (b *BrokerConfig) UnmarshalJSON(data []byte) error {
	type Alias BrokerConfig
	aux := &struct {
		*Alias
		ConnectionTimeout string `json:"connection_timeout"`
		ReadTimeout       string `json:"read_timeout"`
		WriteTimeout      string `json:"write_timeout"`
		IdleTimeout       string `json:"idle_timeout"`
		KeepAlivePeriod   string `json:"keep_alive_period"`
		MetricsInterval   string `json:"metrics_interval"`
		GracefulShutdown  string `json:"graceful_shutdown"`
		MessageTTL        string `json:"message_ttl"`
	}{
		Alias: (*Alias)(b),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if aux.ConnectionTimeout != "" {
		if b.ConnectionTimeout, err = time.ParseDuration(aux.ConnectionTimeout); err != nil {
			return fmt.Errorf("invalid connection_timeout: %w", err)
		}
	}
	if aux.ReadTimeout != "" {
		if b.ReadTimeout, err = time.ParseDuration(aux.ReadTimeout); err != nil {
			return fmt.Errorf("invalid read_timeout: %w", err)
		}
	}
	if aux.WriteTimeout != "" {
		if b.WriteTimeout, err = time.ParseDuration(aux.WriteTimeout); err != nil {
			return fmt.Errorf("invalid write_timeout: %w", err)
		}
	}
	if aux.IdleTimeout != "" {
		if b.IdleTimeout, err = time.ParseDuration(aux.IdleTimeout); err != nil {
			return fmt.Errorf("invalid idle_timeout: %w", err)
		}
	}
	if aux.KeepAlivePeriod != "" {
		if b.KeepAlivePeriod, err = time.ParseDuration(aux.KeepAlivePeriod); err != nil {
			return fmt.Errorf("invalid keep_alive_period: %w", err)
		}
	}
	if aux.MetricsInterval != "" {
		if b.MetricsInterval, err = time.ParseDuration(aux.MetricsInterval); err != nil {
			return fmt.Errorf("invalid metrics_interval: %w", err)
		}
	}
	if aux.GracefulShutdown != "" {
		if b.GracefulShutdown, err = time.ParseDuration(aux.GracefulShutdown); err != nil {
			return fmt.Errorf("invalid graceful_shutdown: %w", err)
		}
	}
	if aux.MessageTTL != "" {
		if b.MessageTTL, err = time.ParseDuration(aux.MessageTTL); err != nil {
			return fmt.Errorf("invalid message_ttl: %w", err)
		}
	}

	return nil
}

func (c *ConsumerConfig) UnmarshalJSON(data []byte) error {
	type Alias ConsumerConfig
	aux := &struct {
		*Alias
		InitialDelay          string `json:"initial_delay"`
		MaxBackoff            string `json:"max_backoff"`
		ReconnectInterval     string `json:"reconnect_interval"`
		HealthCheckInterval   string `json:"health_check_interval"`
		TaskTimeout           string `json:"task_timeout"`
		DeduplicationWindow   string `json:"deduplication_window"`
		CircuitBreakerTimeout string `json:"circuit_breaker_timeout"`
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if aux.InitialDelay != "" {
		if c.InitialDelay, err = time.ParseDuration(aux.InitialDelay); err != nil {
			return fmt.Errorf("invalid initial_delay: %w", err)
		}
	}
	if aux.MaxBackoff != "" {
		if c.MaxBackoff, err = time.ParseDuration(aux.MaxBackoff); err != nil {
			return fmt.Errorf("invalid max_backoff: %w", err)
		}
	}
	if aux.ReconnectInterval != "" {
		if c.ReconnectInterval, err = time.ParseDuration(aux.ReconnectInterval); err != nil {
			return fmt.Errorf("invalid reconnect_interval: %w", err)
		}
	}
	if aux.HealthCheckInterval != "" {
		if c.HealthCheckInterval, err = time.ParseDuration(aux.HealthCheckInterval); err != nil {
			return fmt.Errorf("invalid health_check_interval: %w", err)
		}
	}
	if aux.TaskTimeout != "" {
		if c.TaskTimeout, err = time.ParseDuration(aux.TaskTimeout); err != nil {
			return fmt.Errorf("invalid task_timeout: %w", err)
		}
	}
	if aux.DeduplicationWindow != "" {
		if c.DeduplicationWindow, err = time.ParseDuration(aux.DeduplicationWindow); err != nil {
			return fmt.Errorf("invalid deduplication_window: %w", err)
		}
	}
	if aux.CircuitBreakerTimeout != "" {
		if c.CircuitBreakerTimeout, err = time.ParseDuration(aux.CircuitBreakerTimeout); err != nil {
			return fmt.Errorf("invalid circuit_breaker_timeout: %w", err)
		}
	}

	return nil
}

func (p *PublisherConfig) UnmarshalJSON(data []byte) error {
	type Alias PublisherConfig
	aux := &struct {
		*Alias
		InitialDelay   string `json:"initial_delay"`
		MaxBackoff     string `json:"max_backoff"`
		PublishTimeout string `json:"publish_timeout"`
		BatchTimeout   string `json:"batch_timeout"`
	}{
		Alias: (*Alias)(p),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if aux.InitialDelay != "" {
		if p.InitialDelay, err = time.ParseDuration(aux.InitialDelay); err != nil {
			return fmt.Errorf("invalid initial_delay: %w", err)
		}
	}
	if aux.MaxBackoff != "" {
		if p.MaxBackoff, err = time.ParseDuration(aux.MaxBackoff); err != nil {
			return fmt.Errorf("invalid max_backoff: %w", err)
		}
	}
	if aux.PublishTimeout != "" {
		if p.PublishTimeout, err = time.ParseDuration(aux.PublishTimeout); err != nil {
			return fmt.Errorf("invalid publish_timeout: %w", err)
		}
	}
	if aux.BatchTimeout != "" {
		if p.BatchTimeout, err = time.ParseDuration(aux.BatchTimeout); err != nil {
			return fmt.Errorf("invalid batch_timeout: %w", err)
		}
	}

	return nil
}

func (p *PoolConfig) UnmarshalJSON(data []byte) error {
	type Alias PoolConfig
	aux := &struct {
		*Alias
		TaskTimeout             string `json:"task_timeout"`
		IdleTimeout             string `json:"idle_timeout"`
		ScalingInterval         string `json:"scaling_interval"`
		MaxQueueWaitTime        string `json:"max_queue_wait_time"`
		GracefulShutdownTimeout string `json:"graceful_shutdown_timeout"`
	}{
		Alias: (*Alias)(p),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if aux.TaskTimeout != "" {
		if p.TaskTimeout, err = time.ParseDuration(aux.TaskTimeout); err != nil {
			return fmt.Errorf("invalid task_timeout: %w", err)
		}
	}
	if aux.IdleTimeout != "" {
		if p.IdleWorkerTimeout, err = time.ParseDuration(aux.IdleTimeout); err != nil {
			return fmt.Errorf("invalid idle_timeout: %w", err)
		}
	}
	if aux.ScalingInterval != "" {
		if p.ScalingInterval, err = time.ParseDuration(aux.ScalingInterval); err != nil {
			return fmt.Errorf("invalid scaling_interval: %w", err)
		}
	}
	if aux.MaxQueueWaitTime != "" {
		if p.MaxQueueWaitTime, err = time.ParseDuration(aux.MaxQueueWaitTime); err != nil {
			return fmt.Errorf("invalid max_queue_wait_time: %w", err)
		}
	}
	if aux.GracefulShutdownTimeout != "" {
		if p.GracefulShutdownTimeout, err = time.ParseDuration(aux.GracefulShutdownTimeout); err != nil {
			return fmt.Errorf("invalid graceful_shutdown_timeout: %w", err)
		}
	}

	return nil
}

func (m *MonitoringConfig) UnmarshalJSON(data []byte) error {
	type Alias MonitoringConfig
	aux := &struct {
		*Alias
		HealthCheckInterval string `json:"health_check_interval"`
		MetricsInterval     string `json:"metrics_interval"`
		RetentionPeriod     string `json:"retention_period"`
	}{
		Alias: (*Alias)(m),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if aux.HealthCheckInterval != "" {
		if m.HealthCheckInterval, err = time.ParseDuration(aux.HealthCheckInterval); err != nil {
			return fmt.Errorf("invalid health_check_interval: %w", err)
		}
	}

	return nil
}

func (p *PersistenceConfig) UnmarshalJSON(data []byte) error {
	type Alias PersistenceConfig
	aux := &struct {
		*Alias
		ConnectionTimeout string `json:"connection_timeout"`
		RetentionPeriod   string `json:"retention_period"`
		CleanupInterval   string `json:"cleanup_interval"`
		BackupInterval    string `json:"backup_interval"`
	}{
		Alias: (*Alias)(p),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if aux.ConnectionTimeout != "" {
		if p.ConnectionTimeout, err = time.ParseDuration(aux.ConnectionTimeout); err != nil {
			return fmt.Errorf("invalid connection_timeout: %w", err)
		}
	}
	if aux.RetentionPeriod != "" {
		if p.RetentionPeriod, err = time.ParseDuration(aux.RetentionPeriod); err != nil {
			return fmt.Errorf("invalid retention_period: %w", err)
		}
	}
	if aux.CleanupInterval != "" {
		if p.CleanupInterval, err = time.ParseDuration(aux.CleanupInterval); err != nil {
			return fmt.Errorf("invalid cleanup_interval: %w", err)
		}
	}
	if aux.BackupInterval != "" {
		if p.BackupInterval, err = time.ParseDuration(aux.BackupInterval); err != nil {
			return fmt.Errorf("invalid backup_interval: %w", err)
		}
	}

	return nil
}

func (c *ClusteringConfig) UnmarshalJSON(data []byte) error {
	type Alias ClusteringConfig
	aux := &struct {
		*Alias
		HeartbeatInterval string `json:"heartbeat_interval"`
		ElectionTimeout   string `json:"election_timeout"`
		FailoverTimeout   string `json:"failover_timeout"`
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if aux.HeartbeatInterval != "" {
		if c.HeartbeatInterval, err = time.ParseDuration(aux.HeartbeatInterval); err != nil {
			return fmt.Errorf("invalid heartbeat_interval: %w", err)
		}
	}
	if aux.ElectionTimeout != "" {
		if c.ElectionTimeout, err = time.ParseDuration(aux.ElectionTimeout); err != nil {
			return fmt.Errorf("invalid election_timeout: %w", err)
		}
	}
	if aux.FailoverTimeout != "" {
		if c.FailoverTimeout, err = time.ParseDuration(aux.FailoverTimeout); err != nil {
			return fmt.Errorf("invalid failover_timeout: %w", err)
		}
	}

	return nil
}

func (s *SecurityConfig) UnmarshalJSON(data []byte) error {
	type Alias SecurityConfig
	aux := &struct {
		*Alias
		SessionTimeout  string `json:"session_timeout"`
		LockoutDuration string `json:"lockout_duration"`
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	if aux.SessionTimeout != "" {
		if s.SessionTimeout, err = time.ParseDuration(aux.SessionTimeout); err != nil {
			return fmt.Errorf("invalid session_timeout: %w", err)
		}
	}
	if aux.LockoutDuration != "" {
		if s.LockoutDuration, err = time.ParseDuration(aux.LockoutDuration); err != nil {
			return fmt.Errorf("invalid lockout_duration: %w", err)
		}
	}

	return nil
}

// ConfigWatcher interface for configuration change notifications
type ConfigWatcher interface {
	OnConfigChange(oldConfig, newConfig *ProductionConfig) error
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(configFile string, logger logger.Logger) *ConfigManager {
	return &ConfigManager{
		config:     DefaultProductionConfig(),
		watchers:   make([]ConfigWatcher, 0),
		logger:     logger,
		configFile: configFile,
	}
}

// DefaultProductionConfig returns default production configuration
func DefaultProductionConfig() *ProductionConfig {
	return &ProductionConfig{
		Broker: BrokerConfig{
			Address:              "localhost",
			Port:                 8080,
			MaxConnections:       1000,
			ConnectionTimeout:    30 * time.Second,
			ReadTimeout:          30 * time.Second,
			WriteTimeout:         30 * time.Second,
			IdleTimeout:          5 * time.Minute,
			KeepAlive:            true,
			KeepAlivePeriod:      30 * time.Second,
			MaxQueueDepth:        10000,
			EnableDeadLetter:     true,
			DeadLetterMaxRetries: 3,
			EnableMetrics:        true,
			MetricsInterval:      1 * time.Minute,
			GracefulShutdown:     30 * time.Second,
			MessageTTL:           24 * time.Hour,
			Headers:              make(map[string]string),
		},
		Consumer: ConsumerConfig{
			MaxRetries:              5,
			InitialDelay:            2 * time.Second,
			MaxBackoff:              20 * time.Second,
			JitterPercent:           0.5,
			EnableReconnect:         true,
			ReconnectInterval:       5 * time.Second,
			HealthCheckInterval:     30 * time.Second,
			MaxConcurrentTasks:      100,
			TaskTimeout:             30 * time.Second,
			EnableDeduplication:     true,
			DeduplicationWindow:     5 * time.Minute,
			EnablePriorityQueue:     true,
			EnableHTTPAPI:           true,
			HTTPAPIPort:             0, // Random port
			EnableCircuitBreaker:    true,
			CircuitBreakerThreshold: 10,
			CircuitBreakerTimeout:   30 * time.Second,
		},
		Publisher: PublisherConfig{
			MaxRetries:            5,
			InitialDelay:          2 * time.Second,
			MaxBackoff:            20 * time.Second,
			JitterPercent:         0.5,
			ConnectionPoolSize:    10,
			PublishTimeout:        10 * time.Second,
			EnableBatching:        false,
			BatchSize:             100,
			BatchTimeout:          1 * time.Second,
			EnableCompression:     false,
			CompressionLevel:      6,
			EnableAsync:           false,
			AsyncBufferSize:       1000,
			EnableOrderedDelivery: false,
		},
		Pool: PoolConfig{
			MinWorkers:               1,
			MaxWorkers:               100,
			QueueSize:                1000,
			MaxMemoryLoad:            1024 * 1024 * 1024, // 1GB
			TaskTimeout:              30 * time.Second,
			IdleWorkerTimeout:        5 * time.Minute,
			EnableDynamicScaling:     true,
			ScalingFactor:            1.5,
			ScalingInterval:          1 * time.Minute,
			MaxQueueWaitTime:         10 * time.Second,
			EnableWorkStealing:       false,
			EnablePriorityScheduling: true,
			GracefulShutdownTimeout:  30 * time.Second,
		},
		Security: SecurityConfig{
			EnableTLS:             false,
			TLSCertPath:           "",
			TLSKeyPath:            "",
			TLSCAPath:             "",
			TLSInsecureSkipVerify: false,
			EnableAuthentication:  false,
			AuthenticationMethod:  "basic",
			EnableAuthorization:   false,
			EnableEncryption:      false,
			EncryptionKey:         "",
			EnableAuditLog:        false,
			AuditLogPath:          "/var/log/mq/audit.log",
			SessionTimeout:        30 * time.Minute,
			MaxLoginAttempts:      3,
			LockoutDuration:       15 * time.Minute,
		},
		Monitoring: MonitoringConfig{
			EnableMetrics:       true,
			MetricsPort:         9090,
			MetricsPath:         "/metrics",
			EnableHealthCheck:   true,
			HealthCheckPort:     8081,
			HealthCheckPath:     "/health",
			HealthCheckInterval: 30 * time.Second,
			EnableTracing:       false,
			TracingEndpoint:     "",
			TracingSampleRate:   0.1,
			EnableLogging:       true,
			LogLevel:            "info",
			LogFormat:           "json",
			LogOutput:           "stdout",
			LogFilePath:         "/var/log/mq/app.log",
			LogMaxSize:          100, // MB
			LogMaxBackups:       10,
			LogMaxAge:           30, // days
			EnableProfiling:     false,
			ProfilingPort:       6060,
		},
		Persistence: PersistenceConfig{
			EnablePersistence:  false,
			StorageType:        "memory",
			ConnectionString:   "",
			MaxConnections:     10,
			ConnectionTimeout:  10 * time.Second,
			RetentionPeriod:    7 * 24 * time.Hour, // 7 days
			CleanupInterval:    1 * time.Hour,
			BackupEnabled:      false,
			BackupInterval:     6 * time.Hour,
			BackupPath:         "/var/backup/mq",
			CompressionEnabled: true,
			EncryptionEnabled:  false,
			ReplicationEnabled: false,
			ReplicationNodes:   []string{},
		},
		Clustering: ClusteringConfig{
			EnableClustering:      false,
			NodeID:                "",
			ClusterNodes:          []string{},
			DiscoveryMethod:       "static",
			DiscoveryEndpoint:     "",
			HeartbeatInterval:     5 * time.Second,
			ElectionTimeout:       15 * time.Second,
			EnableLoadBalancing:   false,
			LoadBalancingStrategy: "round_robin",
			EnableFailover:        false,
			FailoverTimeout:       30 * time.Second,
			EnableReplication:     false,
			ReplicationFactor:     3,
			ConsistencyLevel:      "strong",
		},
		RateLimit: RateLimitConfig{
			EnableBrokerRateLimit:    false,
			BrokerRate:               1000,
			BrokerBurst:              100,
			EnableConsumerRateLimit:  false,
			ConsumerRate:             100,
			ConsumerBurst:            10,
			EnablePublisherRateLimit: false,
			PublisherRate:            100,
			PublisherBurst:           10,
			EnablePerQueueRateLimit:  false,
			PerQueueRate:             50,
			PerQueueBurst:            5,
		},
		LastUpdated: time.Now(),
	}
}

// LoadConfig loads configuration from file
func (cm *ConfigManager) LoadConfig() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.configFile == "" {
		cm.logger.Info("No config file specified, using defaults")
		return nil
	}

	data, err := os.ReadFile(cm.configFile)
	if err != nil {
		if os.IsNotExist(err) {
			cm.logger.Info("Config file not found, creating with defaults",
				logger.Field{Key: "file", Value: cm.configFile})
			return cm.saveConfigLocked()
		}
		return fmt.Errorf("failed to read config file: %w", err)
	}

	oldConfig := *cm.config
	if err := json.Unmarshal(data, cm.config); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	cm.config.LastUpdated = time.Now()

	// Notify watchers
	for _, watcher := range cm.watchers {
		if err := watcher.OnConfigChange(&oldConfig, cm.config); err != nil {
			cm.logger.Error("Config watcher error",
				logger.Field{Key: "error", Value: err.Error()})
		}
	}

	cm.logger.Info("Configuration loaded successfully",
		logger.Field{Key: "file", Value: cm.configFile})

	return nil
}

// SaveConfig saves current configuration to file
func (cm *ConfigManager) SaveConfig() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.saveConfigLocked()
}

func (cm *ConfigManager) saveConfigLocked() error {
	if cm.configFile == "" {
		return fmt.Errorf("no config file specified")
	}

	cm.config.LastUpdated = time.Now()

	data, err := json.MarshalIndent(cm.config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(cm.configFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	cm.logger.Info("Configuration saved successfully",
		logger.Field{Key: "file", Value: cm.configFile})

	return nil
}

// GetConfig returns a copy of the current configuration
func (cm *ConfigManager) GetConfig() *ProductionConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Return a copy to prevent external modification
	configCopy := *cm.config
	return &configCopy
}

// UpdateConfig updates the configuration
func (cm *ConfigManager) UpdateConfig(newConfig *ProductionConfig) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	oldConfig := *cm.config

	// Validate configuration
	if err := cm.validateConfig(newConfig); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	cm.config = newConfig
	cm.config.LastUpdated = time.Now()

	// Notify watchers
	for _, watcher := range cm.watchers {
		if err := watcher.OnConfigChange(&oldConfig, cm.config); err != nil {
			cm.logger.Error("Config watcher error",
				logger.Field{Key: "error", Value: err.Error()})
		}
	}

	// Auto-save if file is specified
	if cm.configFile != "" {
		if err := cm.saveConfigLocked(); err != nil {
			cm.logger.Error("Failed to auto-save configuration",
				logger.Field{Key: "error", Value: err.Error()})
		}
	}

	cm.logger.Info("Configuration updated successfully")

	return nil
}

// AddWatcher adds a configuration watcher
func (cm *ConfigManager) AddWatcher(watcher ConfigWatcher) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.watchers = append(cm.watchers, watcher)
}

// RemoveWatcher removes a configuration watcher
func (cm *ConfigManager) RemoveWatcher(watcher ConfigWatcher) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for i, w := range cm.watchers {
		if w == watcher {
			cm.watchers = append(cm.watchers[:i], cm.watchers[i+1:]...)
			break
		}
	}
}

// validateConfig validates the configuration
func (cm *ConfigManager) validateConfig(config *ProductionConfig) error {
	// Validate broker config
	if config.Broker.Port <= 0 || config.Broker.Port > 65535 {
		return fmt.Errorf("invalid broker port: %d", config.Broker.Port)
	}

	if config.Broker.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive")
	}

	// Validate consumer config
	if config.Consumer.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}

	if config.Consumer.JitterPercent < 0 || config.Consumer.JitterPercent > 1 {
		return fmt.Errorf("jitter percent must be between 0 and 1")
	}

	// Validate publisher config
	if config.Publisher.ConnectionPoolSize <= 0 {
		return fmt.Errorf("connection pool size must be positive")
	}

	// Validate pool config
	if config.Pool.MinWorkers <= 0 {
		return fmt.Errorf("min workers must be positive")
	}

	if config.Pool.MaxWorkers < config.Pool.MinWorkers {
		return fmt.Errorf("max workers must be >= min workers")
	}

	if config.Pool.QueueSize <= 0 {
		return fmt.Errorf("queue size must be positive")
	}

	// Validate security config
	if config.Security.EnableTLS {
		if config.Security.TLSCertPath == "" || config.Security.TLSKeyPath == "" {
			return fmt.Errorf("TLS cert and key paths required when TLS is enabled")
		}
	}

	// Validate monitoring config
	if config.Monitoring.EnableMetrics {
		if config.Monitoring.MetricsPort <= 0 || config.Monitoring.MetricsPort > 65535 {
			return fmt.Errorf("invalid metrics port: %d", config.Monitoring.MetricsPort)
		}
	}

	// Validate clustering config
	if config.Clustering.EnableClustering {
		if config.Clustering.NodeID == "" {
			return fmt.Errorf("node ID required when clustering is enabled")
		}
	}

	return nil
}

// StartWatching starts watching for configuration changes
func (cm *ConfigManager) StartWatching(ctx context.Context, interval time.Duration) {
	if cm.configFile == "" {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastModTime time.Time
	if stat, err := os.Stat(cm.configFile); err == nil {
		lastModTime = stat.ModTime()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stat, err := os.Stat(cm.configFile)
			if err != nil {
				continue
			}

			if stat.ModTime().After(lastModTime) {
				lastModTime = stat.ModTime()
				if err := cm.LoadConfig(); err != nil {
					cm.logger.Error("Failed to reload configuration",
						logger.Field{Key: "error", Value: err.Error()})
				} else {
					cm.logger.Info("Configuration reloaded from file")
				}
			}
		}
	}
}
