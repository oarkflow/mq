package dag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
	"golang.org/x/time/rate"
)

// RateLimiter provides rate limiting for DAG operations
type RateLimiter struct {
	limiters map[string]*rate.Limiter
	mu       sync.RWMutex
	logger   logger.Logger
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(logger logger.Logger) *RateLimiter {
	return &RateLimiter{
		limiters: make(map[string]*rate.Limiter),
		logger:   logger,
	}
}

// SetNodeLimit sets rate limit for a specific node
func (rl *RateLimiter) SetNodeLimit(nodeID string, requestsPerSecond float64, burst int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.limiters[nodeID] = rate.NewLimiter(rate.Limit(requestsPerSecond), burst)
	rl.logger.Info("Rate limit set for node",
		logger.Field{Key: "nodeID", Value: nodeID},
		logger.Field{Key: "requestsPerSecond", Value: requestsPerSecond},
		logger.Field{Key: "burst", Value: burst},
	)
}

// Allow checks if the request is allowed for the given node
func (rl *RateLimiter) Allow(nodeID string) bool {
	rl.mu.RLock()
	limiter, exists := rl.limiters[nodeID]
	rl.mu.RUnlock()

	if !exists {
		return true // No limit set
	}

	return limiter.Allow()
}

// Wait waits until the request can be processed for the given node
func (rl *RateLimiter) Wait(ctx context.Context, nodeID string) error {
	rl.mu.RLock()
	limiter, exists := rl.limiters[nodeID]
	rl.mu.RUnlock()

	if !exists {
		return nil // No limit set
	}

	return limiter.Wait(ctx)
}

// DAGCache provides caching capabilities for DAG operations
type DAGCache struct {
	nodeCache    map[string]*CacheEntry
	resultCache  map[string]*CacheEntry
	mu           sync.RWMutex
	ttl          time.Duration
	maxSize      int
	logger       logger.Logger
	cleanupTimer *time.Timer
}

// CacheEntry represents a cached item
type CacheEntry struct {
	Value       interface{}
	ExpiresAt   time.Time
	AccessCount int64
	LastAccess  time.Time
}

// NewDAGCache creates a new DAG cache
func NewDAGCache(ttl time.Duration, maxSize int, logger logger.Logger) *DAGCache {
	cache := &DAGCache{
		nodeCache:   make(map[string]*CacheEntry),
		resultCache: make(map[string]*CacheEntry),
		ttl:         ttl,
		maxSize:     maxSize,
		logger:      logger,
	}

	// Start cleanup routine
	cache.startCleanup()

	return cache
}

// GetNodeResult retrieves a cached node result
func (dc *DAGCache) GetNodeResult(key string) (interface{}, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	entry, exists := dc.resultCache[key]
	if !exists || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	entry.AccessCount++
	entry.LastAccess = time.Now()

	return entry.Value, true
}

// SetNodeResult caches a node result
func (dc *DAGCache) SetNodeResult(key string, value interface{}) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Check if we need to evict entries
	if len(dc.resultCache) >= dc.maxSize {
		dc.evictLRU()
	}

	dc.resultCache[key] = &CacheEntry{
		Value:       value,
		ExpiresAt:   time.Now().Add(dc.ttl),
		AccessCount: 1,
		LastAccess:  time.Now(),
	}
}

// GetNode retrieves a cached node
func (dc *DAGCache) GetNode(key string) (*Node, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	entry, exists := dc.nodeCache[key]
	if !exists || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	entry.AccessCount++
	entry.LastAccess = time.Now()

	if node, ok := entry.Value.(*Node); ok {
		return node, true
	}

	return nil, false
}

// SetNode caches a node
func (dc *DAGCache) SetNode(key string, node *Node) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if len(dc.nodeCache) >= dc.maxSize {
		dc.evictLRU()
	}

	dc.nodeCache[key] = &CacheEntry{
		Value:       node,
		ExpiresAt:   time.Now().Add(dc.ttl),
		AccessCount: 1,
		LastAccess:  time.Now(),
	}
}

// evictLRU evicts the least recently used entry
func (dc *DAGCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time

	// Check result cache
	for key, entry := range dc.resultCache {
		if oldestKey == "" || entry.LastAccess.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.LastAccess
		}
	}

	// Check node cache
	for key, entry := range dc.nodeCache {
		if oldestKey == "" || entry.LastAccess.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.LastAccess
		}
	}

	if oldestKey != "" {
		delete(dc.resultCache, oldestKey)
		delete(dc.nodeCache, oldestKey)
	}
}

// startCleanup starts the background cleanup routine
func (dc *DAGCache) startCleanup() {
	dc.cleanupTimer = time.AfterFunc(dc.ttl, func() {
		dc.cleanup()
		dc.startCleanup() // Reschedule
	})
}

// cleanup removes expired entries
func (dc *DAGCache) cleanup() {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()

	// Clean result cache
	for key, entry := range dc.resultCache {
		if now.After(entry.ExpiresAt) {
			delete(dc.resultCache, key)
		}
	}

	// Clean node cache
	for key, entry := range dc.nodeCache {
		if now.After(entry.ExpiresAt) {
			delete(dc.nodeCache, key)
		}
	}
}

// Stop stops the cache cleanup routine
func (dc *DAGCache) Stop() {
	if dc.cleanupTimer != nil {
		dc.cleanupTimer.Stop()
	}
}

// ConfigManager handles dynamic DAG configuration
type ConfigManager struct {
	config   *DAGConfig
	mu       sync.RWMutex
	watchers []ConfigWatcher
	logger   logger.Logger
}

// DAGConfig holds dynamic configuration for DAG
type DAGConfig struct {
	MaxConcurrentTasks     int              `json:"max_concurrent_tasks"`
	TaskTimeout            time.Duration    `json:"task_timeout"`
	NodeTimeout            time.Duration    `json:"node_timeout"`
	RetryConfig            *RetryConfig     `json:"retry_config"`
	CacheConfig            *CacheConfig     `json:"cache_config"`
	RateLimitConfig        *RateLimitConfig `json:"rate_limit_config"`
	MonitoringEnabled      bool             `json:"monitoring_enabled"`
	AlertingEnabled        bool             `json:"alerting_enabled"`
	CleanupInterval        time.Duration    `json:"cleanup_interval"`
	TransactionTimeout     time.Duration    `json:"transaction_timeout"`
	BatchProcessingEnabled bool             `json:"batch_processing_enabled"`
	BatchSize              int              `json:"batch_size"`
	BatchTimeout           time.Duration    `json:"batch_timeout"`
}

// CacheConfig holds cache configuration
type CacheConfig struct {
	Enabled bool          `json:"enabled"`
	TTL     time.Duration `json:"ttl"`
	MaxSize int           `json:"max_size"`
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	Enabled     bool                     `json:"enabled"`
	GlobalLimit float64                  `json:"global_limit"`
	GlobalBurst int                      `json:"global_burst"`
	NodeLimits  map[string]NodeRateLimit `json:"node_limits"`
}

// NodeRateLimit holds rate limit settings for a specific node
type NodeRateLimit struct {
	RequestsPerSecond float64 `json:"requests_per_second"`
	Burst             int     `json:"burst"`
}

// ConfigWatcher interface for configuration change notifications
type ConfigWatcher interface {
	OnConfigChange(oldConfig, newConfig *DAGConfig) error
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(logger logger.Logger) *ConfigManager {
	return &ConfigManager{
		config:   DefaultDAGConfig(),
		watchers: make([]ConfigWatcher, 0),
		logger:   logger,
	}
}

// DefaultDAGConfig returns default DAG configuration
func DefaultDAGConfig() *DAGConfig {
	return &DAGConfig{
		MaxConcurrentTasks: 100,
		TaskTimeout:        30 * time.Second,
		NodeTimeout:        30 * time.Second,
		RetryConfig:        DefaultRetryConfig(),
		CacheConfig: &CacheConfig{
			Enabled: true,
			TTL:     5 * time.Minute,
			MaxSize: 1000,
		},
		RateLimitConfig: &RateLimitConfig{
			Enabled:     false,
			GlobalLimit: 100,
			GlobalBurst: 10,
			NodeLimits:  make(map[string]NodeRateLimit),
		},
		MonitoringEnabled:      true,
		AlertingEnabled:        true,
		CleanupInterval:        10 * time.Minute,
		TransactionTimeout:     5 * time.Minute,
		BatchProcessingEnabled: false,
		BatchSize:              50,
		BatchTimeout:           5 * time.Second,
	}
}

// GetConfig returns a copy of the current configuration
func (cm *ConfigManager) GetConfig() *DAGConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Return a copy to prevent external modification
	return cm.copyConfig(cm.config)
}

// UpdateConfig updates the configuration
func (cm *ConfigManager) UpdateConfig(newConfig *DAGConfig) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	oldConfig := cm.copyConfig(cm.config)

	// Validate configuration
	if err := cm.validateConfig(newConfig); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	cm.config = newConfig

	// Notify watchers
	for _, watcher := range cm.watchers {
		if err := watcher.OnConfigChange(oldConfig, newConfig); err != nil {
			cm.logger.Error("Config watcher error",
				logger.Field{Key: "error", Value: err.Error()},
			)
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

// validateConfig validates the configuration
func (cm *ConfigManager) validateConfig(config *DAGConfig) error {
	if config.MaxConcurrentTasks <= 0 {
		return fmt.Errorf("max concurrent tasks must be positive")
	}

	if config.TaskTimeout <= 0 {
		return fmt.Errorf("task timeout must be positive")
	}

	if config.NodeTimeout <= 0 {
		return fmt.Errorf("node timeout must be positive")
	}

	if config.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}

	if config.BatchTimeout <= 0 {
		return fmt.Errorf("batch timeout must be positive")
	}

	return nil
}

// copyConfig creates a deep copy of the configuration
func (cm *ConfigManager) copyConfig(config *DAGConfig) *DAGConfig {
	copy := *config

	if config.RetryConfig != nil {
		retryCopy := *config.RetryConfig
		copy.RetryConfig = &retryCopy
	}

	if config.CacheConfig != nil {
		cacheCopy := *config.CacheConfig
		copy.CacheConfig = &cacheCopy
	}

	if config.RateLimitConfig != nil {
		rateLimitCopy := *config.RateLimitConfig
		rateLimitCopy.NodeLimits = make(map[string]NodeRateLimit)
		for k, v := range config.RateLimitConfig.NodeLimits {
			rateLimitCopy.NodeLimits[k] = v
		}
		copy.RateLimitConfig = &rateLimitCopy
	}

	return &copy
}

// PerformanceOptimizer optimizes DAG performance based on metrics
type PerformanceOptimizer struct {
	dag     *DAG
	monitor *Monitor
	config  *ConfigManager
	logger  logger.Logger
}

// NewPerformanceOptimizer creates a new performance optimizer
func NewPerformanceOptimizer(dag *DAG, monitor *Monitor, config *ConfigManager, logger logger.Logger) *PerformanceOptimizer {
	return &PerformanceOptimizer{
		dag:     dag,
		monitor: monitor,
		config:  config,
		logger:  logger,
	}
}

// OptimizePerformance analyzes metrics and adjusts configuration
func (po *PerformanceOptimizer) OptimizePerformance() error {
	metrics := po.monitor.GetMetrics()
	currentConfig := po.config.GetConfig()

	newConfig := po.config.copyConfig(currentConfig)
	changed := false

	// Optimize based on task completion rate
	if metrics.TasksInProgress > int64(currentConfig.MaxConcurrentTasks*80/100) {
		// Increase concurrent tasks if we're at 80% capacity
		newConfig.MaxConcurrentTasks = int(float64(currentConfig.MaxConcurrentTasks) * 1.2)
		changed = true

		po.logger.Info("Increasing max concurrent tasks",
			logger.Field{Key: "from", Value: currentConfig.MaxConcurrentTasks},
			logger.Field{Key: "to", Value: newConfig.MaxConcurrentTasks},
		)
	}

	// Optimize timeout based on average execution time
	if metrics.AverageExecutionTime > currentConfig.TaskTimeout {
		// Increase timeout if average execution time is higher
		newConfig.TaskTimeout = time.Duration(float64(metrics.AverageExecutionTime) * 1.5)
		changed = true

		po.logger.Info("Increasing task timeout",
			logger.Field{Key: "from", Value: currentConfig.TaskTimeout},
			logger.Field{Key: "to", Value: newConfig.TaskTimeout},
		)
	}

	// Apply changes if any
	if changed {
		return po.config.UpdateConfig(newConfig)
	}

	return nil
}
