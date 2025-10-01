package mq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
	"golang.org/x/time/rate"
)

// DedupEntry represents a deduplication cache entry
type DedupEntry struct {
	MessageID   string
	ContentHash string
	FirstSeen   time.Time
	LastSeen    time.Time
	Count       int
}

// DeduplicationManager manages message deduplication
type DeduplicationManager struct {
	cache           map[string]*DedupEntry
	mu              sync.RWMutex
	window          time.Duration
	cleanupInterval time.Duration
	shutdown        chan struct{}
	logger          logger.Logger
	persistent      DedupStorage
	onDuplicate     func(*DedupEntry)
}

// DedupStorage interface for persistent deduplication storage
type DedupStorage interface {
	Store(ctx context.Context, entry *DedupEntry) error
	Get(ctx context.Context, key string) (*DedupEntry, error)
	Delete(ctx context.Context, key string) error
	DeleteOlderThan(ctx context.Context, duration time.Duration) (int, error)
	Close() error
}

// DedupConfig holds configuration for deduplication
type DedupConfig struct {
	Window          time.Duration // Time window for deduplication
	CleanupInterval time.Duration
	Persistent      DedupStorage // Optional persistent storage
	Logger          logger.Logger
}

// NewDeduplicationManager creates a new deduplication manager
func NewDeduplicationManager(config DedupConfig) *DeduplicationManager {
	if config.Window == 0 {
		config.Window = 5 * time.Minute
	}
	if config.CleanupInterval == 0 {
		config.CleanupInterval = 1 * time.Minute
	}

	dm := &DeduplicationManager{
		cache:           make(map[string]*DedupEntry),
		window:          config.Window,
		cleanupInterval: config.CleanupInterval,
		shutdown:        make(chan struct{}),
		logger:          config.Logger,
		persistent:      config.Persistent,
	}

	go dm.cleanupLoop()

	return dm
}

// CheckDuplicate checks if a message is a duplicate
func (dm *DeduplicationManager) CheckDuplicate(ctx context.Context, task *Task) (bool, error) {
	// Generate dedup key from task
	dedupKey := dm.generateDedupKey(task)

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check in-memory cache
	if entry, exists := dm.cache[dedupKey]; exists {
		// Check if within window
		if time.Since(entry.FirstSeen) < dm.window {
			entry.LastSeen = time.Now()
			entry.Count++

			if dm.onDuplicate != nil {
				go dm.onDuplicate(entry)
			}

			dm.logger.Debug("Duplicate message detected",
				logger.Field{Key: "dedupKey", Value: dedupKey},
				logger.Field{Key: "count", Value: entry.Count},
				logger.Field{Key: "taskID", Value: task.ID})

			return true, nil
		}

		// Entry expired, remove it
		delete(dm.cache, dedupKey)
	}

	// Check persistent storage if available
	if dm.persistent != nil {
		entry, err := dm.persistent.Get(ctx, dedupKey)
		if err == nil && time.Since(entry.FirstSeen) < dm.window {
			entry.LastSeen = time.Now()
			entry.Count++
			dm.cache[dedupKey] = entry

			if dm.onDuplicate != nil {
				go dm.onDuplicate(entry)
			}

			return true, nil
		}
	}

	// Not a duplicate, add to cache
	entry := &DedupEntry{
		MessageID:   task.ID,
		ContentHash: dedupKey,
		FirstSeen:   time.Now(),
		LastSeen:    time.Now(),
		Count:       1,
	}

	dm.cache[dedupKey] = entry

	// Persist if storage available
	if dm.persistent != nil {
		go dm.persistent.Store(ctx, entry)
	}

	return false, nil
}

// generateDedupKey generates a deduplication key from a task
func (dm *DeduplicationManager) generateDedupKey(task *Task) string {
	// If task has explicit dedup key, use it
	if task.DedupKey != "" {
		return task.DedupKey
	}

	// Otherwise, hash the content
	hasher := sha256.New()
	hasher.Write([]byte(task.Topic))
	hasher.Write(task.Payload)

	// Include headers in hash for more precise deduplication
	if task.Headers != nil {
		headerBytes, _ := json.Marshal(task.Headers)
		hasher.Write(headerBytes)
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

// cleanupLoop periodically cleans up expired entries
func (dm *DeduplicationManager) cleanupLoop() {
	ticker := time.NewTicker(dm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dm.cleanup()
		case <-dm.shutdown:
			return
		}
	}
}

// cleanup removes expired deduplication entries
func (dm *DeduplicationManager) cleanup() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	cutoff := time.Now().Add(-dm.window)
	removed := 0

	for key, entry := range dm.cache {
		if entry.FirstSeen.Before(cutoff) {
			delete(dm.cache, key)
			removed++
		}
	}

	if removed > 0 {
		dm.logger.Debug("Cleaned up expired dedup entries",
			logger.Field{Key: "removed", Value: removed})
	}

	// Cleanup persistent storage
	if dm.persistent != nil {
		go dm.persistent.DeleteOlderThan(context.Background(), dm.window)
	}
}

// SetOnDuplicate sets callback for duplicate detection
func (dm *DeduplicationManager) SetOnDuplicate(fn func(*DedupEntry)) {
	dm.onDuplicate = fn
}

// GetStats returns deduplication statistics
func (dm *DeduplicationManager) GetStats() map[string]interface{} {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	totalDuplicates := 0
	for _, entry := range dm.cache {
		totalDuplicates += entry.Count - 1 // Subtract 1 for original message
	}

	return map[string]interface{}{
		"cache_size":       len(dm.cache),
		"total_duplicates": totalDuplicates,
		"window":           dm.window,
	}
}

// Shutdown stops the deduplication manager
func (dm *DeduplicationManager) Shutdown(ctx context.Context) error {
	close(dm.shutdown)

	if dm.persistent != nil {
		return dm.persistent.Close()
	}

	return nil
}

// TokenBucketStrategy implements token bucket algorithm
type TokenBucketStrategy struct {
	tokens         int64
	capacity       int64
	refillRate     int64
	refillInterval time.Duration
	lastRefill     time.Time
	mu             sync.Mutex
	shutdown       chan struct{}
	logger         logger.Logger
}

// NewTokenBucketStrategy creates a new token bucket strategy
func NewTokenBucketStrategy(config FlowControlConfig) *TokenBucketStrategy {
	if config.MaxCredits == 0 {
		config.MaxCredits = 1000
	}
	if config.RefillRate == 0 {
		config.RefillRate = 10
	}
	if config.RefillInterval == 0 {
		config.RefillInterval = 100 * time.Millisecond
	}
	if config.BurstSize == 0 {
		config.BurstSize = config.MaxCredits
	}

	tbs := &TokenBucketStrategy{
		tokens:         config.BurstSize,
		capacity:       config.BurstSize,
		refillRate:     config.RefillRate,
		refillInterval: config.RefillInterval,
		lastRefill:     time.Now(),
		shutdown:       make(chan struct{}),
		logger:         config.Logger,
	}

	go tbs.refillLoop()

	return tbs
}

// Acquire attempts to acquire tokens
func (tbs *TokenBucketStrategy) Acquire(ctx context.Context, amount int64) error {
	for {
		tbs.mu.Lock()
		if tbs.tokens >= amount {
			tbs.tokens -= amount
			tbs.mu.Unlock()
			return nil
		}
		tbs.mu.Unlock()

		select {
		case <-time.After(10 * time.Millisecond):
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-tbs.shutdown:
			return fmt.Errorf("token bucket shutting down")
		}
	}
}

// Release returns tokens (not typically used in token bucket)
func (tbs *TokenBucketStrategy) Release(amount int64) {
	// Token bucket doesn't typically release tokens back
	// This is a no-op for token bucket strategy
}

// GetAvailableCredits returns available tokens
func (tbs *TokenBucketStrategy) GetAvailableCredits() int64 {
	tbs.mu.Lock()
	defer tbs.mu.Unlock()
	return tbs.tokens
}

// GetStats returns token bucket statistics
func (tbs *TokenBucketStrategy) GetStats() map[string]interface{} {
	tbs.mu.Lock()
	defer tbs.mu.Unlock()

	utilization := float64(tbs.capacity-tbs.tokens) / float64(tbs.capacity) * 100

	return map[string]interface{}{
		"strategy":    "token_bucket",
		"tokens":      tbs.tokens,
		"capacity":    tbs.capacity,
		"refill_rate": tbs.refillRate,
		"utilization": utilization,
		"last_refill": tbs.lastRefill,
	}
}

// Shutdown stops the token bucket
func (tbs *TokenBucketStrategy) Shutdown() {
	close(tbs.shutdown)
}

// refillLoop periodically refills tokens
func (tbs *TokenBucketStrategy) refillLoop() {
	ticker := time.NewTicker(tbs.refillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tbs.mu.Lock()
			tbs.tokens += tbs.refillRate
			if tbs.tokens > tbs.capacity {
				tbs.tokens = tbs.capacity
			}
			tbs.lastRefill = time.Now()
			tbs.mu.Unlock()
		case <-tbs.shutdown:
			return
		}
	}
}

// FlowControlStrategy defines the interface for different flow control algorithms
type FlowControlStrategy interface {
	// Acquire attempts to acquire credits for processing
	Acquire(ctx context.Context, amount int64) error
	// Release returns credits after processing
	Release(amount int64)
	// GetAvailableCredits returns current available credits
	GetAvailableCredits() int64
	// GetStats returns strategy-specific statistics
	GetStats() map[string]interface{}
	// Shutdown cleans up resources
	Shutdown()
}

// FlowControlConfig holds flow control configuration
type FlowControlConfig struct {
	Strategy       FlowControlStrategyType `json:"strategy" yaml:"strategy"`
	MaxCredits     int64                   `json:"max_credits" yaml:"max_credits"`
	MinCredits     int64                   `json:"min_credits" yaml:"min_credits"`
	RefillRate     int64                   `json:"refill_rate" yaml:"refill_rate"`
	RefillInterval time.Duration           `json:"refill_interval" yaml:"refill_interval"`
	BurstSize      int64                   `json:"burst_size" yaml:"burst_size"` // For token bucket
	Logger         logger.Logger           `json:"-" yaml:"-"`
}

// FlowControlStrategyType represents different flow control strategies
type FlowControlStrategyType string

const (
	StrategyTokenBucket FlowControlStrategyType = "token_bucket"
	StrategyLeakyBucket FlowControlStrategyType = "leaky_bucket"
	StrategyCreditBased FlowControlStrategyType = "credit_based"
	StrategyRateLimiter FlowControlStrategyType = "rate_limiter"
)

// FlowController manages backpressure and flow control using pluggable strategies
type FlowController struct {
	strategy     FlowControlStrategy
	config       FlowControlConfig
	onCreditLow  func(current, max int64)
	onCreditHigh func(current, max int64)
	logger       logger.Logger
	shutdown     chan struct{}
}

// FlowControllerFactory creates flow controllers with different strategies
type FlowControllerFactory struct{}

// NewFlowControllerFactory creates a new factory
func NewFlowControllerFactory() *FlowControllerFactory {
	return &FlowControllerFactory{}
}

// CreateFlowController creates a flow controller with the specified strategy
func (f *FlowControllerFactory) CreateFlowController(config FlowControlConfig) (*FlowController, error) {
	if config.Strategy == "" {
		config.Strategy = StrategyTokenBucket
	}

	// Validate configuration based on strategy
	if err := f.validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return NewFlowController(config), nil
}

// CreateTokenBucketFlowController creates a token bucket flow controller
func (f *FlowControllerFactory) CreateTokenBucketFlowController(maxCredits, refillRate int64, refillInterval time.Duration, logger logger.Logger) *FlowController {
	config := FlowControlConfig{
		Strategy:       StrategyTokenBucket,
		MaxCredits:     maxCredits,
		RefillRate:     refillRate,
		RefillInterval: refillInterval,
		BurstSize:      maxCredits,
		Logger:         logger,
	}
	return NewFlowController(config)
}

// CreateLeakyBucketFlowController creates a leaky bucket flow controller
func (f *FlowControllerFactory) CreateLeakyBucketFlowController(capacity int64, leakInterval time.Duration, logger logger.Logger) *FlowController {
	config := FlowControlConfig{
		Strategy:       StrategyLeakyBucket,
		MaxCredits:     capacity,
		RefillInterval: leakInterval,
		Logger:         logger,
	}
	return NewFlowController(config)
}

// CreateCreditBasedFlowController creates a credit-based flow controller
func (f *FlowControllerFactory) CreateCreditBasedFlowController(maxCredits, minCredits, refillRate int64, refillInterval time.Duration, logger logger.Logger) *FlowController {
	config := FlowControlConfig{
		Strategy:       StrategyCreditBased,
		MaxCredits:     maxCredits,
		MinCredits:     minCredits,
		RefillRate:     refillRate,
		RefillInterval: refillInterval,
		Logger:         logger,
	}
	return NewFlowController(config)
}

// CreateRateLimiterFlowController creates a rate limiter flow controller
func (f *FlowControllerFactory) CreateRateLimiterFlowController(requestsPerSecond, burstSize int64, logger logger.Logger) *FlowController {
	config := FlowControlConfig{
		Strategy:   StrategyRateLimiter,
		RefillRate: requestsPerSecond,
		BurstSize:  burstSize,
		Logger:     logger,
	}
	return NewFlowController(config)
}

// validateConfig validates the configuration for the specified strategy
func (f *FlowControllerFactory) validateConfig(config FlowControlConfig) error {
	switch config.Strategy {
	case StrategyTokenBucket:
		if config.MaxCredits <= 0 {
			return fmt.Errorf("max_credits must be positive for token bucket strategy")
		}
		if config.RefillRate <= 0 {
			return fmt.Errorf("refill_rate must be positive for token bucket strategy")
		}
	case StrategyLeakyBucket:
		if config.MaxCredits <= 0 {
			return fmt.Errorf("max_credits must be positive for leaky bucket strategy")
		}
	case StrategyCreditBased:
		if config.MaxCredits <= 0 {
			return fmt.Errorf("max_credits must be positive for credit-based strategy")
		}
		if config.MinCredits < 0 || config.MinCredits > config.MaxCredits {
			return fmt.Errorf("min_credits must be between 0 and max_credits for credit-based strategy")
		}
	case StrategyRateLimiter:
		if config.RefillRate <= 0 {
			return fmt.Errorf("refill_rate must be positive for rate limiter strategy")
		}
		if config.BurstSize <= 0 {
			return fmt.Errorf("burst_size must be positive for rate limiter strategy")
		}
	default:
		return fmt.Errorf("unknown strategy: %s", config.Strategy)
	}
	return nil
}

// NewFlowController creates a new flow controller with the specified strategy
func NewFlowController(config FlowControlConfig) *FlowController {
	if config.Strategy == "" {
		config.Strategy = StrategyTokenBucket
	}

	var strategy FlowControlStrategy
	switch config.Strategy {
	case StrategyTokenBucket:
		strategy = NewTokenBucketStrategy(config)
	case StrategyLeakyBucket:
		strategy = NewLeakyBucketStrategy(config)
	case StrategyCreditBased:
		strategy = NewCreditBasedStrategy(config)
	case StrategyRateLimiter:
		strategy = NewRateLimiterStrategy(config)
	default:
		// Default to token bucket
		strategy = NewTokenBucketStrategy(config)
	}

	fc := &FlowController{
		strategy: strategy,
		config:   config,
		logger:   config.Logger,
		shutdown: make(chan struct{}),
	}

	return fc
}

// LeakyBucketStrategy implements leaky bucket algorithm
type LeakyBucketStrategy struct {
	queue    chan struct{}
	capacity int64
	leakRate time.Duration
	lastLeak time.Time
	mu       sync.Mutex
	shutdown chan struct{}
	logger   logger.Logger
}

// NewLeakyBucketStrategy creates a new leaky bucket strategy
func NewLeakyBucketStrategy(config FlowControlConfig) *LeakyBucketStrategy {
	if config.MaxCredits == 0 {
		config.MaxCredits = 1000
	}
	if config.RefillInterval == 0 {
		config.RefillInterval = 100 * time.Millisecond
	}

	lbs := &LeakyBucketStrategy{
		queue:    make(chan struct{}, config.MaxCredits),
		capacity: config.MaxCredits,
		leakRate: config.RefillInterval,
		lastLeak: time.Now(),
		shutdown: make(chan struct{}),
		logger:   config.Logger,
	}

	go lbs.leakLoop()

	return lbs
}

// Acquire attempts to add to the bucket
func (lbs *LeakyBucketStrategy) Acquire(ctx context.Context, amount int64) error {
	for i := int64(0); i < amount; i++ {
		select {
		case lbs.queue <- struct{}{}:
			// Successfully added
		case <-ctx.Done():
			return ctx.Err()
		case <-lbs.shutdown:
			return fmt.Errorf("leaky bucket shutting down")
		default:
			// Bucket is full, wait and retry
			select {
			case <-time.After(10 * time.Millisecond):
				continue
			case <-ctx.Done():
				return ctx.Err()
			case <-lbs.shutdown:
				return fmt.Errorf("leaky bucket shutting down")
			}
		}
	}
	return nil
}

// Release removes from the bucket (leaking)
func (lbs *LeakyBucketStrategy) Release(amount int64) {
	for i := int64(0); i < amount; i++ {
		select {
		case <-lbs.queue:
		default:
		}
	}
}

// GetAvailableCredits returns available capacity
func (lbs *LeakyBucketStrategy) GetAvailableCredits() int64 {
	return lbs.capacity - int64(len(lbs.queue))
}

// GetStats returns leaky bucket statistics
func (lbs *LeakyBucketStrategy) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"strategy":    "leaky_bucket",
		"queue_size":  len(lbs.queue),
		"capacity":    lbs.capacity,
		"leak_rate":   lbs.leakRate,
		"utilization": float64(len(lbs.queue)) / float64(lbs.capacity) * 100,
	}
}

// Shutdown stops the leaky bucket
func (lbs *LeakyBucketStrategy) Shutdown() {
	close(lbs.shutdown)
}

// leakLoop periodically leaks from the bucket
func (lbs *LeakyBucketStrategy) leakLoop() {
	ticker := time.NewTicker(lbs.leakRate)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case <-lbs.queue:
				// Leaked one
			default:
				// Empty
			}
		case <-lbs.shutdown:
			return
		}
	}
}

// CreditBasedStrategy implements credit-based flow control
type CreditBasedStrategy struct {
	credits        int64
	maxCredits     int64
	minCredits     int64
	refillRate     int64
	refillInterval time.Duration
	mu             sync.Mutex
	shutdown       chan struct{}
	logger         logger.Logger
	onCreditLow    func(current, max int64)
	onCreditHigh   func(current, max int64)
}

// NewCreditBasedStrategy creates a new credit-based strategy
func NewCreditBasedStrategy(config FlowControlConfig) *CreditBasedStrategy {
	if config.MaxCredits == 0 {
		config.MaxCredits = 1000
	}
	if config.MinCredits == 0 {
		config.MinCredits = 100
	}
	if config.RefillRate == 0 {
		config.RefillRate = 10
	}
	if config.RefillInterval == 0 {
		config.RefillInterval = 100 * time.Millisecond
	}

	cbs := &CreditBasedStrategy{
		credits:        config.MaxCredits,
		maxCredits:     config.MaxCredits,
		minCredits:     config.MinCredits,
		refillRate:     config.RefillRate,
		refillInterval: config.RefillInterval,
		shutdown:       make(chan struct{}),
		logger:         config.Logger,
	}

	go cbs.refillLoop()

	return cbs
}

// Acquire attempts to acquire credits
func (cbs *CreditBasedStrategy) Acquire(ctx context.Context, amount int64) error {
	for {
		cbs.mu.Lock()
		if cbs.credits >= amount {
			cbs.credits -= amount

			if cbs.credits < cbs.minCredits && cbs.onCreditLow != nil {
				go cbs.onCreditLow(cbs.credits, cbs.maxCredits)
			}

			cbs.mu.Unlock()
			return nil
		}
		cbs.mu.Unlock()

		select {
		case <-time.After(10 * time.Millisecond):
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-cbs.shutdown:
			return fmt.Errorf("credit-based strategy shutting down")
		}
	}
}

// Release returns credits
func (cbs *CreditBasedStrategy) Release(amount int64) {
	cbs.mu.Lock()
	defer cbs.mu.Unlock()

	cbs.credits += amount
	if cbs.credits > cbs.maxCredits {
		cbs.credits = cbs.maxCredits
	}

	if cbs.credits > cbs.maxCredits/2 && cbs.onCreditHigh != nil {
		go cbs.onCreditHigh(cbs.credits, cbs.maxCredits)
	}
}

// GetAvailableCredits returns available credits
func (cbs *CreditBasedStrategy) GetAvailableCredits() int64 {
	cbs.mu.Lock()
	defer cbs.mu.Unlock()
	return cbs.credits
}

// GetStats returns credit-based statistics
func (cbs *CreditBasedStrategy) GetStats() map[string]interface{} {
	cbs.mu.Lock()
	defer cbs.mu.Unlock()

	utilization := float64(cbs.maxCredits-cbs.credits) / float64(cbs.maxCredits) * 100

	return map[string]interface{}{
		"strategy":    "credit_based",
		"credits":     cbs.credits,
		"max_credits": cbs.maxCredits,
		"min_credits": cbs.minCredits,
		"refill_rate": cbs.refillRate,
		"utilization": utilization,
	}
}

// Shutdown stops the credit-based strategy
func (cbs *CreditBasedStrategy) Shutdown() {
	close(cbs.shutdown)
}

// refillLoop periodically refills credits
func (cbs *CreditBasedStrategy) refillLoop() {
	ticker := time.NewTicker(cbs.refillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cbs.mu.Lock()
			cbs.credits += cbs.refillRate
			if cbs.credits > cbs.maxCredits {
				cbs.credits = cbs.maxCredits
			}
			cbs.mu.Unlock()
		case <-cbs.shutdown:
			return
		}
	}
}

// RateLimiterStrategy implements rate limiting using golang.org/x/time/rate
type RateLimiterStrategy struct {
	limiter  *rate.Limiter
	shutdown chan struct{}
	logger   logger.Logger
}

// NewRateLimiterStrategy creates a new rate limiter strategy
func NewRateLimiterStrategy(config FlowControlConfig) *RateLimiterStrategy {
	if config.RefillRate == 0 {
		config.RefillRate = 10
	}
	if config.BurstSize == 0 {
		config.BurstSize = 100
	}

	// Convert refill rate to requests per second
	rps := rate.Limit(config.RefillRate) / rate.Limit(time.Second/time.Millisecond*100)

	rls := &RateLimiterStrategy{
		limiter:  rate.NewLimiter(rps, int(config.BurstSize)),
		shutdown: make(chan struct{}),
		logger:   config.Logger,
	}

	return rls
}

// Acquire attempts to acquire permission
func (rls *RateLimiterStrategy) Acquire(ctx context.Context, amount int64) error {
	// For rate limiter, amount represents the number of requests
	for i := int64(0); i < amount; i++ {
		if err := rls.limiter.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Release is a no-op for rate limiter
func (rls *RateLimiterStrategy) Release(amount int64) {
	// Rate limiter doesn't release tokens back
}

// GetAvailableCredits returns burst capacity minus tokens used
func (rls *RateLimiterStrategy) GetAvailableCredits() int64 {
	// This is approximate since rate.Limiter doesn't expose internal state
	return int64(rls.limiter.Burst()) - int64(rls.limiter.Tokens())
}

// GetStats returns rate limiter statistics
func (rls *RateLimiterStrategy) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"strategy": "rate_limiter",
		"limit":    rls.limiter.Limit(),
		"burst":    rls.limiter.Burst(),
		"tokens":   rls.limiter.Tokens(),
	}
}

// Shutdown stops the rate limiter
func (rls *RateLimiterStrategy) Shutdown() {
	close(rls.shutdown)
}

// AcquireCredit attempts to acquire credits for processing
func (fc *FlowController) AcquireCredit(ctx context.Context, amount int64) error {
	return fc.strategy.Acquire(ctx, amount)
}

// ReleaseCredit returns credits after processing
func (fc *FlowController) ReleaseCredit(amount int64) {
	fc.strategy.Release(amount)
}

// GetAvailableCredits returns the current available credits
func (fc *FlowController) GetAvailableCredits() int64 {
	return fc.strategy.GetAvailableCredits()
}

// SetOnCreditLow sets callback for low credit warning
func (fc *FlowController) SetOnCreditLow(fn func(current, max int64)) {
	fc.onCreditLow = fn
	// If strategy supports callbacks, set them
	if cbs, ok := fc.strategy.(*CreditBasedStrategy); ok {
		cbs.onCreditLow = fn
	}
}

// SetOnCreditHigh sets callback for credit recovery
func (fc *FlowController) SetOnCreditHigh(fn func(current, max int64)) {
	fc.onCreditHigh = fn
	// If strategy supports callbacks, set them
	if cbs, ok := fc.strategy.(*CreditBasedStrategy); ok {
		cbs.onCreditHigh = fn
	}
}

// AdjustMaxCredits dynamically adjusts maximum credits
func (fc *FlowController) AdjustMaxCredits(newMax int64) {
	fc.config.MaxCredits = newMax
	fc.logger.Info("Adjusted max credits",
		logger.Field{Key: "newMax", Value: newMax})
}

// GetStats returns flow control statistics
func (fc *FlowController) GetStats() map[string]interface{} {
	stats := fc.strategy.GetStats()
	stats["config"] = map[string]interface{}{
		"strategy":        fc.config.Strategy,
		"max_credits":     fc.config.MaxCredits,
		"min_credits":     fc.config.MinCredits,
		"refill_rate":     fc.config.RefillRate,
		"refill_interval": fc.config.RefillInterval,
		"burst_size":      fc.config.BurstSize,
	}
	return stats
}

// Shutdown stops the flow controller
func (fc *FlowController) Shutdown() {
	close(fc.shutdown)
}

// BackpressureMonitor monitors system backpressure
type BackpressureMonitor struct {
	queueDepthThreshold    int
	memoryThreshold        uint64
	errorRateThreshold     float64
	checkInterval          time.Duration
	logger                 logger.Logger
	shutdown               chan struct{}
	onBackpressureApplied  func(reason string)
	onBackpressureRelieved func()
}

// BackpressureConfig holds backpressure configuration
type BackpressureConfig struct {
	QueueDepthThreshold int
	MemoryThreshold     uint64
	ErrorRateThreshold  float64
	CheckInterval       time.Duration
	Logger              logger.Logger
}

// NewBackpressureMonitor creates a new backpressure monitor
func NewBackpressureMonitor(config BackpressureConfig) *BackpressureMonitor {
	if config.CheckInterval == 0 {
		config.CheckInterval = 5 * time.Second
	}
	if config.ErrorRateThreshold == 0 {
		config.ErrorRateThreshold = 0.5 // 50% error rate
	}

	bm := &BackpressureMonitor{
		queueDepthThreshold: config.QueueDepthThreshold,
		memoryThreshold:     config.MemoryThreshold,
		errorRateThreshold:  config.ErrorRateThreshold,
		checkInterval:       config.CheckInterval,
		logger:              config.Logger,
		shutdown:            make(chan struct{}),
	}

	go bm.monitorLoop()

	return bm
}

// monitorLoop continuously monitors for backpressure conditions
func (bm *BackpressureMonitor) monitorLoop() {
	ticker := time.NewTicker(bm.checkInterval)
	defer ticker.Stop()

	backpressureActive := false

	for {
		select {
		case <-ticker.C:
			shouldApply, reason := bm.shouldApplyBackpressure()

			if shouldApply && !backpressureActive {
				backpressureActive = true
				bm.logger.Warn("Applying backpressure",
					logger.Field{Key: "reason", Value: reason})
				if bm.onBackpressureApplied != nil {
					bm.onBackpressureApplied(reason)
				}
			} else if !shouldApply && backpressureActive {
				backpressureActive = false
				bm.logger.Info("Relieving backpressure")
				if bm.onBackpressureRelieved != nil {
					bm.onBackpressureRelieved()
				}
			}
		case <-bm.shutdown:
			return
		}
	}
}

// shouldApplyBackpressure checks if backpressure should be applied
func (bm *BackpressureMonitor) shouldApplyBackpressure() (bool, string) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Check memory threshold
	if bm.memoryThreshold > 0 && memStats.Alloc > bm.memoryThreshold {
		return true, fmt.Sprintf("memory threshold exceeded: %d > %d",
			memStats.Alloc, bm.memoryThreshold)
	}

	return false, ""
}

// SetOnBackpressureApplied sets callback for backpressure application
func (bm *BackpressureMonitor) SetOnBackpressureApplied(fn func(reason string)) {
	bm.onBackpressureApplied = fn
}

// SetOnBackpressureRelieved sets callback for backpressure relief
func (bm *BackpressureMonitor) SetOnBackpressureRelieved(fn func()) {
	bm.onBackpressureRelieved = fn
}

// Shutdown stops the backpressure monitor
func (bm *BackpressureMonitor) Shutdown() {
	close(bm.shutdown)
}

// FlowControlConfigProvider provides configuration from various sources
type FlowControlConfigProvider interface {
	GetConfig() (FlowControlConfig, error)
}

// EnvConfigProvider loads configuration from environment variables
type EnvConfigProvider struct {
	prefix string // Environment variable prefix, e.g., "FLOW_"
}

// NewEnvConfigProvider creates a new environment config provider
func NewEnvConfigProvider(prefix string) *EnvConfigProvider {
	if prefix == "" {
		prefix = "FLOW_"
	}
	return &EnvConfigProvider{prefix: prefix}
}

// GetConfig loads configuration from environment variables
func (e *EnvConfigProvider) GetConfig() (FlowControlConfig, error) {
	config := FlowControlConfig{}

	// Load strategy
	if strategy := os.Getenv(e.prefix + "STRATEGY"); strategy != "" {
		config.Strategy = FlowControlStrategyType(strategy)
	} else {
		config.Strategy = StrategyTokenBucket
	}

	// Load numeric values
	if maxCredits := os.Getenv(e.prefix + "MAX_CREDITS"); maxCredits != "" {
		if val, err := strconv.ParseInt(maxCredits, 10, 64); err == nil {
			config.MaxCredits = val
		}
	}

	if minCredits := os.Getenv(e.prefix + "MIN_CREDITS"); minCredits != "" {
		if val, err := strconv.ParseInt(minCredits, 10, 64); err == nil {
			config.MinCredits = val
		}
	}

	if refillRate := os.Getenv(e.prefix + "REFILL_RATE"); refillRate != "" {
		if val, err := strconv.ParseInt(refillRate, 10, 64); err == nil {
			config.RefillRate = val
		}
	}

	if burstSize := os.Getenv(e.prefix + "BURST_SIZE"); burstSize != "" {
		if val, err := strconv.ParseInt(burstSize, 10, 64); err == nil {
			config.BurstSize = val
		}
	}

	// Load duration values
	if refillInterval := os.Getenv(e.prefix + "REFILL_INTERVAL"); refillInterval != "" {
		if val, err := time.ParseDuration(refillInterval); err == nil {
			config.RefillInterval = val
		}
	}

	// Set defaults if not specified
	e.setDefaults(&config)

	return config, nil
}

// setDefaults sets default values for missing configuration
func (e *EnvConfigProvider) setDefaults(config *FlowControlConfig) {
	if config.MaxCredits == 0 {
		config.MaxCredits = 1000
	}
	if config.MinCredits == 0 {
		config.MinCredits = 100
	}
	if config.RefillRate == 0 {
		config.RefillRate = 10
	}
	if config.RefillInterval == 0 {
		config.RefillInterval = 100 * time.Millisecond
	}
	if config.BurstSize == 0 {
		config.BurstSize = config.MaxCredits
	}
}

// FileConfigProvider loads configuration from a file
type FileConfigProvider struct {
	filePath string
}

// NewFileConfigProvider creates a new file config provider
func NewFileConfigProvider(filePath string) *FileConfigProvider {
	return &FileConfigProvider{filePath: filePath}
}

// GetConfig loads configuration from a file
func (f *FileConfigProvider) GetConfig() (FlowControlConfig, error) {
	data, err := os.ReadFile(f.filePath)
	if err != nil {
		return FlowControlConfig{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var config FlowControlConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return FlowControlConfig{}, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults for missing values
	f.setDefaults(&config)

	return config, nil
}

// setDefaults sets default values for missing configuration
func (f *FileConfigProvider) setDefaults(config *FlowControlConfig) {
	if config.Strategy == "" {
		config.Strategy = StrategyTokenBucket
	}
	if config.MaxCredits == 0 {
		config.MaxCredits = 1000
	}
	if config.MinCredits == 0 {
		config.MinCredits = 100
	}
	if config.RefillRate == 0 {
		config.RefillRate = 10
	}
	if config.RefillInterval == 0 {
		config.RefillInterval = 100 * time.Millisecond
	}
	if config.BurstSize == 0 {
		config.BurstSize = config.MaxCredits
	}
}

// CompositeConfigProvider combines multiple config providers
type CompositeConfigProvider struct {
	providers []FlowControlConfigProvider
}

// NewCompositeConfigProvider creates a new composite config provider
func NewCompositeConfigProvider(providers ...FlowControlConfigProvider) *CompositeConfigProvider {
	return &CompositeConfigProvider{providers: providers}
}

// GetConfig loads configuration from all providers, with later providers overriding earlier ones
func (c *CompositeConfigProvider) GetConfig() (FlowControlConfig, error) {
	var finalConfig FlowControlConfig

	for _, provider := range c.providers {
		config, err := provider.GetConfig()
		if err != nil {
			return FlowControlConfig{}, fmt.Errorf("config provider failed: %w", err)
		}

		// Merge configurations (simple override for now)
		if config.Strategy != "" {
			finalConfig.Strategy = config.Strategy
		}
		if config.MaxCredits != 0 {
			finalConfig.MaxCredits = config.MaxCredits
		}
		if config.MinCredits != 0 {
			finalConfig.MinCredits = config.MinCredits
		}
		if config.RefillRate != 0 {
			finalConfig.RefillRate = config.RefillRate
		}
		if config.RefillInterval != 0 {
			finalConfig.RefillInterval = config.RefillInterval
		}
		if config.BurstSize != 0 {
			finalConfig.BurstSize = config.BurstSize
		}
		if config.Logger != nil {
			finalConfig.Logger = config.Logger
		}
	}

	return finalConfig, nil
}
