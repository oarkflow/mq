package mq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/oarkflow/mq/logger"
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

// FlowController manages backpressure and flow control
type FlowController struct {
	credits          int64
	maxCredits       int64
	minCredits       int64
	creditRefillRate int64
	mu               sync.Mutex
	logger           logger.Logger
	shutdown         chan struct{}
	refillInterval   time.Duration
	onCreditLow      func(current, max int64)
	onCreditHigh     func(current, max int64)
}

// FlowControlConfig holds flow control configuration
type FlowControlConfig struct {
	MaxCredits     int64
	MinCredits     int64
	RefillRate     int64 // Credits to add per interval
	RefillInterval time.Duration
	Logger         logger.Logger
}

// NewFlowController creates a new flow controller
func NewFlowController(config FlowControlConfig) *FlowController {
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

	fc := &FlowController{
		credits:          config.MaxCredits,
		maxCredits:       config.MaxCredits,
		minCredits:       config.MinCredits,
		creditRefillRate: config.RefillRate,
		refillInterval:   config.RefillInterval,
		logger:           config.Logger,
		shutdown:         make(chan struct{}),
	}

	go fc.refillLoop()

	return fc
}

// AcquireCredit attempts to acquire credits for processing
func (fc *FlowController) AcquireCredit(ctx context.Context, amount int64) error {
	for {
		fc.mu.Lock()
		if fc.credits >= amount {
			fc.credits -= amount

			// Check if credits are low
			if fc.credits < fc.minCredits && fc.onCreditLow != nil {
				go fc.onCreditLow(fc.credits, fc.maxCredits)
			}

			fc.mu.Unlock()
			return nil
		}
		fc.mu.Unlock()

		// Wait before retrying
		select {
		case <-time.After(10 * time.Millisecond):
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-fc.shutdown:
			return fmt.Errorf("flow controller shutting down")
		}
	}
}

// ReleaseCredit returns credits after processing
func (fc *FlowController) ReleaseCredit(amount int64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.credits += amount
	if fc.credits > fc.maxCredits {
		fc.credits = fc.maxCredits
	}

	// Check if credits recovered
	if fc.credits > fc.maxCredits/2 && fc.onCreditHigh != nil {
		go fc.onCreditHigh(fc.credits, fc.maxCredits)
	}
}

// refillLoop periodically refills credits
func (fc *FlowController) refillLoop() {
	ticker := time.NewTicker(fc.refillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fc.mu.Lock()
			fc.credits += fc.creditRefillRate
			if fc.credits > fc.maxCredits {
				fc.credits = fc.maxCredits
			}
			fc.mu.Unlock()
		case <-fc.shutdown:
			return
		}
	}
}

// GetAvailableCredits returns the current available credits
func (fc *FlowController) GetAvailableCredits() int64 {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.credits
}

// SetOnCreditLow sets callback for low credit warning
func (fc *FlowController) SetOnCreditLow(fn func(current, max int64)) {
	fc.onCreditLow = fn
}

// SetOnCreditHigh sets callback for credit recovery
func (fc *FlowController) SetOnCreditHigh(fn func(current, max int64)) {
	fc.onCreditHigh = fn
}

// AdjustMaxCredits dynamically adjusts maximum credits
func (fc *FlowController) AdjustMaxCredits(newMax int64) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	fc.maxCredits = newMax
	if fc.credits > newMax {
		fc.credits = newMax
	}

	fc.logger.Info("Adjusted max credits",
		logger.Field{Key: "newMax", Value: newMax})
}

// GetStats returns flow control statistics
func (fc *FlowController) GetStats() map[string]interface{} {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	utilization := float64(fc.maxCredits-fc.credits) / float64(fc.maxCredits) * 100

	return map[string]interface{}{
		"credits":     fc.credits,
		"max_credits": fc.maxCredits,
		"min_credits": fc.minCredits,
		"utilization": utilization,
		"refill_rate": fc.creditRefillRate,
	}
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
