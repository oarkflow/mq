package codec

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/mq/consts"
)

// HeartbeatManager manages heartbeat messages for connection health monitoring
type HeartbeatManager struct {
	codec            *Codec
	interval         time.Duration
	timeout          time.Duration
	conn             net.Conn
	stopChan         chan struct{}
	lastHeartbeat    atomic.Int64
	lastReceived     atomic.Int64
	onFailure        func(error)
	mu               sync.RWMutex
	isRunning        bool
	heartbeatsSent   uint64
	heartbeatsRecv   uint64
	failedHeartbeats uint64
}

// NewHeartbeatManager creates a new heartbeat manager
func NewHeartbeatManager(codec *Codec, conn net.Conn) *HeartbeatManager {
	return &HeartbeatManager{
		codec:    codec,
		interval: 30 * time.Second,
		timeout:  90 * time.Second, // 3x interval
		conn:     conn,
		stopChan: make(chan struct{}),
	}
}

// SetInterval sets the heartbeat interval
func (hm *HeartbeatManager) SetInterval(interval time.Duration) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.interval = interval
}

// SetTimeout sets the heartbeat timeout
func (hm *HeartbeatManager) SetTimeout(timeout time.Duration) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.timeout = timeout
}

// SetOnFailure sets the callback function for heartbeat failures
func (hm *HeartbeatManager) SetOnFailure(fn func(error)) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.onFailure = fn
}

// Start starts the heartbeat monitoring
func (hm *HeartbeatManager) Start() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if hm.isRunning {
		return
	}

	hm.isRunning = true
	hm.lastHeartbeat.Store(time.Now().Unix())
	hm.lastReceived.Store(time.Now().Unix())

	go hm.sendHeartbeats()
	go hm.monitorHeartbeats()
}

// Stop stops the heartbeat monitoring
func (hm *HeartbeatManager) Stop() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if !hm.isRunning {
		return
	}

	hm.isRunning = false
	close(hm.stopChan)
	// Create a new stop channel for future use
	hm.stopChan = make(chan struct{})
}

// IsRunning returns whether the heartbeat manager is running
func (hm *HeartbeatManager) IsRunning() bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.isRunning
}

// GetStats returns heartbeat statistics
func (hm *HeartbeatManager) GetStats() map[string]uint64 {
	return map[string]uint64{
		"sent":     atomic.LoadUint64(&hm.heartbeatsSent),
		"received": atomic.LoadUint64(&hm.heartbeatsRecv),
		"failed":   atomic.LoadUint64(&hm.failedHeartbeats),
	}
}

// RecordHeartbeat records a received heartbeat
func (hm *HeartbeatManager) RecordHeartbeat() {
	hm.lastReceived.Store(time.Now().Unix())
	atomic.AddUint64(&hm.heartbeatsRecv, 1)
}

// sendHeartbeats sends periodic heartbeat messages
func (hm *HeartbeatManager) sendHeartbeats() {
	ticker := time.NewTicker(hm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := hm.sendHeartbeat(); err != nil {
				atomic.AddUint64(&hm.failedHeartbeats, 1)
				hm.mu.RLock()
				onFailure := hm.onFailure
				hm.mu.RUnlock()

				if onFailure != nil {
					onFailure(err)
				}
			}
		case <-hm.stopChan:
			return
		}
	}
}

// monitorHeartbeats monitors the heartbeat health
func (hm *HeartbeatManager) monitorHeartbeats() {
	ticker := time.NewTicker(hm.interval / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now().Unix()
			lastReceived := hm.lastReceived.Load()

			// Check if we've exceeded the timeout
			if now-lastReceived > int64(hm.timeout.Seconds()) {
				err := fmt.Errorf("heartbeat timeout: last received %v seconds ago",
					now-lastReceived)

				atomic.AddUint64(&hm.failedHeartbeats, 1)
				hm.mu.RLock()
				onFailure := hm.onFailure
				hm.mu.RUnlock()

				if onFailure != nil {
					onFailure(err)
				}
			}
		case <-hm.stopChan:
			return
		}
	}
}

// sendHeartbeat sends a single heartbeat message
func (hm *HeartbeatManager) sendHeartbeat() error {
	msg := &Message{
		Type:      MessageTypeHeartbeat,
		Command:   consts.CMD(0), // Use appropriate heartbeat command from your consts
		Version:   ProtocolVersion,
		Timestamp: time.Now().Unix(),
		Headers:   map[string]string{"type": "heartbeat"},
		Payload:   []byte{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := hm.codec.SendMessage(ctx, hm.conn, msg)
	if err == nil {
		hm.lastHeartbeat.Store(time.Now().Unix())
		atomic.AddUint64(&hm.heartbeatsSent, 1)
	}

	return err
}
