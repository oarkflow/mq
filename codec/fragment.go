package codec

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"sync"
	"time"

	"github.com/oarkflow/mq/consts"
)

// FragmentManager handles message fragmentation and reassembly
type FragmentManager struct {
	codec             *Codec
	config            *Config
	fragmentStore     map[string]*fragmentAssembly
	mu                sync.RWMutex
	cleanupInterval   time.Duration
	cleanupTimer      *time.Timer
	cleanupChan       chan struct{}
	reassemblyTimeout time.Duration
}

// fragmentAssembly holds fragments for a specific message
type fragmentAssembly struct {
	fragments   map[uint16][]byte
	totalSize   int
	createdAt   time.Time
	totalCount  uint16
	messageType MessageType
	queue       string
	command     consts.CMD
	headers     map[string]string
	id          string
}

// NewFragmentManager creates a new fragment manager
func NewFragmentManager(codec *Codec, config *Config) *FragmentManager {
	fm := &FragmentManager{
		codec:             codec,
		config:            config,
		fragmentStore:     make(map[string]*fragmentAssembly),
		cleanupInterval:   time.Minute,
		reassemblyTimeout: 5 * time.Minute,
	}

	fm.startCleanupTimer()
	return fm
}

// startCleanupTimer starts a timer to clean up old fragments
func (fm *FragmentManager) startCleanupTimer() {
	fm.cleanupChan = make(chan struct{})
	fm.cleanupTimer = time.NewTimer(fm.cleanupInterval)

	go func() {
		for {
			select {
			case <-fm.cleanupTimer.C:
				fm.cleanupExpiredFragments()
				fm.cleanupTimer.Reset(fm.cleanupInterval)
			case <-fm.cleanupChan:
				return
			}
		}
	}()
}

// Stop stops the fragment manager
func (fm *FragmentManager) Stop() {
	if fm.cleanupTimer != nil {
		fm.cleanupTimer.Stop()
	}

	if fm.cleanupChan != nil {
		close(fm.cleanupChan)
	}
}

// cleanupExpiredFragments removes expired fragment assemblies
func (fm *FragmentManager) cleanupExpiredFragments() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	now := time.Now()
	for id, assembly := range fm.fragmentStore {
		if now.Sub(assembly.createdAt) > fm.reassemblyTimeout {
			delete(fm.fragmentStore, id)
		}
	}
}

// fragmentMessage splits a large message into fragments
func (fm *FragmentManager) fragmentMessage(ctx context.Context, msg *Message) ([]*Message, error) {
	if msg == nil {
		return nil, ErrInvalidMessage
	}

	// Check if fragmentation is needed
	if len(msg.Payload) <= int(FragmentationThreshold) {
		return []*Message{msg}, nil
	}

	// Calculate how many fragments we need
	fragmentCount := (len(msg.Payload) + int(FragmentSize) - 1) / int(FragmentSize)
	if fragmentCount > int(MaxFragments) {
		return nil, fmt.Errorf("%w: would require %d fragments, maximum is %d",
			ErrMessageTooLarge, fragmentCount, MaxFragments)
	}

	// Generate a fragment ID using a hash of message content + timestamp
	idBytes := []byte(msg.ID)
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(msg.Timestamp))
	hashInput := append(idBytes, timestampBytes...)
	hashInput = append(hashInput, msg.Payload[:min(1024, len(msg.Payload))]...)
	fragmentID := crc32.ChecksumIEEE(hashInput)

	// Create fragment messages
	fragments := make([]*Message, fragmentCount)
	for i := 0; i < fragmentCount; i++ {
		// Calculate fragment payload boundaries
		start := i * int(FragmentSize)
		end := min((i+1)*int(FragmentSize), len(msg.Payload))
		fragmentPayload := msg.Payload[start:end]

		// Create fragment message
		fragment := &Message{
			Headers:    copyHeaders(msg.Headers),
			Queue:      msg.Queue,
			Command:    msg.Command,
			Version:    msg.Version,
			Timestamp:  msg.Timestamp,
			ID:         msg.ID,
			Type:       MessageTypeFragment,
			Flags:      msg.Flags | FlagFragmented,
			FragmentID: fragmentID,
			Fragments:  uint16(fragmentCount),
			Sequence:   uint16(i),
			Payload:    fragmentPayload,
		}

		fragments[i] = fragment
	}

	return fragments, nil
}

// sendFragmentedMessage sends a large message as multiple fragments
func (fm *FragmentManager) sendFragmentedMessage(ctx context.Context, conn net.Conn, msg *Message) error {
	fragments, err := fm.fragmentMessage(ctx, msg)
	if err != nil {
		return err
	}

	// If no fragmentation was needed, send as normal
	if len(fragments) == 1 && fragments[0].Type != MessageTypeFragment {
		return fm.codec.SendMessage(ctx, conn, fragments[0])
	}

	// Send each fragment
	for _, fragment := range fragments {
		if err := fm.codec.SendMessage(ctx, conn, fragment); err != nil {
			return fmt.Errorf("failed to send fragment %d/%d: %w",
				fragment.Sequence+1, fragment.Fragments, err)
		}
	}

	return nil
}

// processFragment processes a fragment and attempts reassembly
func (fm *FragmentManager) processFragment(msg *Message) (*Message, bool, error) {
	if msg == nil || msg.Type != MessageTypeFragment || msg.FragmentID == 0 {
		return msg, false, nil // Not a fragment, return as is
	}

	// Generate a unique key for this fragmented message
	key := fmt.Sprintf("%s-%d-%s", msg.ID, msg.FragmentID, msg.Queue)

	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Check if we already have an assembly for this message
	assembly, exists := fm.fragmentStore[key]
	if !exists {
		// Create a new assembly
		assembly = &fragmentAssembly{
			fragments:   make(map[uint16][]byte),
			createdAt:   time.Now(),
			totalCount:  msg.Fragments,
			messageType: MessageTypeStandard,
			queue:       msg.Queue,
			command:     msg.Command,
			headers:     copyHeaders(msg.Headers),
			id:          msg.ID,
		}
		fm.fragmentStore[key] = assembly
	}

	// Store this fragment
	assembly.fragments[msg.Sequence] = msg.Payload
	assembly.totalSize += len(msg.Payload)

	// Check if we have all fragments
	if len(assembly.fragments) == int(assembly.totalCount) {
		// We have all fragments, reassemble the message
		reassembled, err := fm.reassembleMessage(key, assembly)
		if err != nil {
			return nil, true, err
		}
		return reassembled, true, nil
	}

	// Still waiting for more fragments
	return nil, true, nil
}

// reassembleMessage combines fragments into the original message
func (fm *FragmentManager) reassembleMessage(key string, assembly *fragmentAssembly) (*Message, error) {
	// Remove the assembly from the store
	delete(fm.fragmentStore, key)

	// Check if we have all fragments
	if len(assembly.fragments) != int(assembly.totalCount) {
		return nil, ErrFragmentMissing
	}

	// Allocate space for the full payload
	fullPayload := make([]byte, assembly.totalSize)

	// Combine fragments in order
	offset := 0
	for i := uint16(0); i < assembly.totalCount; i++ {
		fragment, exists := assembly.fragments[i]
		if !exists {
			return nil, fmt.Errorf("%w: missing fragment %d", ErrFragmentMissing, i)
		}

		copy(fullPayload[offset:], fragment)
		offset += len(fragment)
	}

	// Create the reassembled message
	msg := &Message{
		Headers:   assembly.headers,
		Queue:     assembly.queue,
		Command:   assembly.command,
		Version:   ProtocolVersion,
		Timestamp: time.Now().Unix(),
		ID:        assembly.id,
		Type:      assembly.messageType,
		Flags:     FlagNone, // Clear fragmentation flag
		Payload:   fullPayload,
	}

	return msg, nil
}

// Helper function to make a copy of headers to avoid shared references
func copyHeaders(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}

	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
