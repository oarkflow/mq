package codec

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io" // added for full reads
	"net"
	"sync"
	"time" // added for handling deadlines

	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/internal/bpool"
)

// Protocol version for backward compatibility
const (
	ProtocolVersion        = uint8(1)
	MaxMessageSize         = 64 * 1024 * 1024 // 64MB default limit
	MaxHeaderSize          = 1024 * 1024      // 1MB header limit
	MaxQueueLength         = 255              // Max queue name length
	FragmentationThreshold = 16 * 1024 * 1024 // Messages larger than 16MB will be fragmented
	FragmentSize           = 8 * 1024 * 1024  // 8MB fragment size
	MaxFragments           = 256              // Maximum fragments per message
)

// Error definitions
var (
	ErrMessageTooLarge       = errors.New("message exceeds maximum size")
	ErrInvalidMessage        = errors.New("invalid message format")
	ErrInvalidQueue          = errors.New("invalid queue name")
	ErrInvalidCommand        = errors.New("invalid command")
	ErrConnectionClosed      = errors.New("connection closed")
	ErrTimeout               = errors.New("operation timeout")
	ErrProtocolMismatch      = errors.New("protocol version mismatch")
	ErrFragmentationRequired = errors.New("message requires fragmentation")
	ErrInvalidFragment       = errors.New("invalid message fragment")
	ErrFragmentTimeout       = errors.New("timed out waiting for fragments")
	ErrFragmentMissing       = errors.New("missing fragments in sequence")
)

// Config holds codec configuration
type Config struct {
	MaxMessageSize    uint32
	MaxHeaderSize     uint32
	MaxQueueLength    uint8
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	EnableCompression bool
	BufferPoolSize    int
}

// DefaultConfig returns default configuration with NO timeouts for persistent connections
func DefaultConfig() *Config {
	return &Config{
		MaxMessageSize:    MaxMessageSize,
		MaxHeaderSize:     MaxHeaderSize,
		MaxQueueLength:    MaxQueueLength,
		ReadTimeout:       0, // NO read timeout for persistent broker-consumer connections
		WriteTimeout:      0, // NO write timeout for persistent broker-consumer connections
		EnableCompression: false,
		BufferPoolSize:    1000,
	}
}

// MessageType indicates the type of message being sent
type MessageType uint8

const (
	MessageTypeStandard MessageType = iota
	MessageTypeFragment
	MessageTypeHeartbeat
	MessageTypeAck
	MessageTypeError
)

// MessageFlag represents various flags that can be set on messages
type MessageFlag uint16

const (
	FlagNone       MessageFlag = 0
	FlagFragmented MessageFlag = 1 << iota
	FlagCompressed
	FlagEncrypted
	FlagHighPriority
	FlagRedelivered
	FlagNoAck
)

// Message represents a protocol message with validation
type Message struct {
	Headers    map[string]string `msgpack:"h" json:"headers"`
	Queue      string            `msgpack:"q" json:"queue"`
	Payload    []byte            `msgpack:"p" json:"payload"`
	Command    consts.CMD        `msgpack:"c" json:"command"`
	Version    uint8             `msgpack:"v" json:"version"`
	Timestamp  int64             `msgpack:"t" json:"timestamp"`
	ID         string            `msgpack:"i" json:"id,omitempty"`
	Flags      MessageFlag       `msgpack:"f" json:"flags"`
	Type       MessageType       `msgpack:"mt" json:"messageType"`
	FragmentID uint32            `msgpack:"fid" json:"fragmentId,omitempty"`
	Fragments  uint16            `msgpack:"fs" json:"fragments,omitempty"`
	Sequence   uint16            `msgpack:"seq" json:"sequence,omitempty"`
}

// Codec handles message encoding/decoding with configuration
type Codec struct {
	config *Config
	mu     sync.RWMutex
	stats  *Stats
}

// Stats tracks codec statistics
type Stats struct {
	MessagesSent     uint64
	MessagesReceived uint64
	BytesSent        uint64
	BytesReceived    uint64
	Errors           uint64
	mu               sync.RWMutex
}

// NewCodec creates a new codec with configuration
func NewCodec(config *Config) *Codec {
	if config == nil {
		config = DefaultConfig()
	}
	return &Codec{
		config: config,
		stats:  &Stats{},
	}
}

// NewMessage creates a validated message
func NewMessage(cmd consts.CMD, payload []byte, queue string, headers map[string]string) (*Message, error) {
	if err := validateCommand(cmd); err != nil {
		return nil, err
	}

	if err := validateQueue(queue); err != nil {
		return nil, err
	}

	if headers == nil {
		headers = make(map[string]string)
	}

	if err := validateHeaders(headers); err != nil {
		return nil, err
	}

	return &Message{
		Headers:   headers,
		Queue:     queue,
		Command:   cmd,
		Payload:   payload,
		Version:   ProtocolVersion,
		Timestamp: time.Now().Unix(),
	}, nil
}

// Validate performs message validation
func (m *Message) Validate(config *Config) error {
	if m == nil {
		return ErrInvalidMessage
	}

	if m.Version != ProtocolVersion {
		return ErrProtocolMismatch
	}

	if err := validateCommand(m.Command); err != nil {
		return err
	}

	if err := validateQueue(m.Queue); err != nil {
		return err
	}

	if err := validateHeaders(m.Headers); err != nil {
		return err
	}

	if len(m.Payload) > int(config.MaxMessageSize) {
		fmt.Println(string(m.Payload))
		return fmt.Errorf("%v %v, payload size: %d", ErrMessageTooLarge, config.MaxMessageSize, len(m.Payload))
	}

	return nil
}

// Serialize converts message to bytes with validation
func (m *Message) Serialize() ([]byte, error) {
	if m == nil {
		return nil, ErrInvalidMessage
	}

	data, err := Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("serialization failed: %w", err)
	}

	return data, nil
}

// Deserialize converts bytes to message with validation
func Deserialize(data []byte) (*Message, error) {
	if len(data) == 0 {
		return nil, ErrInvalidMessage
	}

	var msg Message
	if err := Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("deserialization failed: %w", err)
	}

	return &msg, nil
}

// SendMessage sends a message with proper error handling and timeouts
func (c *Codec) SendMessage(ctx context.Context, conn net.Conn, msg *Message) error {
	// Check context cancellation before proceeding
	if err := ctx.Err(); err != nil {
		c.incrementErrors()
		return fmt.Errorf("context ended before send: %w", err)
	}

	if msg == nil {
		return ErrInvalidMessage
	}

	// Validate message
	if err := msg.Validate(c.config); err != nil {
		c.incrementErrors()
		return fmt.Errorf("message validation failed: %w", err)
	}

	// Check if this is a fragment message, if so handle it directly
	if msg.Type == MessageTypeFragment {
		return c.sendRawMessage(ctx, conn, msg)
	}

	// Handle fragmentation for large messages if needed
	if len(msg.Payload) > int(FragmentationThreshold) && msg.Type != MessageTypeFragment {
		fm := NewFragmentManager(c, c.config)
		defer fm.Stop()
		return fm.sendFragmentedMessage(ctx, conn, msg)
	}

	// Standard message send path
	return c.sendRawMessage(ctx, conn, msg)
}

// sendRawMessage handles the actual sending of a message or fragment WITHOUT any timeouts
func (c *Codec) sendRawMessage(ctx context.Context, conn net.Conn, msg *Message) error {
	// Serialize message
	data, err := msg.Serialize()
	if err != nil {
		c.incrementErrors()
		return fmt.Errorf("message serialization failed: %w", err)
	}

	// Check message size
	if len(data) > int(c.config.MaxMessageSize) {
		c.incrementErrors()
		return fmt.Errorf("%v %v, payload: %d", ErrMessageTooLarge, c.config.MaxMessageSize, len(data))
	}

	// Prepare buffer
	totalLength := 4 + len(data)
	buffer := bpool.Get()
	defer bpool.Put(buffer)
	buffer.Reset()

	if cap(buffer.B) < totalLength {
		buffer.B = make([]byte, totalLength)
	} else {
		buffer.B = buffer.B[:totalLength]
	}

	// Write length prefix and data
	binary.BigEndian.PutUint32(buffer.B[:4], uint32(len(data)))
	copy(buffer.B[4:], data)

	// CRITICAL: NEVER set any write deadlines for broker-consumer connections
	// These connections must remain open indefinitely for persistent communication
	// Completely removed all timeout/deadline logic to prevent I/O timeouts

	// Write with buffering
	writer := bufio.NewWriter(conn)

	written, err := writer.Write(buffer.B[:totalLength])
	if err != nil {
		c.incrementErrors()
		return fmt.Errorf("write failed after %d bytes: %w", written, err)
	}

	if err := writer.Flush(); err != nil {
		c.incrementErrors()
		return fmt.Errorf("flush failed: %w", err)
	}

	// Update statistics
	c.stats.mu.Lock()
	c.stats.MessagesSent++
	c.stats.BytesSent += uint64(totalLength)
	c.stats.mu.Unlock()

	return nil
}

// ReadMessage reads a message WITHOUT any timeouts for persistent broker-consumer connections
func (c *Codec) ReadMessage(ctx context.Context, conn net.Conn) (*Message, error) {
	// Check context cancellation before proceeding
	if err := ctx.Err(); err != nil {
		c.incrementErrors()
		return nil, fmt.Errorf("context ended before read: %w", err)
	}

	// CRITICAL: NEVER set any read deadlines for broker-consumer connections
	// These connections must remain open indefinitely for persistent communication
	// Completely removed all timeout/deadline logic to prevent I/O timeouts

	// Read length prefix
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBytes); err != nil {
		c.incrementErrors()
		if errors.Is(err, io.EOF) {
			return nil, ErrConnectionClosed
		}
		return nil, fmt.Errorf("failed to read message length: %w", err)
	}

	length := binary.BigEndian.Uint32(lengthBytes)

	// Validate message size
	if length > c.config.MaxMessageSize {
		c.incrementErrors()
		return nil, fmt.Errorf("%v %v, payload size: %d while ReadMessage", ErrMessageTooLarge, c.config.MaxMessageSize, length)
	}

	if length == 0 {
		c.incrementErrors()
		return nil, ErrInvalidMessage
	}

	// Read message data
	buffer := bpool.Get()
	defer bpool.Put(buffer)
	buffer.Reset()

	if cap(buffer.B) < int(length) {
		buffer.B = make([]byte, length)
	} else {
		buffer.B = buffer.B[:length]
	}

	if _, err := io.ReadFull(conn, buffer.B[:length]); err != nil {
		c.incrementErrors()
		if errors.Is(err, io.EOF) {
			return nil, ErrConnectionClosed
		}
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	// Deserialize message
	msg, err := Deserialize(buffer.B[:length])
	if err != nil {
		c.incrementErrors()
		return nil, fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Validate message
	if err := msg.Validate(c.config); err != nil {
		c.incrementErrors()
		return nil, fmt.Errorf("message validation failed: %w", err)
	}

	// Handle message fragments if needed
	if msg.Type == MessageTypeFragment || (msg.Flags&FlagFragmented) != 0 {
		fm := NewFragmentManager(c, c.config)
		reassembled, isFragment, err := fm.processFragment(msg)
		if err != nil {
			c.incrementErrors()
			return nil, fmt.Errorf("fragment processing failed: %w", err)
		}

		// If this is a fragment but reassembly isn't complete yet
		if isFragment && reassembled == nil {
			// Update statistics but return nil with no error to indicate
			// the caller should continue reading messages
			c.stats.mu.Lock()
			c.stats.MessagesReceived++
			c.stats.BytesReceived += uint64(4 + length)
			c.stats.mu.Unlock()

			// Read the next fragment
			return c.ReadMessage(ctx, conn)
		}

		// Use the reassembled message if available
		if reassembled != nil {
			msg = reassembled
		}
	}

	// Update statistics
	c.stats.mu.Lock()
	c.stats.MessagesReceived++
	c.stats.BytesReceived += uint64(4 + length)
	c.stats.mu.Unlock()

	return msg, nil
}

// GetStats returns codec statistics
func (c *Codec) GetStats() Stats {
	c.stats.mu.RLock()
	defer c.stats.mu.RUnlock()
	return *c.stats
}

// ResetStats resets codec statistics
func (c *Codec) ResetStats() {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	*c.stats = Stats{}
}

// Helper functions for validation
func validateCommand(cmd consts.CMD) error {
	// Add validation based on your command constants
	if cmd < 0 {
		return ErrInvalidCommand
	}
	return nil
}

func validateQueue(queue string) error {
	if len(queue) == 0 || len(queue) > MaxQueueLength {
		return ErrInvalidQueue
	}
	return nil
}

func validateHeaders(headers map[string]string) error {
	totalSize := 0
	for k, v := range headers {
		totalSize += len(k) + len(v)
		if totalSize > MaxHeaderSize {
			return fmt.Errorf("%v %v, payload size: %d while validating headers", ErrMessageTooLarge, MaxHeaderSize, totalSize)
		}
	}
	return nil
}

func (c *Codec) incrementErrors() {
	c.stats.mu.Lock()
	c.stats.Errors++
	c.stats.mu.Unlock()
}

// SendMessage Backward compatibility functions
func SendMessage(ctx context.Context, conn net.Conn, msg *Message) error {
	codec := NewCodec(DefaultConfig())
	return codec.SendMessage(ctx, conn, msg)
}

func ReadMessage(ctx context.Context, conn net.Conn) (*Message, error) {
	codec := NewCodec(DefaultConfig())
	return codec.ReadMessage(ctx, conn)
}
