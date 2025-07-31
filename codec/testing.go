package codec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/oarkflow/mq/consts"
)

// MockConn implements net.Conn for testing
type MockConn struct {
	ReadBuffer  *bytes.Buffer
	WriteBuffer *bytes.Buffer
	ReadDelay   time.Duration
	WriteDelay  time.Duration
	IsClosed    bool
	ReadErr     error
	WriteErr    error
	mu          sync.Mutex
}

// NewMockConn creates a new mock connection
func NewMockConn() *MockConn {
	return &MockConn{
		ReadBuffer:  bytes.NewBuffer(nil),
		WriteBuffer: bytes.NewBuffer(nil),
	}
}

// Read implements the net.Conn Read method
func (m *MockConn) Read(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.IsClosed {
		return 0, errors.New("connection closed")
	}

	if m.ReadErr != nil {
		return 0, m.ReadErr
	}

	if m.ReadDelay > 0 {
		time.Sleep(m.ReadDelay)
	}

	return m.ReadBuffer.Read(b)
}

// Write implements the net.Conn Write method
func (m *MockConn) Write(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.IsClosed {
		return 0, errors.New("connection closed")
	}

	if m.WriteErr != nil {
		return 0, m.WriteErr
	}

	if m.WriteDelay > 0 {
		time.Sleep(m.WriteDelay)
	}

	return m.WriteBuffer.Write(b)
}

// Close implements the net.Conn Close method
func (m *MockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.IsClosed = true
	return nil
}

// LocalAddr implements the net.Conn LocalAddr method
func (m *MockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
}

// RemoteAddr implements the net.Conn RemoteAddr method
func (m *MockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
}

// SetDeadline implements the net.Conn SetDeadline method
func (m *MockConn) SetDeadline(t time.Time) error {
	return nil
}

// SetReadDeadline implements the net.Conn SetReadDeadline method
func (m *MockConn) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline implements the net.Conn SetWriteDeadline method
func (m *MockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// CodecTestSuite provides utilities for testing the codec
type CodecTestSuite struct {
	Codec  *Codec
	Config *Config
}

// NewCodecTestSuite creates a new codec test suite
func NewCodecTestSuite() *CodecTestSuite {
	config := DefaultConfig()
	// Set smaller timeouts for testing
	config.ReadTimeout = 500 * time.Millisecond
	config.WriteTimeout = 500 * time.Millisecond

	return &CodecTestSuite{
		Codec:  NewCodec(config),
		Config: config,
	}
}

// SendReceiveTest tests sending and receiving a message
func (ts *CodecTestSuite) SendReceiveTest(msg *Message) error {
	conn := NewMockConn()

	// Send the message
	ctx := context.Background()
	if err := ts.Codec.SendMessage(ctx, conn, msg); err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	// Move written data to read buffer to simulate network transport
	conn.ReadBuffer.Write(conn.WriteBuffer.Bytes())
	conn.WriteBuffer.Reset()

	// Receive the message
	received, err := ts.Codec.ReadMessage(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}

	// Validate the message
	if received.Command != msg.Command {
		return fmt.Errorf("command mismatch: got %v, want %v", received.Command, msg.Command)
	}

	if received.Queue != msg.Queue {
		return fmt.Errorf("queue mismatch: got %v, want %v", received.Queue, msg.Queue)
	}

	if !bytes.Equal(received.Payload, msg.Payload) {
		return fmt.Errorf("payload mismatch: got %d bytes, want %d bytes", len(received.Payload), len(msg.Payload))
	}

	return nil
}

// FragmentationTest tests the fragmentation and reassembly of large messages
func (ts *CodecTestSuite) FragmentationTest(payload []byte) error {
	msg := &Message{
		Command:   consts.CMD(1), // Use appropriate command from your consts
		Queue:     "test_queue",
		Headers:   map[string]string{"test": "header"},
		Payload:   payload,
		Version:   ProtocolVersion,
		Timestamp: time.Now().Unix(),
		ID:        "test-message-id",
	}

	conn := NewMockConn()

	// Configure fragmentation
	fm := NewFragmentManager(ts.Codec, ts.Config)

	// Send the fragmented message
	ctx := context.Background()
	if err := fm.sendFragmentedMessage(ctx, conn, msg); err != nil {
		return fmt.Errorf("failed to send fragmented message: %w", err)
	}

	// Move written data to read buffer to simulate network transport
	conn.ReadBuffer.Write(conn.WriteBuffer.Bytes())
	conn.WriteBuffer.Reset()

	// Receive and reassemble the message
	received, err := ts.Codec.ReadMessage(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}

	// Validate the reassembled message
	if received.Command != msg.Command {
		return fmt.Errorf("command mismatch: got %v, want %v", received.Command, msg.Command)
	}

	if received.Queue != msg.Queue {
		return fmt.Errorf("queue mismatch: got %v, want %v", received.Queue, msg.Queue)
	}

	if !bytes.Equal(received.Payload, msg.Payload) {
		return fmt.Errorf("payload mismatch: got %d bytes, want %d bytes", len(received.Payload), len(msg.Payload))
	}

	return nil
}
