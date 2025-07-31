package codec

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/oarkflow/json"
	"golang.org/x/crypto/chacha20poly1305"
)

// Error definitions for serialization
var (
	ErrSerializationFailed   = errors.New("serialization failed")
	ErrDeserializationFailed = errors.New("deserialization failed")
	ErrCompressionFailed     = errors.New("compression failed")
	ErrDecompressionFailed   = errors.New("decompression failed")
	ErrEncryptionFailed      = errors.New("encryption failed")
	ErrDecryptionFailed      = errors.New("decryption failed")
	ErrInvalidKey            = errors.New("invalid encryption key")
)

// ContentType represents the content type of serialized data
type ContentType string

const (
	ContentTypeJSON    ContentType = "application/json"
	ContentTypeMsgPack ContentType = "application/msgpack"
	ContentTypeCBOR    ContentType = "application/cbor"
)

// Marshaller interface for pluggable serialization
type Marshaller interface {
	Marshal(v any) ([]byte, error)
	ContentType() ContentType
}

// Unmarshaller interface for pluggable deserialization
type Unmarshaller interface {
	Unmarshal(data []byte, v any) error
	ContentType() ContentType
}

// MarshallerFunc adapter
type MarshallerFunc func(v any) ([]byte, error)

func (f MarshallerFunc) Marshal(v any) ([]byte, error) {
	return f(v)
}

func (f MarshallerFunc) ContentType() ContentType {
	return ContentTypeJSON
}

// UnmarshallerFunc adapter
type UnmarshallerFunc func(data []byte, v any) error

func (f UnmarshallerFunc) Unmarshal(data []byte, v any) error {
	return f(data, v)
}

func (f UnmarshallerFunc) ContentType() ContentType {
	return ContentTypeJSON
}

// SerializationConfig holds serialization configuration
type SerializationConfig struct {
	EnableCompression   bool
	CompressionLevel    int
	MaxCompressionRatio float64
	EnableEncryption    bool
	EncryptionKey       []byte
	PreferredCipher     string // "chacha20poly1305" or "aes-gcm"
}

// DefaultSerializationConfig returns default configuration
func DefaultSerializationConfig() *SerializationConfig {
	return &SerializationConfig{
		EnableCompression:   false,
		CompressionLevel:    gzip.DefaultCompression,
		MaxCompressionRatio: 0.8, // Only compress if we save at least 20%
		EnableEncryption:    false,
		PreferredCipher:     "chacha20poly1305",
	}
}

// SerializationManager manages serialization with configuration
type SerializationManager struct {
	marshaller   Marshaller
	unmarshaller Unmarshaller
	config       *SerializationConfig
	mu           sync.RWMutex
	cachedCipher cipher.AEAD
	cipherMu     sync.Mutex
}

// NewSerializationManager creates a new serialization manager
func NewSerializationManager(config *SerializationConfig) *SerializationManager {
	if config == nil {
		config = DefaultSerializationConfig()
	}

	return &SerializationManager{
		marshaller: MarshallerFunc(json.Marshal),
		unmarshaller: UnmarshallerFunc(func(data []byte, v any) error {
			return json.Unmarshal(data, v)
		}),
		config: config,
	}
}

// SetMarshaller sets custom marshaller
func (sm *SerializationManager) SetMarshaller(marshaller Marshaller) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.marshaller = marshaller
}

// SetUnmarshaller sets custom unmarshaller
func (sm *SerializationManager) SetUnmarshaller(unmarshaller Unmarshaller) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.unmarshaller = unmarshaller
}

// SetEncryptionKey sets the encryption key
func (sm *SerializationManager) SetEncryptionKey(key []byte) error {
	if sm.config.EnableEncryption && len(key) == 0 {
		return ErrInvalidKey
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.config.EncryptionKey = key
	// Clear the cached cipher so it will be recreated with the new key
	sm.cipherMu.Lock()
	defer sm.cipherMu.Unlock()
	sm.cachedCipher = nil

	return nil
}

// Marshal serializes data with optional compression and encryption
func (sm *SerializationManager) Marshal(v any) ([]byte, error) {
	sm.mu.RLock()
	marshaller := sm.marshaller
	config := sm.config
	sm.mu.RUnlock()

	// Serialize the data
	data, err := marshaller.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSerializationFailed, err)
	}

	// Apply compression if enabled and beneficial
	if config.EnableCompression && len(data) > 256 { // Only compress larger payloads
		compressed, err := sm.compress(data)
		if err != nil {
			// Continue with uncompressed data on error
			// but don't return error to allow for graceful degradation
		} else if float64(len(compressed))/float64(len(data)) <= config.MaxCompressionRatio {
			// Only use compression if it provides significant benefit
			data = compressed
		}
	}

	// Apply encryption if enabled
	if config.EnableEncryption && len(config.EncryptionKey) > 0 {
		encrypted, err := sm.encrypt(data)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrEncryptionFailed, err)
		}
		data = encrypted
	}

	return data, nil
}

// Unmarshal deserializes data with optional decompression and decryption
func (sm *SerializationManager) Unmarshal(data []byte, v any) error {
	if len(data) == 0 {
		return fmt.Errorf("%w: empty data", ErrDeserializationFailed)
	}

	sm.mu.RLock()
	unmarshaller := sm.unmarshaller
	config := sm.config
	sm.mu.RUnlock()

	// Apply decryption if enabled
	if config.EnableEncryption && len(config.EncryptionKey) > 0 {
		decrypted, err := sm.decrypt(data)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDecryptionFailed, err)
		}
		data = decrypted
	}

	// Try decompression if enabled
	if config.EnableCompression && sm.isCompressed(data) {
		decompressed, err := sm.decompress(data)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDecompressionFailed, err)
		}
		data = decompressed
	}

	// Deserialize the data
	if err := unmarshaller.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%w: %v", ErrDeserializationFailed, err)
	}

	return nil
}

// compress compresses data using gzip
func (sm *SerializationManager) compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	// Add compression marker
	buf.WriteByte(0x1f) // gzip magic number

	writer, err := gzip.NewWriterLevel(&buf, sm.config.CompressionLevel)
	if err != nil {
		return nil, err
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// decompress decompresses gzip data
func (sm *SerializationManager) decompress(data []byte) ([]byte, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("invalid compressed data")
	}

	// Remove compression marker
	if data[0] != 0x1f {
		return data, nil // Not compressed
	}

	reader, err := gzip.NewReader(bytes.NewReader(data[1:]))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(reader); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// isCompressed checks if data is compressed
func (sm *SerializationManager) isCompressed(data []byte) bool {
	return len(data) > 0 && data[0] == 0x1f
}

// getCipher returns a cipher.AEAD instance for encryption/decryption
func (sm *SerializationManager) getCipher() (cipher.AEAD, error) {
	sm.cipherMu.Lock()
	defer sm.cipherMu.Unlock()

	if sm.cachedCipher != nil {
		return sm.cachedCipher, nil
	}

	var aead cipher.AEAD
	var err error

	sm.mu.RLock()
	key := sm.config.EncryptionKey
	preferredCipher := sm.config.PreferredCipher
	sm.mu.RUnlock()

	switch preferredCipher {
	case "chacha20poly1305":
		aead, err = chacha20poly1305.New(key)
	case "aes-gcm":
		block, e := aes.NewCipher(key)
		if e != nil {
			err = e
			break
		}
		aead, err = cipher.NewGCM(block)
	default:
		// Default to ChaCha20-Poly1305
		aead, err = chacha20poly1305.New(key)
	}

	if err != nil {
		return nil, err
	}

	sm.cachedCipher = aead
	return aead, nil
}

// encrypt encrypts data using authenticated encryption
func (sm *SerializationManager) encrypt(data []byte) ([]byte, error) {
	aead, err := sm.getCipher()
	if err != nil {
		return nil, err
	}

	// Generate a random nonce
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Add timestamp to associated data for replay protection
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(time.Now().Unix()))

	// Encrypt and authenticate
	ciphertext := aead.Seal(nil, nonce, data, timestampBytes)

	// Prepend nonce and timestamp to ciphertext
	result := make([]byte, len(nonce)+len(timestampBytes)+len(ciphertext))
	copy(result, nonce)
	copy(result[len(nonce):], timestampBytes)
	copy(result[len(nonce)+len(timestampBytes):], ciphertext)

	return result, nil
}

// decrypt decrypts data using authenticated decryption
func (sm *SerializationManager) decrypt(data []byte) ([]byte, error) {
	aead, err := sm.getCipher()
	if err != nil {
		return nil, err
	}

	// Extract nonce
	nonceSize := aead.NonceSize()
	if len(data) < nonceSize+8 { // 8 bytes for timestamp
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce := data[:nonceSize]
	timestampBytes := data[nonceSize : nonceSize+8]
	ciphertext := data[nonceSize+8:]

	// Decrypt and verify
	plaintext, err := aead.Open(nil, nonce, ciphertext, timestampBytes)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// Global serialization manager instance
var globalSerializationManager = NewSerializationManager(nil)
var globalSerializationManagerMu sync.RWMutex

// Global functions for backward compatibility
func SetMarshaller(marshaller MarshallerFunc) {
	globalSerializationManagerMu.Lock()
	defer globalSerializationManagerMu.Unlock()
	globalSerializationManager.SetMarshaller(marshaller)
}

func SetUnmarshaller(unmarshaller UnmarshallerFunc) {
	globalSerializationManagerMu.Lock()
	defer globalSerializationManagerMu.Unlock()
	globalSerializationManager.SetUnmarshaller(unmarshaller)
}

func EnableCompression(enable bool) {
	globalSerializationManagerMu.Lock()
	defer globalSerializationManagerMu.Unlock()
	globalSerializationManager.config.EnableCompression = enable
}

func EnableEncryption(enable bool, key []byte) error {
	globalSerializationManagerMu.Lock()
	defer globalSerializationManagerMu.Unlock()
	globalSerializationManager.config.EnableEncryption = enable
	if enable {
		return globalSerializationManager.SetEncryptionKey(key)
	}
	return nil
}

func Marshal(v any) ([]byte, error) {
	globalSerializationManagerMu.RLock()
	defer globalSerializationManagerMu.RUnlock()
	return globalSerializationManager.Marshal(v)
}

func Unmarshal(data []byte, v any) error {
	globalSerializationManagerMu.RLock()
	defer globalSerializationManagerMu.RUnlock()
	return globalSerializationManager.Unmarshal(data, v)
}
