# Message Queue Codec

This package provides a robust, production-ready codec implementation for serializing, transmitting, and deserializing messages in a distributed messaging system.

## Features

- **Message Validation**: Comprehensive validation of message format and content
- **Efficient Serialization**: Pluggable serialization with JSON as default
- **Compression**: Optional payload compression for large messages
- **Encryption**: Optional message encryption for sensitive data
- **Large Message Support**: Automatic fragmentation and reassembly of large messages
- **Connection Health**: Heartbeat mechanism for connection monitoring
- **Performance Optimized**: Buffer pooling, efficient memory usage
- **Robust Error Handling**: Detailed error types and error wrapping
- **Timeout Management**: Context-aware deadline handling
- **Observability**: Built-in statistics tracking

## Usage

### Basic Message Sending/Receiving

```go
// Create a message
msg, err := codec.NewMessage(consts.CmdPublish, payload, "my-queue", headers)
if err != nil {
    log.Fatalf("Failed to create message: %v", err)
}

// Send the message
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

codec := codec.NewCodec(codec.DefaultConfig())
if err := codec.SendMessage(ctx, conn, msg); err != nil {
    log.Fatalf("Failed to send message: %v", err)
}

// Receive a message
ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

receivedMsg, err := codec.ReadMessage(ctx, conn)
if err != nil {
    log.Fatalf("Failed to receive message: %v", err)
}
```

### Custom Serialization

```go
// Set a custom marshaller/unmarshaller
codec.SetMarshaller(func(v any) ([]byte, error) {
    // Custom serialization logic
    return someCustomSerializer.Marshal(v)
})

codec.SetUnmarshaller(func(data []byte, v any) error {
    // Custom deserialization logic
    return someCustomSerializer.Unmarshal(data, v)
})
```

### Enabling Compression

```go
// Enable compression globally
codec.EnableCompression(true)

// Or configure it per codec instance
config := codec.DefaultConfig()
config.EnableCompression = true
codec := codec.NewCodec(config)
```

### Enabling Encryption

```go
// Generate a secure key
key := make([]byte, 32) // 256-bit key
if _, err := rand.Read(key); err != nil {
    log.Fatalf("Failed to generate key: %v", err)
}

// Enable encryption globally
codec.EnableEncryption(true, key)

// Or configure it per serialization manager
config := codec.DefaultSerializationConfig()
config.EnableEncryption = true
config.EncryptionKey = key
config.PreferredCipher = "chacha20poly1305" // or "aes-gcm"
```

### Connection Health Monitoring

```go
// Create a heartbeat manager
codec := codec.NewCodec(codec.DefaultConfig())
hm := codec.NewHeartbeatManager(codec, conn)

// Configure heartbeat
hm.SetInterval(15 * time.Second)
hm.SetTimeout(45 * time.Second)
hm.SetOnFailure(func(err error) {
    log.Printf("Heartbeat failure: %v", err)
    // Take action like closing connection
})

// Start heartbeat monitoring
hm.Start()
defer hm.Stop()
```

## Configuration

The codec behavior can be customized through the `Config` struct:

```go
config := &codec.Config{
    MaxMessageSize:    32 * 1024 * 1024, // 32MB max message size
    MaxHeaderSize:     64 * 1024,       // 64KB max header size
    MaxQueueLength:    128,             // Max queue name length
    ReadTimeout:       15 * time.Second,
    WriteTimeout:      10 * time.Second,
    EnableCompression: true,
    BufferPoolSize:    2000,
}

codec := codec.NewCodec(config)
```

## Error Handling

The codec provides detailed error types for different failure scenarios:

- `ErrMessageTooLarge`: Message exceeds maximum size
- `ErrInvalidMessage`: Invalid message format
- `ErrInvalidQueue`: Invalid queue name
- `ErrInvalidCommand`: Invalid command
- `ErrConnectionClosed`: Connection closed
- `ErrTimeout`: Operation timeout
- `ErrProtocolMismatch`: Protocol version mismatch
- `ErrFragmentationRequired`: Message requires fragmentation
- `ErrInvalidFragment`: Invalid message fragment
- `ErrFragmentTimeout`: Timed out waiting for fragments
- `ErrFragmentMissing`: Missing fragments in sequence

Error handling example:

```go
if err := codec.SendMessage(ctx, conn, msg); err != nil {
    if errors.Is(err, codec.ErrMessageTooLarge) {
        // Handle message size error
    } else if errors.Is(err, codec.ErrTimeout) {
        // Handle timeout error
    } else {
        // Handle other errors
    }
}
```

## Testing

The codec package includes testing utilities for validation and performance testing:

```go
ts := codec.NewCodecTestSuite()

// Test basic message sending/receiving
msg, _ := codec.NewMessage(consts.CmdPublish, []byte("test"), "test-queue", nil)
if err := ts.SendReceiveTest(msg); err != nil {
    log.Fatalf("Test failed: %v", err)
}

// Test fragmentation/reassembly
largePayload := make([]byte, 20*1024*1024) // 20MB payload
rand.Read(largePayload)                   // Fill with random data
if err := ts.FragmentationTest(largePayload); err != nil {
    log.Fatalf("Fragmentation test failed: %v", err)
}
```
