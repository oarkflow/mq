package codec

import (
	"encoding/binary"
	"net"
	"sync"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/oarkflow/mq/consts"
)

// Message represents the structure of our message.
type Message struct {
	Headers map[string]string `msgpack:"h"`
	Queue   string            `msgpack:"q"`
	Command consts.CMD        `msgpack:"c"`
	Payload []byte            `msgpack:"p"` // Using []byte instead of json.RawMessage
	m       sync.RWMutex
}

// NewMessage creates a new Message instance.
func NewMessage(cmd consts.CMD, payload []byte, queue string, headers map[string]string) *Message {
	return &Message{
		Headers: headers,
		Queue:   queue,
		Command: cmd,
		Payload: payload,
	}
}

// Serialize encodes the Message to a byte slice using MessagePack.
func (m *Message) Serialize() ([]byte, error) {
	m.m.RLock()
	defer m.m.RUnlock()

	data, err := msgpack.Marshal(m)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Deserialize decodes a byte slice to a Message instance using MessagePack.
func Deserialize(data []byte) (*Message, error) {
	var msg Message
	if err := msgpack.Unmarshal(data, &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}

// SendMessage sends a Message over a net.Conn.
func SendMessage(conn net.Conn, msg *Message) error {
	data, err := msg.Serialize()
	if err != nil {
		return err
	}

	// Send the length of the data followed by the data itself
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)))

	if _, err := conn.Write(length); err != nil {
		return err
	}
	if _, err := conn.Write(data); err != nil {
		return err
	}
	return nil
}

// ReadMessage receives a Message from a net.Conn.
func ReadMessage(conn net.Conn) (*Message, error) {
	// Read the length of the incoming message
	lengthBytes := make([]byte, 4)
	if _, err := conn.Read(lengthBytes); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBytes)

	// Read the actual message data
	data := make([]byte, length)
	if _, err := conn.Read(data); err != nil {
		return nil, err
	}

	return Deserialize(data)
}
