package codec

import (
	"encoding/binary"
	"net"
	"sync"

	"github.com/oarkflow/mq/consts"
)

type Message struct {
	Headers map[string]string `msgpack:"h"`
	Queue   string            `msgpack:"q"`
	Command consts.CMD        `msgpack:"c"`
	Payload []byte            `msgpack:"p"`
	m       sync.RWMutex
}

func NewMessage(cmd consts.CMD, payload []byte, queue string, headers map[string]string) *Message {
	return &Message{
		Headers: headers,
		Queue:   queue,
		Command: cmd,
		Payload: payload,
	}
}

func (m *Message) Serialize() ([]byte, error) {
	m.m.RLock()
	defer m.m.RUnlock()
	data, err := Marshal(m)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func Deserialize(data []byte) (*Message, error) {
	var msg Message
	if err := Unmarshal(data, &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}

func SendMessage(conn net.Conn, msg *Message) error {
	data, err := msg.Serialize()
	if err != nil {
		return err
	}

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

func ReadMessage(conn net.Conn) (*Message, error) {
	lengthBytes := make([]byte, 4)
	if _, err := conn.Read(lengthBytes); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBytes)
	data := make([]byte, length)
	if _, err := conn.Read(data); err != nil {
		return nil, err
	}
	return Deserialize(data)
}
