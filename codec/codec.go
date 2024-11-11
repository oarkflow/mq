package codec

import (
	"bufio"
	"context"
	"encoding/binary"
	"net"

	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/internal/bpool"
)

type Message struct {
	Headers map[string]string `msgpack:"h"`
	Queue   string            `msgpack:"q"`
	Payload []byte            `msgpack:"p"`
	Command consts.CMD        `msgpack:"c"`
}

func NewMessage(cmd consts.CMD, payload []byte, queue string, headers map[string]string) *Message {
	if headers == nil {
		headers = make(map[string]string)
	}
	return &Message{
		Headers: headers,
		Queue:   queue,
		Command: cmd,
		Payload: payload,
	}
}

func (m *Message) Serialize() ([]byte, error) {
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

func SendMessage(ctx context.Context, conn net.Conn, msg *Message) error {
	data, err := msg.Serialize()
	if err != nil {
		return err
	}
	totalLength := 4 + len(data)
	buffer := bpool.Get()
	defer bpool.Put(buffer)
	buffer.Reset()
	if cap(buffer.B) < totalLength {
		buffer.B = make([]byte, totalLength)
	} else {
		buffer.B = buffer.B[:totalLength]
	}
	binary.BigEndian.PutUint32(buffer.B[:4], uint32(len(data)))
	copy(buffer.B[4:], data)
	writer := bufio.NewWriter(conn)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if _, err := writer.Write(buffer.B[:totalLength]); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func ReadMessage(ctx context.Context, conn net.Conn) (*Message, error) {
	lengthBytes := make([]byte, 4)
	if _, err := conn.Read(lengthBytes); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBytes)
	buffer := bpool.Get()
	defer bpool.Put(buffer)
	buffer.Reset()
	if cap(buffer.B) < int(length) {
		buffer.B = make([]byte, length)
	} else {
		buffer.B = buffer.B[:length]
	}
	totalRead := 0
	for totalRead < int(length) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			n, err := conn.Read(buffer.B[totalRead:])
			if err != nil {
				return nil, err
			}
			totalRead += n
		}
	}
	return Deserialize(buffer.B[:length])
}
