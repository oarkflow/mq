package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/oarkflow/mq/consts"
)

type Message struct {
	Headers map[string]string `json:"h"`
	Queue   string            `json:"q"`
	Command consts.CMD        `json:"c"`
	Payload json.RawMessage   `json:"p"`
	// Metadata map[string]any    `json:"m"`
}

func NewMessage(cmd consts.CMD, payload json.RawMessage, queue string, headers map[string]string) *Message {
	return &Message{
		Headers: headers,
		Queue:   queue,
		Command: cmd,
		Payload: payload,
		// Metadata: nil,
	}
}

func (m *Message) Serialize(aesKey, hmacKey []byte, encrypt bool) ([]byte, string, error) {
	var buf bytes.Buffer

	// Serialize Headers, Topic, Command, Payload, and Metadata
	if err := writeLengthPrefixedJSON(&buf, m.Headers); err != nil {
		return nil, "", fmt.Errorf("error serializing headers: %v", err)
	}
	if err := writeLengthPrefixed(&buf, []byte(m.Queue)); err != nil {
		return nil, "", fmt.Errorf("error serializing topic: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, m.Command); err != nil {
		return nil, "", fmt.Errorf("error serializing command: %v", err)
	}
	if err := writePayload(&buf, aesKey, m.Payload, encrypt); err != nil {
		return nil, "", err
	}
	/*if err := writeLengthPrefixedJSON(&buf, m.Metadata); err != nil {
		return nil, "", fmt.Errorf("error serializing metadata: %v", err)
	}*/

	// Calculate HMAC
	messageBytes := buf.Bytes()
	hmacSignature := CalculateHMAC(hmacKey, messageBytes)

	return messageBytes, hmacSignature, nil
}

func Deserialize(data, aesKey, hmacKey []byte, receivedHMAC string, decrypt bool) (*Message, error) {
	if !VerifyHMAC(hmacKey, data, receivedHMAC) {
		return nil, fmt.Errorf("HMAC verification failed")
	}

	buf := bytes.NewReader(data)

	// Deserialize Headers, Topic, Command, Payload, and Metadata
	headers := make(map[string]string)
	if err := readLengthPrefixedJSON(buf, &headers); err != nil {
		return nil, fmt.Errorf("error deserializing headers: %v", err)
	}

	topic, err := readLengthPrefixedString(buf)
	if err != nil {
		return nil, fmt.Errorf("error deserializing topic: %v", err)
	}

	var command consts.CMD
	if err := binary.Read(buf, binary.LittleEndian, &command); err != nil {
		return nil, fmt.Errorf("error deserializing command: %v", err)
	}

	payload, err := readPayload(buf, aesKey, decrypt)
	if err != nil {
		return nil, fmt.Errorf("error deserializing payload: %v", err)
	}

	/*metadata := make(map[string]any)
	if err := readLengthPrefixedJSON(buf, &metadata); err != nil {
		return nil, fmt.Errorf("error deserializing metadata: %v", err)
	}*/

	return &Message{
		Headers: headers,
		Queue:   topic,
		Command: command,
		Payload: payload,
		// Metadata: metadata,
	}, nil
}

func SendMessage(conn io.Writer, msg *Message, aesKey, hmacKey []byte, encrypt bool) error {
	sentData, hmacSignature, err := msg.Serialize(aesKey, hmacKey, encrypt)
	if err != nil {
		return fmt.Errorf("error serializing message: %v", err)
	}

	if err := writeMessageWithHMAC(conn, sentData, hmacSignature); err != nil {
		return fmt.Errorf("error sending message: %v", err)
	}

	return nil
}

func ReadMessage(conn io.Reader, aesKey, hmacKey []byte, decrypt bool) (*Message, error) {
	data, receivedHMAC, err := readMessageWithHMAC(conn)
	if err != nil {
		return nil, err
	}

	return Deserialize(data, aesKey, hmacKey, receivedHMAC, decrypt)
}

func writeLengthPrefixed(buf *bytes.Buffer, data []byte) error {
	length := uint32(len(data))
	if err := binary.Write(buf, binary.LittleEndian, length); err != nil {
		return err
	}
	buf.Write(data)
	return nil
}

func writeLengthPrefixedJSON(buf *bytes.Buffer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeLengthPrefixed(buf, data)
}

func readLengthPrefixed(r *bytes.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	if _, err := r.Read(data); err != nil {
		return nil, err
	}
	return data, nil
}

func readLengthPrefixedJSON(r *bytes.Reader, v any) error {
	data, err := readLengthPrefixed(r)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func readLengthPrefixedString(r *bytes.Reader) (string, error) {
	data, err := readLengthPrefixed(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func writePayload(buf *bytes.Buffer, aesKey []byte, payload json.RawMessage, encrypt bool) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshalling payload: %v", err)
	}

	var encryptedPayload, nonce []byte
	if encrypt {
		encryptedPayload, nonce, err = EncryptPayload(aesKey, payloadBytes)
		if err != nil {
			return err
		}
	} else {
		encryptedPayload = payloadBytes
	}

	if err := writeLengthPrefixed(buf, encryptedPayload); err != nil {
		return err
	}

	if encrypt {
		buf.Write(nonce)
	}
	return nil
}

func readPayload(r *bytes.Reader, aesKey []byte, decrypt bool) (json.RawMessage, error) {
	encryptedPayload, err := readLengthPrefixed(r)
	if err != nil {
		return nil, err
	}

	var payloadBytes []byte
	if decrypt {
		nonce := make([]byte, 12)
		if _, err := r.Read(nonce); err != nil {
			return nil, err
		}
		payloadBytes, err = DecryptPayload(aesKey, encryptedPayload, nonce)
		if err != nil {
			return nil, fmt.Errorf("error decrypting payload: %v", err)
		}
	} else {
		payloadBytes = encryptedPayload
	}

	var payload json.RawMessage
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("error unmarshalling payload: %v", err)
	}

	return payload, nil
}
func writeMessageWithHMAC(conn io.Writer, messageBytes []byte, hmacSignature string) error {
	if err := binary.Write(conn, binary.LittleEndian, uint32(len(messageBytes))); err != nil {
		return err
	}
	if _, err := conn.Write(messageBytes); err != nil {
		return err
	}
	if _, err := conn.Write([]byte(hmacSignature)); err != nil {
		return err
	}
	return nil
}

func readMessageWithHMAC(conn io.Reader) ([]byte, string, error) {
	var length uint32
	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		return nil, "", err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, "", err
	}

	hmacBytes := make([]byte, 64)
	if _, err := io.ReadFull(conn, hmacBytes); err != nil {
		return nil, "", err
	}
	receivedHMAC := string(hmacBytes)

	return data, receivedHMAC, nil
}
