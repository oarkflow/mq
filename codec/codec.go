package codec

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/oarkflow/mq/consts"
)

type Message struct {
	Headers map[string]string `json:"h"`
	Topic   string            `json:"t"`
	Command consts.CMD        `json:"c"`
	Payload json.RawMessage   `json:"p"`
}

func EncryptPayload(key []byte, plaintext []byte) ([]byte, []byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, aesGCM.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}
	ciphertext := aesGCM.Seal(nil, nonce, plaintext, nil)
	return ciphertext, nonce, nil
}

func DecryptPayload(key []byte, ciphertext []byte, nonce []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return aesGCM.Open(nil, nonce, ciphertext, nil)
}

func CalculateHMAC(key []byte, data []byte) string {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func VerifyHMAC(key []byte, data []byte, receivedHMAC string) bool {
	expectedHMAC := CalculateHMAC(key, data)
	return hmac.Equal([]byte(expectedHMAC), []byte(receivedHMAC))
}

func (m *Message) Serialize(aesKey []byte, hmacKey []byte) ([]byte, string, error) {
	var buf bytes.Buffer
	headersBytes, err := json.Marshal(m.Headers)
	if err != nil {
		return nil, "", fmt.Errorf("error marshalling headers: %v", err)
	}
	headersLen := uint32(len(headersBytes))
	if err := binary.Write(&buf, binary.LittleEndian, headersLen); err != nil {
		return nil, "", err
	}
	buf.Write(headersBytes)
	topicBytes := []byte(m.Topic)
	topicLen := uint8(len(topicBytes))
	if err := binary.Write(&buf, binary.LittleEndian, topicLen); err != nil {
		return nil, "", err
	}
	buf.Write(topicBytes)
	if !m.Command.IsValid() {
		return nil, "", fmt.Errorf("invalid command: %s", m.Command)
	}
	if err := binary.Write(&buf, binary.LittleEndian, m.Command); err != nil {
		return nil, "", err
	}
	payloadBytes, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, "", fmt.Errorf("error marshalling payload: %v", err)
	}
	encryptedPayload, nonce, err := EncryptPayload(aesKey, payloadBytes)
	if err != nil {
		return nil, "", err
	}
	payloadLen := uint32(len(encryptedPayload))
	if err := binary.Write(&buf, binary.LittleEndian, payloadLen); err != nil {
		return nil, "", err
	}
	buf.Write(encryptedPayload)
	buf.Write(nonce)
	messageBytes := buf.Bytes()
	hmacSignature := CalculateHMAC(hmacKey, messageBytes)
	return messageBytes, hmacSignature, nil
}

func Deserialize(data []byte, aesKey []byte, hmacKey []byte, receivedHMAC string) (*Message, error) {
	if !VerifyHMAC(hmacKey, data, receivedHMAC) {
		return nil, fmt.Errorf("HMAC verification failed")
	}
	buf := bytes.NewReader(data)
	var topicLen uint8
	var payloadLen uint32
	var headersLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &headersLen); err != nil {
		return nil, err
	}
	headersBytes := make([]byte, headersLen)
	if _, err := buf.Read(headersBytes); err != nil {
		return nil, err
	}
	var headers map[string]string
	if err := json.Unmarshal(headersBytes, &headers); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &topicLen); err != nil {
		return nil, err
	}
	topicBytes := make([]byte, topicLen)
	if _, err := buf.Read(topicBytes); err != nil {
		return nil, err
	}
	topic := string(topicBytes)
	var command consts.CMD
	if err := binary.Read(buf, binary.LittleEndian, &command); err != nil {
		return nil, err
	}
	if !command.IsValid() {
		return nil, fmt.Errorf("invalid command: %s", command)
	}
	if err := binary.Read(buf, binary.LittleEndian, &payloadLen); err != nil {
		return nil, err
	}
	encryptedPayload := make([]byte, payloadLen)
	if _, err := buf.Read(encryptedPayload); err != nil {
		return nil, err
	}
	nonce := make([]byte, 12)
	if _, err := buf.Read(nonce); err != nil {
		return nil, err
	}
	payloadBytes, err := DecryptPayload(aesKey, encryptedPayload, nonce)
	if err != nil {
		return nil, err
	}
	var payload json.RawMessage
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("error unmarshalling payload: %v", err)
	}
	return &Message{
		Headers: headers,
		Topic:   topic,
		Command: command,
		Payload: payload,
	}, nil
}

func SendMessage(conn io.Writer, msg *Message, aesKey []byte, hmacKey []byte) error {
	sentData, hmacSignature, err := msg.Serialize(aesKey, hmacKey)
	if err != nil {
		return fmt.Errorf("error serializing message: %v", err)
	}
	if err := binary.Write(conn, binary.LittleEndian, uint32(len(sentData))); err != nil {
		return err
	}

	if _, err := conn.Write(sentData); err != nil {
		return err
	}
	if _, err := conn.Write([]byte(hmacSignature)); err != nil {
		return err
	}
	return nil
}

func ReadMessage(conn io.Reader, aesKey []byte, hmacKey []byte) (*Message, error) {
	var length uint32
	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}

	hmacBytes := make([]byte, 64)
	if _, err := io.ReadFull(conn, hmacBytes); err != nil {
		return nil, err
	}
	receivedHMAC := string(hmacBytes)
	return Deserialize(data, aesKey, hmacKey, receivedHMAC)
}
