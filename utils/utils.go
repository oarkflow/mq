package utils

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
)

type Message struct {
	Headers map[string]string `json:"headers"`
	Data    json.RawMessage   `json:"data"`
}

func IsClosed(conn net.Conn) bool {
	_, err := conn.Read(make([]byte, 1))
	if err != nil {
		if err == net.ErrClosed {
			return true
		}
	}
	return false
}

func SetHeadersToContext(ctx context.Context, headers map[string]string) context.Context {
	return context.WithValue(ctx, "headers", headers)
}

func GetHeadersFromContext(ctx context.Context) (map[string]string, bool) {
	headers, ok := ctx.Value("headers").(map[string]string)
	return headers, ok
}

func Write(ctx context.Context, conn net.Conn, data any) error {
	msg := Message{Headers: make(map[string]string)}
	if headers, ok := GetHeadersFromContext(ctx); ok {
		msg.Headers = headers
	}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg.Data = dataBytes
	messageBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = conn.Write(append(messageBytes, '\n'))
	return err
}

type MessageHandler func(context.Context, net.Conn, []byte) error

func ReadFromConn(ctx context.Context, conn net.Conn, handler MessageHandler) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		messageBytes, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF || IsClosed(conn) || strings.Contains(err.Error(), "closed network connection") {
				break
			}
			fmt.Println("Error reading message:", err)
			continue
		}
		messageBytes = bytes.TrimSpace(messageBytes)
		if len(messageBytes) == 0 {
			continue
		}
		var msg Message
		err = json.Unmarshal(messageBytes, &msg)
		if err != nil {
			fmt.Println("Error unmarshalling message:", err)
			continue
		}
		ctx = SetHeadersToContext(ctx, msg.Headers)
		if handler != nil {
			err = handler(ctx, conn, msg.Data)
			if err != nil {
				fmt.Println("Error handling message:", err)
				continue
			}
		}
	}
}
