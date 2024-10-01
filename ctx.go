package mq

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/oarkflow/xid"
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

func SetHeaders(ctx context.Context, headers map[string]string) context.Context {
	hd, ok := GetHeaders(ctx)
	if !ok {
		hd = make(map[string]string)
	}
	for key, val := range headers {
		hd[key] = val
	}
	return context.WithValue(ctx, HeaderKey, hd)
}

func GetHeaders(ctx context.Context) (map[string]string, bool) {
	headers, ok := ctx.Value(HeaderKey).(map[string]string)
	return headers, ok
}

func GetContentType(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[ContentType]
	return contentType, ok
}

func GetConsumerID(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[ConsumerKey]
	return contentType, ok
}

func GetTriggerNode(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[TriggerNode]
	return contentType, ok
}

func GetPublisherID(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[PublisherKey]
	return contentType, ok
}

func Write(ctx context.Context, conn net.Conn, data any) error {
	msg := Message{Headers: make(map[string]string)}
	if headers, ok := GetHeaders(ctx); ok {
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

type CloseHandler func(context.Context, net.Conn) error

type ErrorHandler func(context.Context, net.Conn, error)

func ReadFromConn(ctx context.Context, conn net.Conn, handler MessageHandler, closeHandler CloseHandler, errorHandler ErrorHandler) {
	defer func() {
		if closeHandler != nil {
			if err := closeHandler(ctx, conn); err != nil {
				fmt.Println("Error in close handler:", err)
			}
		}
		conn.Close()
	}()
	reader := bufio.NewReader(conn)
	for {
		messageBytes, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF || IsClosed(conn) || strings.Contains(err.Error(), "closed network connection") {
				break
			}
			if errorHandler != nil {
				errorHandler(ctx, conn, err)
			}
			continue
		}
		messageBytes = bytes.TrimSpace(messageBytes)
		if len(messageBytes) == 0 {
			continue
		}
		var msg Message
		err = json.Unmarshal(messageBytes, &msg)
		if err != nil {
			if errorHandler != nil {
				errorHandler(ctx, conn, err)
			}
			continue
		}
		ctx = SetHeaders(ctx, msg.Headers)
		if handler != nil {
			err = handler(ctx, conn, msg.Data)
			if err != nil {
				if errorHandler != nil {
					errorHandler(ctx, conn, err)
				}
				continue
			}
		}
	}
}

func NewID() string {
	return xid.New().String()
}

func createTLSConnection(addr, certPath, keyPath string, caPath ...string) (net.Conn, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client cert/key: %w", err)
	}
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	if len(caPath) > 0 && caPath[0] != "" {
		caCert, err := os.ReadFile(caPath[0])
		if err != nil {
			return nil, fmt.Errorf("failed to load CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial TLS connection: %w", err)
	}

	return conn, nil
}

func GetConnection(addr string, config TLSConfig) (net.Conn, error) {
	if config.UseTLS {
		return createTLSConnection(addr, config.CertPath, config.KeyPath, config.CAPath)
	} else {
		return net.Dial("tcp", addr)
	}
}
