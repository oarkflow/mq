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

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
)

type MessageHandler func(context.Context, net.Conn, []byte) error

type CloseHandler func(context.Context, net.Conn) error

type ErrorHandler func(context.Context, net.Conn, error)

type Handlers struct {
	MessageHandler MessageHandler
	CloseHandler   CloseHandler
	ErrorHandler   ErrorHandler
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
	return context.WithValue(ctx, consts.HeaderKey, hd)
}

func GetHeaders(ctx context.Context) (map[string]string, bool) {
	headers, ok := ctx.Value(consts.HeaderKey).(map[string]string)
	return headers, ok
}

func GetContentType(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[consts.ContentType]
	return contentType, ok
}

func GetConsumerID(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[consts.ConsumerKey]
	return contentType, ok
}

func GetTriggerNode(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[consts.TriggerNode]
	return contentType, ok
}

func GetPublisherID(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[consts.PublisherKey]
	return contentType, ok
}

func Write(ctx context.Context, conn net.Conn, data any) error {
	msg := codec.Message{Headers: make(map[string]string)}
	if headers, ok := GetHeaders(ctx); ok {
		msg.Headers = headers
	}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	msg.Payload = dataBytes
	messageBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = conn.Write(append(messageBytes, '\n'))
	return err
}

func ReadFromConn(ctx context.Context, conn net.Conn, handlers Handlers) {
	defer func() {
		if handlers.CloseHandler != nil {
			if err := handlers.CloseHandler(ctx, conn); err != nil {
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
			if handlers.ErrorHandler != nil {
				handlers.ErrorHandler(ctx, conn, err)
			}
			continue
		}
		messageBytes = bytes.TrimSpace(messageBytes)
		if len(messageBytes) == 0 {
			continue
		}
		var msg codec.Message
		err = json.Unmarshal(messageBytes, &msg)
		if err != nil {
			if handlers.ErrorHandler != nil {
				handlers.ErrorHandler(ctx, conn, err)
			}
			continue
		}
		ctx = SetHeaders(ctx, msg.Headers)
		if handlers.MessageHandler != nil {
			err = handlers.MessageHandler(ctx, conn, msg.Payload)
			if err != nil {
				if handlers.ErrorHandler != nil {
					handlers.ErrorHandler(ctx, conn, err)
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
		ClientAuth:         tls.RequireAndVerifyClientCert,
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
		tlsConfig.ClientCAs = caCertPool
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
