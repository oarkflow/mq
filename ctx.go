package mq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/mq/consts"
)

type Task struct {
	ID          string            `json:"id"`
	Results     map[string]Result `json:"results"`
	Topic       string            `json:"topic"`
	Payload     json.RawMessage   `json:"payload"`
	CreatedAt   time.Time         `json:"created_at"`
	ProcessedAt time.Time         `json:"processed_at"`
	Status      string            `json:"status"`
	Error       error             `json:"error"`
}

type Handler func(context.Context, *Task) Result

func IsClosed(conn net.Conn) bool {
	_, err := conn.Read(make([]byte, 1))
	if err != nil {
		if err == net.ErrClosed {
			return true
		}
	}
	return false
}

var m = sync.RWMutex{}

func SetHeaders(ctx context.Context, headers map[string]string) context.Context {
	m.Lock()
	defer m.Unlock()
	hd, ok := GetHeaders(ctx)
	if !ok {
		hd = make(map[string]string)
	}
	for key, val := range headers {
		hd[key] = val
	}
	return context.WithValue(ctx, consts.HeaderKey, hd)
}

func WithHeaders(ctx context.Context, headers map[string]string) map[string]string {
	hd, ok := GetHeaders(ctx)
	if !ok {
		hd = make(map[string]string)
	}
	for key, val := range headers {
		hd[key] = val
	}
	return hd
}

func GetHeaders(ctx context.Context) (map[string]string, bool) {
	headers, ok := ctx.Value(consts.HeaderKey).(map[string]string)
	return headers, ok
}

func GetHeader(ctx context.Context, key string) (string, bool) {
	headers, ok := ctx.Value(consts.HeaderKey).(map[string]string)
	if !ok {
		return "", false
	}
	val, ok := headers[key]
	return val, ok
}

func GetContentType(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[consts.ContentType]
	return contentType, ok
}

func GetQueue(ctx context.Context) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	contentType, ok := headers[consts.QueueKey]
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
