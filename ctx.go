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
} // HeaderMap wraps a map and a mutex for thread-safe access
type HeaderMap struct {
	mu      sync.RWMutex
	headers map[string]string
}

// NewHeaderMap initializes a new HeaderMap
func NewHeaderMap() *HeaderMap {
	return &HeaderMap{
		headers: make(map[string]string),
	}
}

func SetHeaders(ctx context.Context, headers map[string]string) context.Context {
	hd, _ := GetHeaders(ctx)
	if hd == nil {
		hd = NewHeaderMap()
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()
	for key, val := range headers {
		hd.headers[key] = val
	}
	return context.WithValue(ctx, consts.HeaderKey, hd)
}

func WithHeaders(ctx context.Context, headers map[string]string) map[string]string {
	hd, _ := GetHeaders(ctx)
	if hd == nil {
		hd = NewHeaderMap()
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()
	for key, val := range headers {
		hd.headers[key] = val
	}
	return getMapAsRegularMap(hd)
}

func GetHeaders(ctx context.Context) (*HeaderMap, bool) {
	headers, ok := ctx.Value(consts.HeaderKey).(*HeaderMap)
	return headers, ok
}

func GetHeader(ctx context.Context, key string) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	headers.mu.RLock()
	defer headers.mu.RUnlock()
	val, ok := headers.headers[key]
	return val, ok
}

func GetContentType(ctx context.Context) (string, bool) {
	return GetHeader(ctx, consts.ContentType)
}

func GetQueue(ctx context.Context) (string, bool) {
	return GetHeader(ctx, consts.QueueKey)
}

func GetConsumerID(ctx context.Context) (string, bool) {
	return GetHeader(ctx, consts.ConsumerKey)
}

func GetTriggerNode(ctx context.Context) (string, bool) {
	return GetHeader(ctx, consts.TriggerNode)
}

func GetAwaitResponse(ctx context.Context) (string, bool) {
	return GetHeader(ctx, consts.AwaitResponseKey)
}

func GetPublisherID(ctx context.Context) (string, bool) {
	return GetHeader(ctx, consts.PublisherKey)
}

// Helper function to convert HeaderMap to a regular map
func getMapAsRegularMap(hd *HeaderMap) map[string]string {
	result := make(map[string]string)
	hd.mu.RLock()
	defer hd.mu.RUnlock()
	for key, value := range hd.headers {
		result[key] = value
	}
	return result
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
