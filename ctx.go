package mq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"github.com/oarkflow/errors"
	"github.com/oarkflow/xid"

	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/storage"
	"github.com/oarkflow/mq/storage/memory"
)

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

func SetHeaders(ctx context.Context, headers map[string]string) context.Context {
	hd, _ := GetHeaders(ctx)
	if hd == nil {
		hd = memory.New[string, string]()
	}
	for key, val := range headers {
		hd.Set(key, val)
	}
	return context.WithValue(ctx, consts.HeaderKey, hd)
}

func WithHeaders(ctx context.Context, headers map[string]string) map[string]string {
	hd, _ := GetHeaders(ctx)
	if hd == nil {
		hd = memory.New[string, string]()
	}
	for key, val := range headers {
		hd.Set(key, val)
	}
	return hd.AsMap()
}

func GetHeaders(ctx context.Context) (storage.IMap[string, string], bool) {
	headers, ok := ctx.Value(consts.HeaderKey).(storage.IMap[string, string])
	return headers, ok
}

func GetHeader(ctx context.Context, key string) (string, bool) {
	headers, ok := GetHeaders(ctx)
	if !ok {
		return "", false
	}
	val, ok := headers.Get(key)
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

func WrapError(err error, msg, op string) error {
	return errors.Wrap(err, msg, op)
}
