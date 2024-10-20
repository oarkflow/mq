package conn

import (
	"time"
)

type Option func(*ConnectionPool)

func WithMaxIdleConns(maxIdleConns int) Option {
	return func(cp *ConnectionPool) {
		cp.maxIdleConns = maxIdleConns
	}
}

func WithMaxRetries(maxRetries int) Option {
	return func(cp *ConnectionPool) {
		cp.maxRetries = maxRetries
	}
}

func WithRetryBackoff(retryBackoff time.Duration) Option {
	return func(cp *ConnectionPool) {
		cp.retryBackoff = retryBackoff
	}
}

func WithIdleTimeout(idleTimeout time.Duration) Option {
	return func(cp *ConnectionPool) {
		cp.idleTimeout = idleTimeout
	}
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(cp *ConnectionPool) {
		cp.connectTimeout = connectTimeout
	}
}

func WithKeepAlive(enabled bool, period time.Duration) Option {
	return func(cp *ConnectionPool) {
		cp.keepAliveEnabled = enabled
		cp.keepAlivePeriod = period
	}
}

func WithOnConnectionInit(fn func(*PooledConn) error) Option {
	return func(cp *ConnectionPool) {
		cp.onConnectionInit = fn
	}
}

func WithOnConnectionError(fn func(error)) Option {
	return func(cp *ConnectionPool) {
		cp.onConnectionError = fn
	}
}

func WithOnReceive(fn func(*PooledConn, []byte)) Option {
	return func(cp *ConnectionPool) {
		cp.onReceive = fn
	}
}

func WithOnConnect(fn func(*PooledConn)) Option {
	return func(cp *ConnectionPool) {
		cp.onConnect = fn
	}
}

func WithOnWrite(fn func(*PooledConn, []byte)) Option {
	return func(cp *ConnectionPool) {
		cp.onWrite = fn
	}
}

func WithOnClose(fn func(*PooledConn)) Option {
	return func(cp *ConnectionPool) {
		cp.onClose = fn
	}
}
