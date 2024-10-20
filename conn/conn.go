package conn

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ConnectionPool struct {
	pool              chan *PooledConn
	idleConns         sync.Map
	maxIdleConns      int
	maxRetries        int
	retryBackoff      time.Duration
	idleTimeout       time.Duration
	serverAddress     string
	shutdown          int32
	wg                sync.WaitGroup
	connectTimeout    time.Duration
	keepAliveEnabled  bool
	keepAlivePeriod   time.Duration
	metrics           *PoolMetrics
	onConnectionInit  func(*PooledConn) error
	onConnectionError func(error)
	onReceive         func(*PooledConn, []byte)
	onConnect         func(*PooledConn)
	onWrite           func(*PooledConn, []byte)
	onError           func(*PooledConn, error)
	onClose           func(*PooledConn)
}

type PoolMetrics struct {
	TotalConnections  int
	FailedAttempts    int
	IdleConnections   int
	ActiveConnections int
}

type PooledConn struct {
	net.Conn
	pool     *ConnectionPool
	isAlive  bool
	lastUsed time.Time
}

func NewConnectionPool(serverAddress string, opts ...Option) *ConnectionPool {
	cp := &ConnectionPool{
		pool:          make(chan *PooledConn, 10),
		serverAddress: serverAddress,
		metrics:       &PoolMetrics{},
	}
	for _, opt := range opts {
		opt(cp)
	}
	return cp
}

func (cp *ConnectionPool) GetConnection(ctx context.Context) (*PooledConn, error) {
	if cp.isShutdown() {
		return nil, errors.New("connection pool is shut down")
	}
	select {
	case pooledConn := <-cp.pool:
		if cp.isConnectionAlive(pooledConn) {
			log.Println("Reusing connection from pool")
			return pooledConn, nil
		}
		log.Println("Stale connection found, closing")
		pooledConn.Close()
	default:
	}
	conn, err := cp.createConnection(ctx)
	if err != nil {
		cp.metrics.FailedAttempts++
		if cp.onConnectionError != nil {
			cp.onConnectionError(err)
		}
		return nil, err
	}
	cp.metrics.TotalConnections++
	cp.metrics.ActiveConnections++
	if cp.onConnect != nil {
		cp.onConnect(conn)
	}
	return conn, nil
}

func (cp *ConnectionPool) createConnection(ctx context.Context) (*PooledConn, error) {
	var conn net.Conn
	var err error
	backoff := cp.retryBackoff
	for attempt := 0; attempt < cp.maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		conn, err = net.DialTimeout("tcp", cp.serverAddress, cp.connectTimeout)
		if err == nil {
			log.Printf("Successfully connected to %s", cp.serverAddress)
			if cp.keepAliveEnabled {
				if tcpConn, ok := conn.(*net.TCPConn); ok {
					tcpConn.SetKeepAlive(true)
					tcpConn.SetKeepAlivePeriod(cp.keepAlivePeriod)
				}
			}
			pooledConn := &PooledConn{Conn: conn, pool: cp, isAlive: true, lastUsed: time.Now()}
			if cp.onConnectionInit != nil {
				if initErr := cp.onConnectionInit(pooledConn); initErr != nil {
					conn.Close()
					return nil, initErr
				}
			}
			return pooledConn, nil
		}
		log.Printf("Failed to connect (attempt %d/%d): %v", attempt+1, cp.maxRetries, err)
		time.Sleep(backoff)
		backoff *= 2
	}
	return nil, fmt.Errorf("failed to connect to %s after %d attempts", cp.serverAddress, cp.maxRetries)
}

func (cp *ConnectionPool) ReleaseConnection(conn *PooledConn) {
	if cp.isShutdown() {
		conn.Close()
		return
	}
	if len(cp.pool) < cp.maxIdleConns && cp.isConnectionAlive(conn) {
		conn.lastUsed = time.Now() // Update last used time
		cp.idleConns.Store(conn, conn.lastUsed)
		cp.pool <- conn
		cp.metrics.IdleConnections = len(cp.pool)
		cp.metrics.ActiveConnections--
		log.Println("Connection returned to pool")
	} else {
		log.Println("Closing connection as it's not idle or not alive")
		conn.Close()
	}
}

func (cp *ConnectionPool) CleanIdleConnections() {
	now := time.Now()
	cp.idleConns.Range(func(key, value interface{}) bool {
		conn := key.(*PooledConn)
		lastUsed := value.(time.Time)
		if now.Sub(lastUsed) > cp.idleTimeout {
			conn.Close()
			cp.idleConns.Delete(key)
			cp.metrics.IdleConnections = len(cp.pool) // Update metrics after cleanup
			log.Println("Closed idle connection due to timeout")
		}
		return true
	})
}

func (cp *ConnectionPool) Shutdown() {
	if !atomic.CompareAndSwapInt32(&cp.shutdown, 0, 1) {
		return
	}
	close(cp.pool)
	cp.CleanIdleConnections()
	log.Println("Connection pool shutdown complete")
	cp.wg.Wait()
}

func (cp *ConnectionPool) isConnectionAlive(conn *PooledConn) bool {
	if conn == nil || conn.Conn == nil {
		return false
	}

	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	one := make([]byte, 1)
	_, err := conn.Read(one)
	if err == nil {
		conn.lastUsed = time.Now() // Update last used time
		return true
	}

	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			log.Println("Connection alive: timeout but no closure")
			return true
		}
	}

	log.Printf("Connection dead: %v", err)
	conn.isAlive = false
	cp.metrics.ActiveConnections--
	return false
}

func (p *PooledConn) Close() error {
	if !p.isAlive {
		log.Println("Attempted to close an already dead connection")
		return nil
	}
	p.isAlive = false
	if p.pool.onClose != nil {
		p.pool.onClose(p)
	}
	p.pool.ReleaseConnection(p)
	return nil
}

func (p *PooledConn) Read(b []byte) (n int, err error) {
	if !p.isAlive {
		log.Println("Attempted to read from a closed connection")
		return 0, errors.New("connection is closed")
	}
	n, err = p.Conn.Read(b)
	if err != nil {
		p.isAlive = false // Mark as dead if an error occurs
		log.Printf("Read error: %v", err)
		if p.pool.onError != nil {
			p.pool.onError(p, err)
		}
	}
	return
}

func (p *PooledConn) Write(b []byte) (n int, err error) {
	if !p.isAlive {
		log.Println("Attempted to write to a closed connection")
		return 0, errors.New("connection is closed")
	}

	n, err = p.Conn.Write(b)
	if err != nil {
		p.isAlive = false // Mark as dead if an error occurs
		log.Printf("Write error: %v", err)
		if p.pool.onError != nil {
			p.pool.onError(p, err)
		}
	}
	return n, err
}

func (cp *ConnectionPool) WriteToConnection(conn *PooledConn, data []byte) error {
	n, err := conn.Write(data)
	if err != nil {
		if errors.Is(err, errors.New("connection is closed")) {
			log.Println("Connection is closed; trying to retrieve a new connection")

			newConn, err := cp.GetConnection(context.Background())
			if err != nil {
				return fmt.Errorf("failed to get a new connection: %w", err)
			}
			defer cp.ReleaseConnection(newConn)

			_, err = newConn.Write(data)
			return err
		}
		return err
	}

	log.Printf("Wrote %d bytes to connection", n)
	if cp.onWrite != nil {
		cp.onWrite(conn, data)
	}
	return nil
}

func (p *PooledConn) LocalAddr() net.Addr {
	return p.Conn.LocalAddr()
}

func (p *PooledConn) RemoteAddr() net.Addr {
	return p.Conn.RemoteAddr()
}

func (p *PooledConn) SetDeadline(t time.Time) error {
	return p.Conn.SetDeadline(t)
}

func (p *PooledConn) SetReadDeadline(t time.Time) error {
	return p.Conn.SetReadDeadline(t)
}

func (p *PooledConn) SetWriteDeadline(t time.Time) error {
	return p.Conn.SetWriteDeadline(t)
}

func (cp *ConnectionPool) isShutdown() bool {
	return atomic.LoadInt32(&cp.shutdown) != 0
}

func (cp *ConnectionPool) GetMetrics() *PoolMetrics {
	return cp.metrics
}
