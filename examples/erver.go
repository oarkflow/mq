package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/oarkflow/mq/conn"
)

func main() {
	pool := conn.NewConnectionPool("localhost:8080",
		conn.WithMaxIdleConns(5),
		conn.WithMaxRetries(3),
		conn.WithRetryBackoff(200*time.Millisecond),
		conn.WithIdleTimeout(30*time.Second),
		conn.WithConnectTimeout(5*time.Second),
		conn.WithKeepAlive(true, 10*time.Minute),
		conn.WithOnConnectionInit(func(conn *conn.PooledConn) error {
			return nil
		}),
		conn.WithOnConnectionError(func(err error) {
		}),
	)
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}
	defer listener.Close()
	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleClient(clientConn, pool)
	}
}

func handleClient(clientConn net.Conn, pool *conn.ConnectionPool) {
	defer clientConn.Close()
	buffer := make([]byte, 1024)
	n, err := clientConn.Read(buffer)
	if err != nil {
		log.Printf("Error reading from client: %v", err)
		return
	}
	message := string(buffer[:n]) + " OK"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pooledConn, err := pool.GetConnection(ctx)
	if err != nil {
		log.Printf("Failed to get connection from pool: %v", err)
		return
	}
	defer pooledConn.Close()
	response := fmt.Sprintf("Server received: %s", message)
	_, err = clientConn.Write([]byte(response))
	if err != nil {
		log.Printf("Error writing to client: %v", err)
		return
	}
}
