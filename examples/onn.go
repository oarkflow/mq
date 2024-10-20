package main

import (
	"context"
	"log"
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
			log.Println("Connection error:", err)
		}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pooledConn, err := pool.GetConnection(ctx)
	if err != nil {
		log.Fatalf("Failed to get connection: %v", err)
	}
	defer pooledConn.Close()
	message := "Hello, server!"
	_, err = pooledConn.Write([]byte(message))
	if err != nil {
		log.Fatalf("Failed to write to connection: %v", err)
	}
	log.Println("Message sent:", message)
	response := make([]byte, 1024)
	n, err := pooledConn.Read(response)
	if err != nil {
		log.Fatalf("Failed to read from connection: %v", err)
	}
	log.Println("Response received:", string(response[:n]))
}
