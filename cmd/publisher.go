package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

func main() {
	// Load publisher certificate
	cert, err := tls.LoadX509KeyPair("publisher-cert.pem", "publisher-key.pem")
	if err != nil {
		fmt.Println("Failed to load client certificate:", err)
		return
	}

	// Load CA certificate
	caCert, err := os.ReadFile("ca-cert.pem")
	if err != nil {
		fmt.Println("Failed to load CA cert:", err)
		return
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Create TLS configuration
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	// Connect to the broker
	conn, err := tls.Dial("tcp", "localhost:8443", tlsConfig)
	if err != nil {
		fmt.Println("Failed to connect to broker:", err)
		return
	}
	defer conn.Close()

	// Identify as publisher for a specific topic
	_, err = conn.Write([]byte("publisher:news"))
	if err != nil {
		fmt.Println("Failed to send publisher identification:", err)
		return
	}

	// Create JSON encoder
	encoder := json.NewEncoder(conn)

	// Send 5 JSON-encoded messages to the broker
	for i := 0; i < 5; i++ {
		msg := Message{
			Topic:   "news",
			Content: fmt.Sprintf("Breaking news %d", i+1),
		}

		err := encoder.Encode(msg)
		if err != nil {
			fmt.Println("Failed to publish message:", err)
			return
		}

		fmt.Println("Published:", msg.Content)
		time.Sleep(2 * time.Second)
	}
}
