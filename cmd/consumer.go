package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
)

type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

func main() {
	// Load consumer certificate
	cert, err := tls.LoadX509KeyPair("consumer-cert.pem", "consumer-key.pem")
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

	// Identify as consumer for the "news" topic
	_, err = conn.Write([]byte("consumer:news"))
	if err != nil {
		fmt.Println("Failed to send consumer identification:", err)
		return
	}

	// Create JSON decoder
	decoder := json.NewDecoder(conn)

	// Receive and process JSON messages
	for {
		var msg Message
		err := decoder.Decode(&msg)
		if err != nil {
			fmt.Println("Failed to decode message:", err)
			return
		}

		fmt.Println("Received:", msg.Content)
	}
}
