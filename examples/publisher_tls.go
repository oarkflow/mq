package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/oarkflow/mq"
)

func main() {
	// Load publisher's certificate and private key
	cert, err := tls.LoadX509KeyPair("publisher.crt", "publisher.key")
	if err != nil {
		log.Fatalf("Failed to load publisher certificate and key: %v", err)
	}

	// Load the CA certificate
	caCert, err := ioutil.ReadFile("ca.crt")
	if err != nil {
		log.Fatalf("Failed to read CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Configure TLS for the publisher
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: false, // Ensure we verify the server certificate
	}

	// Dial TLS connection to the broker
	conn, err := tls.Dial("tcp", "localhost:8443", tlsConfig)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	payload := []byte(`{"message":"Message Publisher \n Task"}`)
	task := mq.Task{
		Payload: payload,
	}

	publisher := mq.NewPublisher("publish-1")
	err = publisher.Publish(context.Background(), "queue1", task)
	if err != nil {
		log.Fatalf("Failed to publish task: %v", err)
	}
	fmt.Println("Async task published successfully")

	// Example for request (sync)
	payload = []byte(`{"message":"Fire-and-Forget \n Task"}`)
	task = mq.Task{
		Payload: payload,
	}
	result, err := publisher.Request(context.Background(), "queue1", task)
	if err != nil {
		log.Fatalf("Failed to send sync request: %v", err)
	}
	fmt.Printf("Sync task published. Result: %v\n", string(result.Payload))
}
