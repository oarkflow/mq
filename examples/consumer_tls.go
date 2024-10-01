package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/examples/tasks"
)

func main() {
	// Load consumer's certificate and private key
	cert, err := tls.LoadX509KeyPair("consumer.crt", "consumer.key")
	if err != nil {
		log.Fatalf("Failed to load consumer certificate and key: %v", err)
	}

	// Load the CA certificate
	caCert, err := ioutil.ReadFile("ca.crt")
	if err != nil {
		log.Fatalf("Failed to read CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Configure TLS for the consumer
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

	consumer := mq.NewConsumer("consumer-1")
	consumer.RegisterHandler("queue1", tasks.Node1)
	consumer.RegisterHandler("queue2", tasks.Node2)

	// Start consuming tasks
	consumer.Consume(context.Background())
}
