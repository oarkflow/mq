package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"
)

// Message structure representing a published message
type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

// Broker struct to manage publishers, consumers, and topics
type Broker struct {
	consumers   map[string][]chan Message // topic -> consumers
	topicsMutex sync.RWMutex              // mutex to manage topic registration
}

// NewBroker initializes a new message broker
func NewBroker() *Broker {
	return &Broker{
		consumers: make(map[string][]chan Message),
	}
}

// Publish a message to a topic
func (b *Broker) Publish(msg Message) {
	b.topicsMutex.RLock()
	defer b.topicsMutex.RUnlock()

	if chans, ok := b.consumers[msg.Topic]; ok {
		for _, ch := range chans {
			// Send the message to each subscribed consumer
			go func(c chan Message) {
				c <- msg
			}(ch)
		}
	}
}

// Subscribe to a topic, returns a channel to receive messages
func (b *Broker) Subscribe(topic string) chan Message {
	b.topicsMutex.Lock()
	defer b.topicsMutex.Unlock()

	msgChan := make(chan Message, 100) // buffered channel for message queue
	b.consumers[topic] = append(b.consumers[topic], msgChan)
	return msgChan
}

// createTLSServer starts a TLS listener
func createTLSServer(certPath, keyPath, caPath string) (*net.Listener, error) {
	// Load server certificate
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert/key: %w", err)
	}

	// Load CA certificate for client verification
	caCert, err := ioutil.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA cert: %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Create TLS config
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert, // Require client certificates
	}

	// Start TLS listener
	ln, err := tls.Listen("tcp", ":8443", tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to start TLS listener: %w", err)
	}

	return &ln, nil
}

// handlePublisher handles incoming messages from a publisher
func handlePublisher(conn net.Conn, broker *Broker) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)

	for {
		var msg Message
		err := decoder.Decode(&msg)
		if err != nil {
			fmt.Println("Failed to decode message from publisher:", err)
			return
		}

		broker.Publish(msg)
	}
}

// handleConsumer manages the subscription and workers for a consumer
func handleConsumer(conn net.Conn, broker *Broker, topic string, numWorkers int) {
	defer conn.Close()

	// Subscribe to the topic
	msgChan := broker.Subscribe(topic)

	encoder := json.NewEncoder(conn)

	// Worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(&wg, msgChan, encoder)
	}

	// Wait for workers to finish
	wg.Wait()
}

// worker function that processes messages for consumers
func worker(wg *sync.WaitGroup, msgChan chan Message, encoder *json.Encoder) {
	defer wg.Done()

	for msg := range msgChan {
		// Simulate message processing
		time.Sleep(1 * time.Second)

		// Send message to the consumer
		err := encoder.Encode(&msg)
		if err != nil {
			fmt.Println("Failed to send message to consumer:", err)
			return
		}
	}
}

func main() {
	// Initialize broker
	broker := NewBroker()

	// Start the TLS server
	listener, err := createTLSServer("server-cert.pem", "server-key.pem", "ca-cert.pem")
	if err != nil {
		fmt.Println("Failed to start TLS server:", err)
		return
	}
	defer (*listener).Close()

	fmt.Println("Broker server started...")

	// Accept connections from publishers and consumers
	for {
		conn, err := (*listener).Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}

		// TLS handshake and verify client certificates
		tlsConn, ok := conn.(*tls.Conn)
		if !ok {
			fmt.Println("Connection is not a TLS connection")
			conn.Close()
			continue
		}

		err = tlsConn.Handshake()
		if err != nil {
			fmt.Println("TLS handshake failed:", err)
			tlsConn.Close()
			continue
		}

		// Read initial message to determine whether the client is a publisher or a consumer
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Failed to read initial message:", err)
			conn.Close()
			continue
		}

		// Initial message format: "type:topic" (type = publisher/consumer, topic = name of the topic)
		initialMsg := string(buf[:n])
		parts := strings.SplitN(initialMsg, ":", 2)
		if len(parts) < 2 {
			fmt.Println("Invalid initial message")
			conn.Close()
			continue
		}

		clientType, topic := parts[0], parts[1]
		if clientType == "publisher" {
			go handlePublisher(tlsConn, broker)
		} else if clientType == "consumer" {
			go handleConsumer(tlsConn, broker, topic, 3) // 3 workers for each consumer
		}
	}
}
