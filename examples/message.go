package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
)

type Broker struct {
	aesKey      json.RawMessage
	hmacKey     json.RawMessage
	subscribers map[string][]net.Conn
	mu          sync.RWMutex
}

func NewBroker(aesKey, hmacKey json.RawMessage) *Broker {
	return &Broker{
		aesKey:      aesKey,
		hmacKey:     hmacKey,
		subscribers: make(map[string][]net.Conn),
	}
}

func (b *Broker) addSubscriber(topic string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[topic] = append(b.subscribers[topic], conn)
}

func (b *Broker) removeSubscriber(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for topic, conns := range b.subscribers {
		for i, c := range conns {
			if c == conn {
				b.subscribers[topic] = append(conns[:i], conns[i+1:]...)
				break
			}
		}
	}
}

func (b *Broker) broadcastToSubscribers(topic string, msg *codec.Message) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subscribers, ok := b.subscribers[topic]
	if !ok || len(subscribers) == 0 {
		log.Printf("No subscribers for topic: %s", topic)
		return
	}

	for _, conn := range subscribers {
		err := codec.SendMessage(conn, msg, b.aesKey, b.hmacKey, true)
		if err != nil {
			log.Printf("Error sending message to subscriber: %v", err)
		}
	}
}

func (b *Broker) OnMessage(ctx context.Context, msg *codec.Message, conn net.Conn) {
	switch msg.Command {
	case consts.PUBLISH:
		b.broadcastToSubscribers(msg.Queue, msg)
		ack := &codec.Message{
			Headers: msg.Headers,
			Queue:   msg.Queue,
			Command: consts.PUBLISH_ACK,
		}
		if err := codec.SendMessage(conn, ack, b.aesKey, b.hmacKey, true); err != nil {
			log.Printf("Error sending PUBLISH_ACK: %v\n", err)
		}
	case consts.SUBSCRIBE:
		b.addSubscriber(msg.Queue, conn)
		ack := &codec.Message{
			Headers: msg.Headers,
			Queue:   msg.Queue,
			Command: consts.SUBSCRIBE_ACK,
		}
		if err := codec.SendMessage(conn, ack, b.aesKey, b.hmacKey, true); err != nil {
			log.Printf("Error sending SUBSCRIBE_ACK: %v\n", err)
		}
	}
}

func (b *Broker) OnClose(ctx context.Context, conn net.Conn) {
	log.Println("Connection closed")
	b.removeSubscriber(conn)
}

func (b *Broker) OnError(ctx context.Context, err error) {
	log.Printf("Connection Error: %v\n", err)
}

func (b *Broker) readMessage(ctx context.Context, c net.Conn) error {
	msg, err := codec.ReadMessage(c, b.aesKey, b.hmacKey, true)
	if err == nil {
		ctx = mq.SetHeaders(ctx, msg.Headers)
		b.OnMessage(ctx, msg, c)
		return nil
	}
	if err.Error() == "EOF" || strings.Contains(err.Error(), "closed network connection") {
		b.OnClose(ctx, c)
		return err
	}
	b.OnError(ctx, err)
	return err
}

func (b *Broker) Serve(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			b.OnError(ctx, err)
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			for {
				err := b.readMessage(ctx, c)
				if err != nil {
					break
				}
			}
		}(conn)
	}
}

type Publisher struct {
	aesKey  json.RawMessage
	hmacKey json.RawMessage
}

func NewPublisher(aesKey, hmacKey json.RawMessage) *Publisher {
	return &Publisher{aesKey: aesKey, hmacKey: hmacKey}
}

func (p *Publisher) Publish(ctx context.Context, addr, topic string, payload json.RawMessage) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	headers, _ := mq.GetHeaders(ctx)
	msg := &codec.Message{
		Headers: headers,
		Queue:   topic,
		Command: consts.PUBLISH,
		Payload: payload,
	}
	if err := codec.SendMessage(conn, msg, p.aesKey, p.hmacKey, true); err != nil {
		return err
	}

	return p.waitForAck(conn)
}

func (p *Publisher) waitForAck(conn net.Conn) error {
	msg, err := codec.ReadMessage(conn, p.aesKey, p.hmacKey, true)
	if err != nil {
		return err
	}
	if msg.Command == consts.PUBLISH_ACK {
		log.Println("Received PUBLISH_ACK: Message published successfully")
		return nil
	}
	return fmt.Errorf("expected PUBLISH_ACK, got: %v", msg.Command)
}

type Consumer struct {
	aesKey  json.RawMessage
	hmacKey json.RawMessage
}

func NewConsumer(aesKey, hmacKey json.RawMessage) *Consumer {
	return &Consumer{aesKey: aesKey, hmacKey: hmacKey}
}

func (c *Consumer) Subscribe(ctx context.Context, addr, topic string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	headers, _ := mq.GetHeaders(ctx)
	msg := &codec.Message{
		Headers: headers,
		Queue:   topic,
		Command: consts.SUBSCRIBE,
	}
	if err := codec.SendMessage(conn, msg, c.aesKey, c.hmacKey, true); err != nil {
		return err
	}

	err = c.waitForAck(conn)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, err := codec.ReadMessage(conn, c.aesKey, c.hmacKey, true)
			if err != nil {
				log.Printf("Error reading message: %v\n", err)
				break
			}
			log.Printf("Received task on topic %s: %s\n", msg.Queue, msg.Payload)
		}
	}()

	wg.Wait()

	return nil
}

func (c *Consumer) waitForAck(conn net.Conn) error {
	msg, err := codec.ReadMessage(conn, c.aesKey, c.hmacKey, true)
	if err != nil {
		return err
	}
	if msg.Command == consts.SUBSCRIBE_ACK {
		log.Println("Received SUBSCRIBE_ACK: Subscribed successfully")
		return nil
	}
	return fmt.Errorf("expected SUBSCRIBE_ACK, got: %v", msg.Command)
}

func main() {
	addr := ":8081"
	aesKey := []byte("thisis32bytekeyforaesencryption1")
	hmacKey := []byte("thisisasecrethmackey1")

	broker := NewBroker(aesKey, hmacKey)
	publisher := NewPublisher(aesKey, hmacKey)
	consumer := NewConsumer(aesKey, hmacKey)

	go broker.Serve(context.Background(), addr)

	time.Sleep(1 * time.Second)
	go consumer.Subscribe(context.Background(), addr, "sensor_data")

	time.Sleep(3 * time.Second)
	data := map[string]interface{}{"temperature": 23.5, "humidity": 60}
	payload, _ := json.Marshal(data)
	go publisher.Publish(context.Background(), addr, "sensor_data", payload)

	time.Sleep(10 * time.Second)
}
