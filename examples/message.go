package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/oarkflow/mq/codec"
	"github.com/oarkflow/mq/consts"
)

func main() {
	aesKey := []byte("thisis32bytekeyforaesencryption1")
	hmacKey := []byte("thisisasecrethmackey1")
	go func() {
		listener, err := net.Listen("tcp", ":8081")
		if err != nil {
			log.Fatal(err)
		}
		defer listener.Close()
		log.Println("Server is listening on :8080")
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println("Connection error:", err)
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				for {
					msg, err := codec.ReadMessage(c, aesKey, hmacKey)
					if err != nil {
						if err.Error() == "EOF" {
							log.Println("Client disconnected")
							break
						}
						log.Println("Failed to receive message:", err)
						break
					}
					log.Printf("Received Message:\n  Headers: %v\n  Topic: %s\n  Command: %v\n  Payload: %s\n",
						msg.Headers, msg.Topic, msg.Command, msg.Payload)
				}
			}(conn)
		}
	}()
	time.Sleep(5 * time.Second)
	conn, err := net.Dial("tcp", ":8081")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	headers := map[string]string{"Api-Key": "121323"}
	data := map[string]interface{}{"temperature": 23.5, "humidity": 60}
	payload, _ := json.Marshal(data)
	msg := &codec.Message{
		Headers: headers,
		Topic:   "sensor_data",
		Command: consts.SUBSCRIBE,
		Payload: payload,
	}
	if err := codec.SendMessage(conn, msg, aesKey, hmacKey); err != nil {
		log.Fatalf("Error sending message: %v", err)
	}
	fmt.Println("Message sent successfully")
	time.Sleep(5 * time.Second)
}
