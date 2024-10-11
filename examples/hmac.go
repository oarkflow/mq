package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func generateHMACKey() ([]byte, error) {
	key := make([]byte, 32) // 32 bytes = 256 bits
	_, err := rand.Read(key)
	if err != nil {
		return nil, err
	}
	return key, nil
}

func main() {
	hmacKey, err := generateHMACKey()
	if err != nil {
		fmt.Println("Error generating HMAC key:", err)
		return
	}

	fmt.Println("HMAC Key (hex):", hex.EncodeToString(hmacKey))
}
