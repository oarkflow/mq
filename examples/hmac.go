package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
)

func GenerateSecretKey() (string, error) {
	// Create a byte slice to hold 32 random bytes
	key := make([]byte, 32)

	// Fill the slice with secure random bytes
	_, err := rand.Read(key)
	if err != nil {
		return "", err
	}

	// Encode the byte slice to a Base64 string
	secretKey := base64.StdEncoding.EncodeToString(key)

	// Return the first 32 characters
	return secretKey[:32], nil
}

func main() {
	secretKey, err := GenerateSecretKey()
	if err != nil {
		log.Fatalf("Error generating secret key: %v", err)
	}

	fmt.Println("Generated Secret Key:", secretKey)
}
