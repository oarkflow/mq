package utils

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

func GenerateHMACKey(length int) (string, error) {
	key := make([]byte, length)
	_, err := rand.Read(key)
	if err != nil {
		return "", fmt.Errorf("failed to generate random key: %v", err)
	}
	return hex.EncodeToString(key), nil
}

func MustGenerateHMACKey(length int) string {
	key, err := GenerateHMACKey(length)
	if err != nil {
		panic(err)
	}
	return key
}

func GenerateSecretKey(length int) (string, error) {
	key := make([]byte, length)
	_, err := rand.Read(key)
	if err != nil {
		return "", err
	}
	secretKey := base64.StdEncoding.EncodeToString(key)
	return secretKey[:length], nil
}

func MustGenerateSecretKey(length int) string {
	key, err := GenerateSecretKey(length)
	if err != nil {
		panic(err)
	}
	return key
}
