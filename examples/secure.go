package main

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/oarkflow/mq/license"
	"log"
	"net/http"
	"os"
)

var SerialNumber string // This will be injected at build time

func main() {
	licensePath := "license.key"

	// Check if the license file exists
	if _, err := os.Stat(licensePath); os.IsNotExist(err) {
		// Prompt user for license file path if it doesn't exist
		fmt.Print("Enter the path to your license key: ")
		fmt.Scan(&licensePath)
	}

	// Load the license key
	licenseKey, err := os.ReadFile(licensePath)
	if err != nil {
		log.Fatalf("Failed to load license key: %v", err)
	}

	// Load the CA public key (from the certificate)
	certData, err := os.ReadFile("ca.crt")
	if err != nil {
		log.Fatalf("Failed to load CA certificate: %v", err)
	}
	block, _ := pem.Decode(certData)
	if block == nil || block.Type != "CERTIFICATE" {
		log.Fatalf("Failed to decode CA certificate")
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		log.Fatalf("Failed to parse CA certificate: %v", err)
	}
	publicKey := caCert.PublicKey.(*rsa.PublicKey)

	// Validate the license
	valid, encryptedData, err := license.ValidateLicense(publicKey, licenseKey, SerialNumber)
	if err != nil || !valid {
		log.Fatalf("License validation failed: %v", err)
	}

	// Send the encrypted data to a central server for further validation
	err = sendToServer(encryptedData)
	if err != nil {
		log.Fatalf("Failed to verify encrypted data with server: %v", err)
	}

	// If everything is valid, store the license for future use
	err = os.WriteFile("license.key", licenseKey, 0644)
	if err != nil {
		log.Fatalf("Failed to store license key: %v", err)
	}

	fmt.Println("License is valid. Proceeding with execution.")
	// Continue with program execution...
}

// sendToServer sends the decrypted license data to the central server for validation.
func sendToServer(encryptedData []byte) error {
	url := "https://myserver.com/verify" // Central server URL
	resp, err := http.Post(url, "application/json", bytes.NewReader(encryptedData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server validation failed: %v", resp.Status)
	}
	return nil
}
