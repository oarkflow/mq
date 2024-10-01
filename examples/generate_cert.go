package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"os"
	"time"
)

func main() {
	// 1. Generate the CA private key
	caPrivateKey, err := generatePrivateKey(2048)
	if err != nil {
		log.Fatalf("Failed to generate CA private key: %v", err)
	}
	err = savePrivateKey("ca.key", caPrivateKey)
	if err != nil {
		log.Fatalf("Failed to save CA private key: %v", err)
	}

	// 2. Generate the CA certificate
	caCertBytes, caCert, err := generateCACertificate(caPrivateKey)
	if err != nil {
		log.Fatalf("Failed to generate CA certificate: %v", err)
	}
	err = saveCertificate("ca.crt", caCertBytes)
	if err != nil {
		log.Fatalf("Failed to save CA certificate: %v", err)
	}

	log.Println("CA Certificate and key generated")

	// 3. Generate the server certificate
	serverCertBytes, serverPrivateKey, err := generateSignedCertificate(caCert, caPrivateKey, "server", true)
	if err != nil {
		log.Fatalf("Failed to generate server certificate: %v", err)
	}
	err = saveCertificate("server.crt", serverCertBytes)
	if err != nil {
		log.Fatalf("Failed to save server certificate: %v", err)
	}
	err = savePrivateKey("server.key", serverPrivateKey)
	if err != nil {
		log.Fatalf("Failed to save server private key: %v", err)
	}

	log.Println("Server certificate and key generated")

	// 4. Generate the publisher certificate
	publisherCertBytes, publisherPrivateKey, err := generateSignedCertificate(caCert, caPrivateKey, "publisher", false)
	if err != nil {
		log.Fatalf("Failed to generate publisher certificate: %v", err)
	}
	err = saveCertificate("publisher.crt", publisherCertBytes)
	if err != nil {
		log.Fatalf("Failed to save publisher certificate: %v", err)
	}
	err = savePrivateKey("publisher.key", publisherPrivateKey)
	if err != nil {
		log.Fatalf("Failed to save publisher private key: %v", err)
	}

	log.Println("Publisher certificate and key generated")

	// 5. Generate the consumer certificate
	consumerCertBytes, consumerPrivateKey, err := generateSignedCertificate(caCert, caPrivateKey, "consumer", false)
	if err != nil {
		log.Fatalf("Failed to generate consumer certificate: %v", err)
	}
	err = saveCertificate("consumer.crt", consumerCertBytes)
	if err != nil {
		log.Fatalf("Failed to save consumer certificate: %v", err)
	}
	err = savePrivateKey("consumer.key", consumerPrivateKey)
	if err != nil {
		log.Fatalf("Failed to save consumer private key: %v", err)
	}

	log.Println("Consumer certificate and key generated")
}

// Generate a private key
func generatePrivateKey(bits int) (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

// Save private key to file
func savePrivateKey(filename string, privateKey *rsa.PrivateKey) error {
	keyOut, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer keyOut.Close()

	privateKeyPEM := pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}

	return pem.Encode(keyOut, &privateKeyPEM)
}

// Generate a self-signed certificate for the CA
func generateCACertificate(privateKey *rsa.PrivateKey) ([]byte, *x509.Certificate, error) {
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	caTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:  []string{"My CA"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"My City"},
			StreetAddress: []string{"My Street"},
			PostalCode:    []string{"00000"},
			CommonName:    "My CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // 1 year validity
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLen:            0,
	}

	caCertBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	caCert, err := x509.ParseCertificate(caCertBytes)
	if err != nil {
		return nil, nil, err
	}

	return caCertBytes, caCert, nil
}

// Save the certificate to a file
func saveCertificate(filename string, certBytes []byte) error {
	certOut, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer certOut.Close()

	certPEM := pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}
	return pem.Encode(certOut, &certPEM)
}

// Generate a signed certificate using the CA
func generateSignedCertificate(caCert *x509.Certificate, caPrivateKey *rsa.PrivateKey, commonName string, isServer bool) ([]byte, *rsa.PrivateKey, error) {
	privateKey, err := generatePrivateKey(2048)
	if err != nil {
		return nil, nil, err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	certTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour), // 1 year
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	if isServer {
		certTemplate.KeyUsage |= x509.KeyUsageKeyEncipherment
		certTemplate.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &certTemplate, caCert, &privateKey.PublicKey, caPrivateKey)
	if err != nil {
		return nil, nil, err
	}

	return certBytes, privateKey, nil
}
