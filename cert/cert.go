package cert

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"time"
)

type CertInfo struct {
	Organization  string
	Country       string
	Province      string
	Locality      string
	StreetAddress string
	PostalCode    string
	CommonName    string
}

type CACert struct {
	PrivateKey *rsa.PrivateKey
	Cert       *x509.Certificate
	cert, key  string
}

func NewCACert(cert, key string) *CACert {
	return &CACert{
		cert: cert,
		key:  key,
	}
}

func (ca *CACert) Setup(info CertInfo) error {
	var caCertBytes []byte
	var err error
	if ca.PrivateKey, err = LoadPrivateKey(ca.key); err != nil {
		ca.PrivateKey, err = generatePrivateKey(2048)
		if err != nil {
			return fmt.Errorf("failed to generate CA private key: %v", err)
		}
		if err = savePrivateKey(ca.key, ca.PrivateKey); err != nil {
			return fmt.Errorf("failed to save CA private key: %v", err)
		}
	}
	if caCertBytes, ca.Cert, err = LoadCertificate(ca.cert); err != nil {
		caCertBytes, ca.Cert, err = ca.generateCACertificate(info)
		if err != nil {
			return fmt.Errorf("failed to generate CA certificate: %v", err)
		}
		if err = saveCertificate(ca.cert, caCertBytes); err != nil {
			return fmt.Errorf("failed to save CA certificate: %v", err)
		}
	}
	return nil
}

func (ca *CACert) GenerateKeys(commonName, certFile, keyFile string) error {
	serverCertBytes, serverPrivateKey, err := generateSignedCertificate(ca.Cert, ca.PrivateKey, commonName, true)
	if err != nil {
		return fmt.Errorf("failed to generate server certificate: %v", err)
	}
	err = saveCertificate(certFile, serverCertBytes)
	if err != nil {
		return fmt.Errorf("failed to save server certificate: %v", err)
	}
	err = savePrivateKey(keyFile, serverPrivateKey)
	if err != nil {
		return fmt.Errorf("failed to save server private key: %v", err)
	}
	return nil
}

func (ca *CACert) generateCACertificate(info CertInfo) ([]byte, *x509.Certificate, error) {
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	caTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:  []string{info.Organization},
			Country:       []string{info.Country},
			Province:      []string{info.Province},
			Locality:      []string{info.Locality},
			StreetAddress: []string{info.StreetAddress},
			PostalCode:    []string{info.PostalCode},
			CommonName:    info.CommonName,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLen:            0,
	}
	caCertBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &ca.PrivateKey.PublicKey, ca.PrivateKey)
	if err != nil {
		return nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caCertBytes)
	if err != nil {
		return nil, nil, err
	}
	return caCertBytes, caCert, nil
}

func (ca *CACert) SignBinary(binaryPath, signatureFile string) error {
	binary, err := os.Open(binaryPath)
	if err != nil {
		return fmt.Errorf("failed to open binary: %v", err)
	}
	defer binary.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, binary); err != nil {
		return fmt.Errorf("failed to hash binary: %v", err)
	}
	binaryHash := hash.Sum(nil)
	signature, err := rsa.SignPKCS1v15(rand.Reader, ca.PrivateKey, crypto.SHA256, binaryHash)
	if err != nil {
		return fmt.Errorf("failed to sign binary hash: %v", err)
	}
	return os.WriteFile(signatureFile, signature, 0644)
}

func (ca *CACert) VerifyBinary(binaryPath, signatureFile string) (bool, error) {
	binary, err := os.Open(binaryPath)
	if err != nil {
		return false, fmt.Errorf("failed to open binary: %v", err)
	}
	defer binary.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, binary); err != nil {
		return false, fmt.Errorf("failed to hash binary: %v", err)
	}
	binaryHash := hash.Sum(nil)
	signature, err := os.ReadFile(signatureFile)
	if err != nil {
		return false, fmt.Errorf("failed to load signature: %v", err)
	}
	err = rsa.VerifyPKCS1v15(ca.Cert.PublicKey.(*rsa.PublicKey), crypto.SHA256, binaryHash, signature)
	if err != nil {
		return false, fmt.Errorf("signature verification failed: %v", err)
	}
	return true, nil
}

func LoadPrivateKey(filename string) (*rsa.PrivateKey, error) {
	keyData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(keyData)
	if block == nil || block.Type != "RSA PRIVATE KEY" {
		return nil, errors.New("failed to decode PEM block containing private key")
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func LoadCertificate(filename string) ([]byte, *x509.Certificate, error) {
	certData, err := os.ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}
	block, _ := pem.Decode(certData)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, nil, errors.New("failed to decode PEM block containing certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return certData, cert, nil
}

func generatePrivateKey(bits int) (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

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
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
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
