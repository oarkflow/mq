package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"time"

	"github.com/oarkflow/json"
)

func main() {

	caCertDER, caPrivateKey := generateCA("P384", "My Secure CA")
	saveCertificate("ca.crt", caCertDER)
	signFileContent("ca.crt", caPrivateKey)

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		log.Fatal("Failed to parse CA certificate:", err)
	}

	serverCertDER, serverKey := generateServerCert(caCert, caPrivateKey, "P256", "server.example.com",
		[]string{"localhost", "server.example.com"}, []net.IP{net.ParseIP("127.0.0.1")})
	saveCertificate("server.crt", serverCertDER)
	signFileContent("server.crt", caPrivateKey)

	clientCertDER, clientKey := generateClientCert(caCert, caPrivateKey, "client-user")
	saveCertificate("client.crt", clientCertDER)
	signFileContent("client.crt", caPrivateKey)

	codeSignCertDER, codeSignKey := generateCodeSigningCert(caCert, caPrivateKey, 2048, "file-signer")
	saveCertificate("code_sign.crt", codeSignCertDER)
	signFileContent("code_sign.crt", caPrivateKey)

	revokedCerts := []pkix.RevokedCertificate{
		{SerialNumber: big.NewInt(1), RevocationTime: time.Now()},
	}
	generateCRL(caCert, caPrivateKey, revokedCerts)

	if err := validateClientCert("client.crt", "ca.crt"); err != nil {
		log.Fatal("Client certificate validation failed:", err)
	}

	log.Println("All certificates generated and signed successfully.")

	dss := NewDigitalSignatureService(clientKey)

	plainText := "The quick brown fox jumps over the lazy dog."
	textSig, err := dss.SignText(plainText)
	if err != nil {
		log.Fatal("Failed to sign plain text:", err)
	}
	if err := dss.VerifyText(plainText, textSig); err != nil {
		log.Fatal("Plain text signature verification failed:", err)
	}
	log.Println("Plain text signature verified.")

	jsonData := map[string]interface{}{
		"name":    "Alice",
		"age":     30,
		"premium": true,
	}
	jsonSig, err := dss.SignJSON(jsonData)
	if err != nil {
		log.Fatal("Failed to sign JSON data:", err)
	}
	fmt.Println(string(jsonSig))
	if err := dss.VerifyJSON(jsonData, jsonSig); err != nil {
		log.Fatal("JSON signature verification failed:", err)
	}
	log.Println("JSON signature verified.")

	_ = serverKey
	_ = codeSignKey
}

type DigitalSignatureService struct {
	Signer    crypto.Signer
	PublicKey crypto.PublicKey
}

func NewDigitalSignatureService(signer crypto.Signer) *DigitalSignatureService {
	return &DigitalSignatureService{
		Signer:    signer,
		PublicKey: signer.Public(),
	}
}

func (dss *DigitalSignatureService) SignData(data []byte) ([]byte, error) {
	switch dss.Signer.(type) {
	case ed25519.PrivateKey:
		return dss.Signer.Sign(rand.Reader, data, crypto.Hash(0))
	default:
		h := sha256.New()
		h.Write(data)
		hashed := h.Sum(nil)
		return dss.Signer.Sign(rand.Reader, hashed, crypto.SHA256)
	}
}

func (dss *DigitalSignatureService) VerifyData(data, signature []byte) error {
	switch pub := dss.PublicKey.(type) {
	case ed25519.PublicKey:
		if ed25519.Verify(pub, data, signature) {
			return nil
		}
		return errors.New("ed25519 signature verification failed")
	case *rsa.PublicKey:
		h := sha256.New()
		h.Write(data)
		hashed := h.Sum(nil)
		return rsa.VerifyPKCS1v15(pub, crypto.SHA256, hashed, signature)
	case *ecdsa.PublicKey:
		h := sha256.New()
		h.Write(data)
		hashed := h.Sum(nil)
		var sig struct {
			R, S *big.Int
		}
		if _, err := asn1.Unmarshal(signature, &sig); err != nil {
			return err
		}
		if ecdsa.Verify(pub, hashed, sig.R, sig.S) {
			return nil
		}
		return errors.New("ecdsa signature verification failed")
	default:
		return errors.New("unsupported public key type")
	}
}

func (dss *DigitalSignatureService) SignText(text string) ([]byte, error) {
	return dss.SignData([]byte(text))
}

func (dss *DigitalSignatureService) VerifyText(text string, signature []byte) error {
	return dss.VerifyData([]byte(text), signature)
}

func (dss *DigitalSignatureService) SignJSON(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return dss.SignData(b)
}

func (dss *DigitalSignatureService) VerifyJSON(v interface{}, signature []byte) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return dss.VerifyData(b, signature)
}

func generateCA(curveName string, commonName string) ([]byte, crypto.Signer) {
	privKey, err := generatePrivateKey("ECDSA", curveName)
	if err != nil {
		log.Fatal("CA key generation failed:", err)
	}
	subject := pkix.Name{
		CommonName:   commonName,
		Organization: []string{"Secure CA Org"},
		Country:      []string{"US"},
	}
	template := x509.Certificate{
		SerialNumber:          randomSerial(),
		Subject:               subject,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, privKey.Public(), privKey)
	if err != nil {
		log.Fatal("CA cert creation failed:", err)
	}

	return certBytes, privKey
}

func generateServerCert(caCert *x509.Certificate, caKey crypto.Signer, keyType string, commonName string, dnsNames []string, ips []net.IP) ([]byte, crypto.Signer) {
	privKey, err := generatePrivateKey("ECDSA", keyType)
	if err != nil {
		log.Fatal("Server key generation failed:", err)
	}
	template := x509.Certificate{
		SerialNumber: randomSerial(),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(1, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    dnsNames,
		IPAddresses: ips,
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, privKey.Public(), caKey)
	if err != nil {
		log.Fatal("Server cert creation failed:", err)
	}
	return certBytes, privKey
}

func generateClientCert(caCert *x509.Certificate, caKey crypto.Signer, commonName string) ([]byte, crypto.Signer) {
	privKey, err := generatePrivateKey("Ed25519", nil)
	if err != nil {
		log.Fatal("Client key generation failed:", err)
	}
	template := x509.Certificate{
		SerialNumber: randomSerial(),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(1, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, privKey.Public(), caKey)
	if err != nil {
		log.Fatal("Client cert creation failed:", err)
	}
	return certBytes, privKey
}

func generateCodeSigningCert(caCert *x509.Certificate, caKey crypto.Signer, rsaBits int, commonName string) ([]byte, crypto.Signer) {
	privKey, err := generatePrivateKey("RSA", rsaBits)
	if err != nil {
		log.Fatal("Code signing key generation failed:", err)
	}
	template := x509.Certificate{
		SerialNumber: randomSerial(),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(5, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, privKey.Public(), caKey)
	if err != nil {
		log.Fatal("Code signing cert creation failed:", err)
	}
	return certBytes, privKey
}

func generateCRL(caCert *x509.Certificate, caKey crypto.Signer, revoked []pkix.RevokedCertificate) {
	crlTemplate := &x509.RevocationList{
		SignatureAlgorithm:  caCert.SignatureAlgorithm,
		RevokedCertificates: revoked,
		Number:              randomSerial(),
		ThisUpdate:          time.Now(),
		NextUpdate:          time.Now().AddDate(0, 1, 0),
		Issuer:              caCert.Subject,
	}
	crlBytes, err := x509.CreateRevocationList(rand.Reader, crlTemplate, caCert, caKey)
	if err != nil {
		log.Fatal("CRL creation failed:", err)
	}
	saveCRL("ca.crl", crlBytes)
	signFileContent("ca.crl", caKey)
}

func generatePrivateKey(algo string, param interface{}) (crypto.Signer, error) {
	switch algo {
	case "RSA":
		bits, ok := param.(int)
		if !ok {
			return nil, fmt.Errorf("invalid RSA parameter")
		}
		return rsa.GenerateKey(rand.Reader, bits)
	case "ECDSA":
		curveName, ok := param.(string)
		if !ok {
			return nil, fmt.Errorf("invalid ECDSA parameter")
		}
		var curve elliptic.Curve
		switch curveName {
		case "P224":
			curve = elliptic.P224()
		case "P256":
			curve = elliptic.P256()
		case "P384":
			curve = elliptic.P384()
		case "P521":
			curve = elliptic.P521()
		default:
			return nil, fmt.Errorf("unsupported curve")
		}
		return ecdsa.GenerateKey(curve, rand.Reader)
	case "Ed25519":
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		return priv, err
	default:
		return nil, fmt.Errorf("unsupported algorithm")
	}
}

func saveCertificate(filename string, cert []byte) {
	err := os.WriteFile(filename, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	}), 0644)
	if err != nil {
		log.Fatal("Failed to save certificate:", err)
	}
}

func savePrivateKey(filename string, key crypto.Signer) {
	keyBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		log.Fatal("Failed to marshal private key:", err)
	}
	err = os.WriteFile(filename, pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: keyBytes,
	}), 0600)
	if err != nil {
		log.Fatal("Failed to save private key:", err)
	}
}

func saveCRL(filename string, crl []byte) {
	err := os.WriteFile(filename, pem.EncodeToMemory(&pem.Block{
		Type:  "X509 CRL",
		Bytes: crl,
	}), 0644)
	if err != nil {
		log.Fatal("Failed to save CRL:", err)
	}
}

func signFileContent(filename string, signer crypto.Signer) {
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal("Failed to read file for signing:", err)
	}
	sig, err := signData(data, signer)
	if err != nil {
		log.Fatal("Failed to sign file content:", err)
	}
	err = os.WriteFile(filename+".sig", sig, 0644)
	if err != nil {
		log.Fatal("Failed to save signature file:", err)
	}
}

func signData(data []byte, key crypto.Signer) ([]byte, error) {
	switch key.(type) {
	case ed25519.PrivateKey:
		return key.Sign(rand.Reader, data, crypto.Hash(0))
	default:
		h := sha256.New()
		h.Write(data)
		hashed := h.Sum(nil)
		return key.Sign(rand.Reader, hashed, crypto.SHA256)
	}
}

func verifyDataSignature(data, signature []byte, pub crypto.PublicKey) error {
	switch pub := pub.(type) {
	case ed25519.PublicKey:
		if ed25519.Verify(pub, data, signature) {
			return nil
		}
		return errors.New("ed25519 signature verification failed")
	case *rsa.PublicKey:
		h := sha256.New()
		h.Write(data)
		hashed := h.Sum(nil)
		return rsa.VerifyPKCS1v15(pub, crypto.SHA256, hashed, signature)
	case *ecdsa.PublicKey:
		h := sha256.New()
		h.Write(data)
		hashed := h.Sum(nil)
		var sig struct {
			R, S *big.Int
		}
		if _, err := asn1.Unmarshal(signature, &sig); err != nil {
			return err
		}
		if ecdsa.Verify(pub, hashed, sig.R, sig.S) {
			return nil
		}
		return errors.New("ecdsa signature verification failed")
	default:
		return errors.New("unsupported public key type")
	}
}

func verifyFileContentSignature(filename, sigFilename string, pub crypto.PublicKey) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	sig, err := os.ReadFile(sigFilename)
	if err != nil {
		return fmt.Errorf("failed to read signature file: %w", err)
	}
	return verifyDataSignature(data, sig, pub)
}

func validateClientCert(clientCertPath, caCertPath string) error {
	caCertBytes, err := os.ReadFile(caCertPath)
	if err != nil {
		return err
	}
	block, _ := pem.Decode(caCertBytes)
	if block == nil {
		return fmt.Errorf("failed to decode CA cert PEM")
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	clientCertBytes, err := os.ReadFile(clientCertPath)
	if err != nil {
		return err
	}
	block, _ = pem.Decode(clientCertBytes)
	if block == nil {
		return fmt.Errorf("failed to decode client cert PEM")
	}
	clientCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	opts := x509.VerifyOptions{
		Roots:     x509.NewCertPool(),
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	opts.Roots.AddCert(caCert)
	if _, err = clientCert.Verify(opts); err != nil {
		return fmt.Errorf("certificate chain verification failed: %w", err)
	}
	if err = verifyFileContentSignature(clientCertPath, clientCertPath+".sig", caCert.PublicKey); err != nil {
		return fmt.Errorf("file signature verification failed: %w", err)
	}
	return nil
}

func randomSerial() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		log.Fatal("failed to generate serial number:", err)
	}
	return serial
}
