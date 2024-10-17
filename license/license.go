package license

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

type License struct {
	Expiration    time.Time `json:"expiration"`
	Version       string    `json:"version"`
	SerialNumber  string    `json:"serial_number"`
	EncryptedData []byte    `json:"encrypted_data"`
}

func GenerateLicenseKey(privateKey *rsa.PrivateKey, license License, dataToEncrypt []byte) ([]byte, error) {
	encryptedData, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, &privateKey.PublicKey, dataToEncrypt, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt data: %v", err)
	}
	license.EncryptedData = encryptedData
	licenseBytes, err := json.Marshal(license)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal license data: %v", err)
	}
	hashed := sha256.Sum256(licenseBytes)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		return nil, fmt.Errorf("failed to sign license: %v", err)
	}
	signedLicense := append(licenseBytes, signature...)
	return signedLicense, nil
}

func SaveLicenseKey(filePath string, signedLicense []byte) error {
	return os.WriteFile(filePath, signedLicense, 0644)
}

func LoadLicenseKey(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func ValidateLicense(publicKey *rsa.PublicKey, signedLicense []byte, embeddedSerialNumber string) (bool, []byte, error) {
	licenseData := signedLicense[:len(signedLicense)-256]
	signature := signedLicense[len(signedLicense)-256:]
	hashed := sha256.Sum256(licenseData)
	err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hashed[:], signature)
	if err != nil {
		return false, nil, fmt.Errorf("license signature verification failed: %v", err)
	}
	var license License
	err = json.Unmarshal(licenseData, &license)
	if err != nil {
		return false, nil, fmt.Errorf("failed to unmarshal license data: %v", err)
	}
	if license.SerialNumber != embeddedSerialNumber {
		return false, nil, errors.New("serial number mismatch")
	}
	if license.Expiration.Before(time.Now()) {
		return false, nil, errors.New("license has expired")
	}
	return true, license.EncryptedData, nil
}
