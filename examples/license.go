package main

import (
	"crypto/rsa"
	"fmt"
	"github.com/oarkflow/mq/cert"
	"github.com/oarkflow/mq/license"
	"time"
)

func main() {
	info := cert.CertInfo{
		Organization:  "My CA",
		Country:       "US",
		Province:      "",
		Locality:      "My City",
		StreetAddress: "My Street",
		PostalCode:    "00000",
		CommonName:    "My CA",
	}
	ca := cert.NewCACert("ca.crt", "ca.key")
	err := ca.Setup(info)
	if err != nil {
		panic(err)
	}
	err = generateLicenseKey(ca.PrivateKey)
	if err != nil {
		panic(err)
	}
}

func generateLicenseKey(caPrivateKey *rsa.PrivateKey) error {
	l := license.License{
		Expiration:   time.Now().Add(30 * 24 * time.Hour),
		Version:      "v1.0.0",
		SerialNumber: "12345",
	}
	signedLicense, err := license.GenerateLicenseKey(caPrivateKey, l, []byte("Data to encrypt"))
	if err != nil {
		return fmt.Errorf("failed to generate license key: %v", err)
	}
	err = license.SaveLicenseKey("license.key", signedLicense)
	if err != nil {
		return fmt.Errorf("failed to save license key: %v", err)
	}
	return nil
}
