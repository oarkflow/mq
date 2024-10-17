package main

import (
	"fmt"
	"github.com/oarkflow/mq/cert"
	"log"
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
	// Sign the binary
	binaryPath := "dag"
	signaturePath := "dag.sig"
	err = ca.SignBinary(binaryPath, signaturePath)
	if err != nil {
		log.Fatalf("Failed to sign binary: %v", err)
	}
	fmt.Println("Binary signed successfully.")
	valid, err := ca.VerifyBinary(binaryPath, signaturePath)
	if err != nil {
		log.Fatalf("Failed to verify binary: %v", err)
	}
	if valid {
		fmt.Println("Signature is valid!")
	} else {
		fmt.Println("Signature is invalid!")
	}
}
