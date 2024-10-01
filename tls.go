package mq

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
)

// Connect to broker with optional TLS support
func connectToBroker(address string, useTLS bool, certFile string, caCertFile string) (net.Conn, error) {
	if useTLS {
		// Load the client certificate
		cert, err := tls.LoadX509KeyPair(certFile, certFile)
		if err != nil {
			return nil, err
		}

		// Load CA certificate if provided (optional)
		var tlsConfig *tls.Config
		if caCertFile != "" {
			caCert, err := os.ReadFile(caCertFile)
			if err != nil {
				return nil, err
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      caCertPool,
			}
		} else {
			tlsConfig = &tls.Config{
				Certificates:       []tls.Certificate{cert},
				InsecureSkipVerify: true, // For testing without CA verification
			}
		}

		// Dial TLS connection
		conn, err := tls.Dial("tcp", address, tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to broker via TLS: %v", err)
		}
		return conn, nil
	}

	// If not using TLS, use plain TCP connection
	return net.Dial("tcp", address)
}
