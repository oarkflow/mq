package utils

import (
	"net"
)

func localAddr(c net.Conn) string  { return c.LocalAddr().String() }
func remoteAddr(c net.Conn) string { return c.RemoteAddr().String() }

func ConnectionsEqual(c1, c2 net.Conn) bool {
	if c1 == nil || c2 == nil {
		return false
	}
	return localAddr(c1) == localAddr(c2) && remoteAddr(c1) == remoteAddr(c2)
}

// GetRandomPort returns a free port chosen by the operating system.
func GetRandomPort() (int, error) {
	// Bind to port 0, which instructs the OS to assign an available port.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()

	// Extract the port number from the listener's address.
	addr := ln.Addr().(*net.TCPAddr)
	return addr.Port, nil
}
