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
