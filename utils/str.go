package utils

import (
	"unsafe"
)

func ToByte(s string) []byte {
	p := unsafe.StringData(s)
	b := unsafe.Slice(p, len(s))
	return b
}

func FromByte(b []byte) string {
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}
