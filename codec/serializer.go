package codec

import (
	"github.com/oarkflow/json"
)

type MarshallerFunc func(v any) ([]byte, error)

type UnmarshallerFunc func(data []byte, v any) error

func (f MarshallerFunc) Marshal(v any) ([]byte, error) {
	return f(v)
}

func (f UnmarshallerFunc) Unmarshal(data []byte, v any) error {
	return f(data, v)
}

var defaultMarshaller MarshallerFunc = json.Marshal

var defaultUnmarshaller UnmarshallerFunc = func(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func SetMarshaller(marshaller MarshallerFunc) {
	defaultMarshaller = marshaller
}

func SetUnmarshaller(unmarshaller UnmarshallerFunc) {
	defaultUnmarshaller = unmarshaller
}

func Marshal(v any) ([]byte, error) {
	return defaultMarshaller(v)
}

func Unmarshal(data []byte, v any) error {
	return defaultUnmarshaller(data, v)
}
