package mq

type CMD int

const (
	SUBSCRIBE CMD = iota + 1
	SUBSCRIBE_ACK
	PUBLISH
	REQUEST
	RESPONSE
	STOP
)

var (
	ConsumerKey  = "Consumer-Key"
	PublisherKey = "Publisher-Key"
	ContentType  = "Content-Type"
	TypeJson     = "application/json"
	HeaderKey    = "headers"
)
