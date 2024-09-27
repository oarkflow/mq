package mq

type CMD int

const (
	SUBSCRIBE CMD = iota + 1
	PUBLISH
	REQUEST
	STOP
)

var (
	ConsumerKey  = "Consumer-Key"
	PublisherKey = "Publisher-Key"
	ContentType  = "Content-Type"
	TypeJson     = "application/json"
	HeaderKey    = "headers"
)
