package consts

type CMD byte

func (c CMD) IsValid() bool { return c >= PING && c <= STOP }

const (
	PING CMD = iota + 1
	SUBSCRIBE
	SUBSCRIBE_ACK
	PUBLISH
	REQUEST
	RESPONSE
	STOP
)

func (c CMD) String() string {
	switch c {
	case PING:
		return "PING"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case STOP:
		return "STOP"
	default:
		return "UNKNOWN"
	}
}

var (
	ConsumerKey  = "Consumer-Key"
	PublisherKey = "Publisher-Key"
	ContentType  = "Content-Type"
	TypeJson     = "application/json"
	HeaderKey    = "headers"
	TriggerNode  = "triggerNode"
)
