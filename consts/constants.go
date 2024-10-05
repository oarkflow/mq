package consts

type CMD byte

func (c CMD) IsValid() bool { return c >= PING && c <= STOP }

const (
	PING CMD = iota + 1
	SUBSCRIBE
	SUBSCRIBE_ACK

	MESSAGE_SEND
	MESSAGE_RESPONSE
	MESSAGE_ACK
	MESSAGE_ERROR

	PUBLISH
	PUBLISH_ACK
	RESPONSE
	STOP
)

func (c CMD) String() string {
	switch c {
	case PING:
		return "PING"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBSCRIBE_ACK:
		return "SUBSCRIBE_ACK"
	case MESSAGE_SEND:
		return "MESSAGE_SEND"
	case MESSAGE_RESPONSE:
		return "MESSAGE_RESPONSE"
	case MESSAGE_ERROR:
		return "MESSAGE_ERROR"
	case MESSAGE_ACK:
		return "MESSAGE_ACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBLISH_ACK:
		return "PUBLISH_ACK"
	case STOP:
		return "STOP"
	case RESPONSE:
		return "RESPONSE"
	default:
		return "UNKNOWN"
	}
}

var (
	ConsumerKey      = "Consumer-Key"
	PublisherKey     = "Publisher-Key"
	ContentType      = "Content-Type"
	AwaitResponseKey = "Await-Response"
	QueueKey         = "Queue"
	TypeJson         = "application/json"
	HeaderKey        = "headers"
	TriggerNode      = "triggerNode"
)
