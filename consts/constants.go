package consts

type CMD byte

func (c CMD) IsValid() bool { return c >= PING && c <= CONSUMER_STOP }

const (
	PING CMD = iota + 1
	SUBSCRIBE
	SUBSCRIBE_ACK

	MESSAGE_SEND
	MESSAGE_RESPONSE
	MESSAGE_DENY
	MESSAGE_ACK
	MESSAGE_ERROR

	PUBLISH
	PUBLISH_ACK
	RESPONSE

	CONSUMER_PAUSE
	CONSUMER_RESUME
	CONSUMER_STOP
	CONSUMER_UPDATE

	CONSUMER_PAUSED
	CONSUMER_RESUMED
	CONSUMER_STOPPED
	CONSUMER_UPDATED
)

type ConsumerState byte

const (
	ConsumerStateActive ConsumerState = iota
	ConsumerStatePaused
	ConsumerStateStopped
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
	case MESSAGE_DENY:
		return "MESSAGE_DENY"
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
	case CONSUMER_PAUSE:
		return "CONSUMER_PAUSE"
	case CONSUMER_RESUME:
		return "CONSUMER_RESUME"
	case CONSUMER_UPDATE:
		return "CONSUMER_UPDATE"
	case CONSUMER_STOP:
		return "CONSUMER_STOP"
	case CONSUMER_PAUSED:
		return "CONSUMER_PAUSED"
	case CONSUMER_RESUMED:
		return "CONSUMER_RESUMED"
	case CONSUMER_STOPPED:
		return "CONSUMER_STOPPED"
	case CONSUMER_UPDATED:
		return "CONSUMER_UPDATED"
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
	QueueKey         = "Topic"
	TypeJson         = "application/json; charset=utf-8"
	TypeHtml         = "text/html; charset=utf-8"
	HeaderKey        = "headers"
	TriggerNode      = "triggerNode"
)
