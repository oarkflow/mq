package dag

type NodeStatus int

func (c NodeStatus) IsValid() bool { return c >= Pending && c <= Failed }

func (c NodeStatus) String() string {
	switch c {
	case Pending:
		return "Pending"
	case Processing:
		return "Processing"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	}
	return ""
}

const (
	Pending NodeStatus = iota
	Processing
	Completed
	Failed
)
