package v2

import "time"

const (
	Delimiter          = "___"
	ContextIndex       = "index"
	DefaultChannelSize = 1000
	RetryInterval      = 5 * time.Second
)

type Status string

const (
	Pending    Status = "Pending"
	Processing Status = "Processing"
	Completed  Status = "Completed"
	Failed     Status = "Failed"
)

type NodeType int

func (c NodeType) IsValid() bool { return c >= Function && c <= Page }

const (
	Function NodeType = iota
	Page
)

type EdgeType int

func (c EdgeType) IsValid() bool { return c >= Simple && c <= Iterator }

const (
	Simple EdgeType = iota
	Iterator
)
