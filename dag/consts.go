package dag

import "time"

const (
	Delimiter          = "___"
	ContextIndex       = "index"
	DefaultChannelSize = 1000
	RetryInterval      = 5 * time.Second
)

type NodeType int

func (c NodeType) IsValid() bool { return c >= Function && c <= Page }

func (c NodeType) String() string {
	switch c {
	case Function:
		return "Function"
	case Page:
		return "Page"
	}
	return "Function"
}

const (
	Function NodeType = iota
	Page
)

type EdgeType int

func (c EdgeType) IsValid() bool { return c >= Simple && c <= Iterator }

func (c EdgeType) String() string {
	switch c {
	case Simple:
		return "Simple"
	case Iterator:
		return "Iterator"
	}
	return "Simple"
}

const (
	Simple EdgeType = iota
	Iterator
)

type Direction string

const (
	TB Direction = "TB"
	LR Direction = "LR"
)
