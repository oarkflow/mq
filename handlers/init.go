package handlers

import (
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

func Init() {
	dag.AddHandler("start", func(id string) mq.Processor { return NewStartHandler(id) })
	dag.AddHandler("loop", func(id string) mq.Processor { return NewLoop(id) })
	dag.AddHandler("condition", func(id string) mq.Processor { return NewCondition(id) })
	dag.AddHandler("print", func(id string) mq.Processor { return NewPrintHandler(id) })
	dag.AddHandler("render", func(id string) mq.Processor { return NewRenderHTMLNode(id) })
	dag.AddHandler("log", func(id string) mq.Processor { return NewLogHandler(id) })
}
