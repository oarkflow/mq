package handlers

import (
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
)

func Init() {
	// Basic handlers
	dag.AddHandler("start", func(id string) mq.Processor { return NewStartHandler(id) })
	dag.AddHandler("loop", func(id string) mq.Processor { return NewLoop(id) })
	dag.AddHandler("condition", func(id string) mq.Processor { return NewCondition(id) })
	dag.AddHandler("print", func(id string) mq.Processor { return NewPrintHandler(id) })
	dag.AddHandler("render", func(id string) mq.Processor { return NewRenderHTMLNode(id) })
	dag.AddHandler("log", func(id string) mq.Processor { return NewLogHandler(id) })

	// Data transformation handlers
	dag.AddHandler("data:transform", func(id string) mq.Processor { return NewDataHandler(id) })
	dag.AddHandler("field", func(id string) mq.Processor { return NewFieldHandler(id) })
	dag.AddHandler("format", func(id string) mq.Processor { return NewFormatHandler(id) })
	dag.AddHandler("group", func(id string) mq.Processor { return NewGroupHandler(id) })
	dag.AddHandler("flatten", func(id string) mq.Processor { return NewFlattenHandler(id) })
	dag.AddHandler("output", func(id string) mq.Processor { return NewOutputHandler(id) })
	dag.AddHandler("split", func(id string) mq.Processor { return NewSplitHandler(id) })
	dag.AddHandler("join", func(id string) mq.Processor { return NewJoinHandler(id) })
}
