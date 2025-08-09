package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/handlers"
	"github.com/oarkflow/mq/services"
)

var availableHandlers = map[string]bool{
	"json/login.json":      false,
	"json/send-email.json": true,
}

func main() {
	flow, err := services.SetupHandlers(availableHandlers, ":8081")
	if err != nil {
		fmt.Println("Error setting up handlers:", err)
		return
	}
	if flow == nil {
		fmt.Println("Flow is configured but not started.")
		return
	}
	flow.Start(context.Background(), ":5000")
}

func init() {
	dag.AddHandler("render-html", func(id string) mq.Processor { return handlers.NewRenderHTMLNode(id) })
	dag.AddHandler("condition", func(id string) mq.Processor { return handlers.NewCondition(id) })
	dag.AddHandler("output", func(id string) mq.Processor { return handlers.NewOutputHandler(id) })
}
