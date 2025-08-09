package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/handlers"
	"github.com/oarkflow/mq/services"
)

var availableHandlers = map[string]bool{
	"json/login.json":      false,
	"json/send-email.json": true,
}

func SetupHandler() (*dag.DAG, error) {
	var flowToServe *dag.DAG
	brokerAddr := ":8081"
	for file, serve := range availableHandlers {
		handlerBytes, err := os.ReadFile(file)
		if err != nil {
			panic(err)
		}
		var handler services.Handler
		err = json.Unmarshal(handlerBytes, &handler)
		if err != nil {
			panic(err)
		}
		flow := services.SetupHandler(handler, brokerAddr)
		if flow.Error != nil {
			return nil, flow.Error
		}
		if serve {
			flowToServe = flow
		}
	}
	return flowToServe, nil
}

func main() {
	flow, err := SetupHandler()
	if err != nil {
		fmt.Println("Error setting up handlers:", err)
		return
	}
	flow.Start(context.Background(), ":5000")
}

func init() {
	dag.AddHandler("render-html", func(id string) mq.Processor { return handlers.NewRenderHTMLNode(id) })
	dag.AddHandler("condition", func(id string) mq.Processor { return handlers.NewCondition(id) })
	dag.AddHandler("output", func(id string) mq.Processor { return handlers.NewOutputHandler(id) })
}
