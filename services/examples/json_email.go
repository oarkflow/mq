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

func main() {
	handlerBytes, err := os.ReadFile("json/login.json")
	if err != nil {
		panic(err)
	}
	var handler services.Handler
	err = json.Unmarshal(handlerBytes, &handler)
	if err != nil {
		panic(err)
	}
	brokerAddr := ":8081"
	flow := services.SetupHandler(handler, brokerAddr)
	if flow.Error != nil {
		fmt.Println("Error setting up handler:", flow.Error)
		return
	}
	flow.Start(context.Background(), ":5000")
}

func init() {
	dag.AddHandler("render-html", func(id string) mq.Processor { return handlers.NewRenderHTMLNode(id) })
	dag.AddHandler("condition", func(id string) mq.Processor { return handlers.NewCondition(id) })
}
