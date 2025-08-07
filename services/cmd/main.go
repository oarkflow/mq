package main

import (
	"os"

	"github.com/oarkflow/cli"
	"github.com/oarkflow/cli/console"
	"github.com/oarkflow/cli/contracts"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/handlers"
	"github.com/oarkflow/mq/services"
	dagConsole "github.com/oarkflow/mq/services/console"
)

func main() {
	brokerAddr := ":5051"
	loader := services.NewLoader("examples/config")
	loader.Load()
	cli.SetName("DAG CLI")
	cli.SetVersion("v0.0.1")
	app := cli.New()
	client := app.Instance.Client()
	client.Register([]contracts.Command{
		console.NewListCommand(client),
		dagConsole.NewRunHandler(loader.UserConfig, loader.ParsedPath, brokerAddr),
	})
	client.Run(os.Args, true)
}

func init() {
	dag.AddHandler("start", func(id string) mq.Processor { return handlers.NewStartHandler(id) })
	dag.AddHandler("loop", func(id string) mq.Processor { return handlers.NewLoop(id) })
	dag.AddHandler("condition", func(id string) mq.Processor { return handlers.NewCondition(id) })
	dag.AddHandler("print", func(id string) mq.Processor { return handlers.NewPrintHandler(id) })
	dag.AddHandler("render", func(id string) mq.Processor { return handlers.NewRenderHTMLNode(id) })
	dag.AddHandler("log", func(id string) mq.Processor { return handlers.NewLogHandler(id) })
}
