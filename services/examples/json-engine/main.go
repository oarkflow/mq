package main

import (
	"github.com/gofiber/fiber/v2"
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
	handlers.Init()
	brokerAddr := ":5051"
	loader := services.NewLoader("config")
	loader.Load()
	serverApp := fiber.New(fiber.Config{EnablePrintRoutes: true})
	services.Setup(loader, serverApp, brokerAddr)
	cli.Run("json-engine", "v1.0.0", func(client contracts.Cli) []contracts.Command {
		return []contracts.Command{
			console.NewListCommand(client),
			dagConsole.NewRunHandler(loader.UserConfig, loader.ParsedPath, brokerAddr),
			dagConsole.NewRunServer(serverApp),
		}
	})
}

func init() {
	// Register standard handlers
	dag.AddHandler("render-html", func(id string) mq.Processor { return handlers.NewRenderHTMLNode(id) })
	dag.AddHandler("condition", func(id string) mq.Processor { return handlers.NewCondition(id) })
	dag.AddHandler("output", func(id string) mq.Processor { return handlers.NewOutputHandler(id) })
	dag.AddHandler("print", func(id string) mq.Processor { return handlers.NewPrintHandler(id) })
	dag.AddHandler("format", func(id string) mq.Processor { return handlers.NewFormatHandler(id) })
	dag.AddHandler("data", func(id string) mq.Processor { return handlers.NewDataHandler(id) })
	dag.AddHandler("log", func(id string) mq.Processor { return handlers.NewLogHandler(id) })
	dag.AddHandler("json", func(id string) mq.Processor { return handlers.NewJSONHandler(id) })
	dag.AddHandler("split", func(id string) mq.Processor { return handlers.NewSplitHandler(id) })
	dag.AddHandler("join", func(id string) mq.Processor { return handlers.NewJoinHandler(id) })
	dag.AddHandler("field", func(id string) mq.Processor { return handlers.NewFieldHandler(id) })
	dag.AddHandler("flatten", func(id string) mq.Processor { return handlers.NewFlattenHandler(id) })
	dag.AddHandler("group", func(id string) mq.Processor { return handlers.NewGroupHandler(id) })
	dag.AddHandler("start", func(id string) mq.Processor { return handlers.NewStartHandler(id) })
}
