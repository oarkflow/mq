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
	cli.Run("mq", "0.0.1", func(client contracts.Cli) []contracts.Command {
		return []contracts.Command{
			console.NewListCommand(client),
			dagConsole.NewRunHandler(loader.UserConfig, loader.ParsedPath, brokerAddr),
			dagConsole.NewRunServer(serverApp),
		}
	})
}

func init() {
	dag.AddHandler("render-html", func(id string) mq.Processor { return handlers.NewRenderHTMLNode(id) })
	dag.AddHandler("condition", func(id string) mq.Processor { return handlers.NewCondition(id) })
	dag.AddHandler("output", func(id string) mq.Processor { return handlers.NewOutputHandler(id) })
}
