package main

import (
	"fmt"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/cli"
	"github.com/oarkflow/cli/console"
	"github.com/oarkflow/cli/contracts"
	"github.com/oarkflow/mq/handlers"
	"github.com/oarkflow/mq/services"
	"github.com/oarkflow/mq/services/cmd"
	dagConsole "github.com/oarkflow/mq/services/console"
)

func main() {
	handlers.Init()
	brokerAddr := ":5051"
	serverAddr := ":3000"
	loader := services.NewLoader("config")
	loader.Load()
	serverApp := fiber.New()
	cmd.Setup(loader, serverApp, brokerAddr)
	app := cli.New()
	client := app.Instance.Client()
	client.Register([]contracts.Command{
		console.NewListCommand(client),
		dagConsole.NewRunHandler(loader.UserConfig, loader.ParsedPath, brokerAddr),
		dagConsole.NewRunApiHandler(serverApp, serverAddr),
	})
	client.Run(os.Args, true)
}

func mai1n() {
	loader := services.NewLoader("config")
	loader.Load()
	fmt.Println(loader.UserConfig)
}
