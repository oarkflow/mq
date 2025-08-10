package console

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/cli/contracts"
)

type RunServer struct {
	server *fiber.App
}

func NewRunServer(server *fiber.App) *RunServer {
	return &RunServer{
		server: server,
	}
}

// Signature The name and signature of the console command.
func (receiver *RunServer) Signature() string {
	return "run:server"
}

// Description The console command description.
func (receiver *RunServer) Description() string {
	return "Run Server"
}

// Extend The console command extend.
func (receiver *RunServer) Extend() contracts.Extend {
	return contracts.Extend{
		Flags: []contracts.Flag{
			{
				Name:    "port",
				Value:   "",
				Aliases: []string{"p"},
				Usage:   "Port for the server to be running",
			},
		},
	}
}

// Handle Execute the console command.
func (receiver *RunServer) Handle(ctx contracts.Context) error {
	if receiver.server == nil {
		return errors.New("server is not configured")
	}
	port := ctx.Option("port")
	if port == "" {
		port = "3000" // Default port if not specified
	}
	if port[0] != ':' {
		port = ":" + port // Ensure the port starts with a colon
	}
	if err := receiver.server.Listen(port); err != nil {
		return errors.New("Failed to start server: " + err.Error())
	}
	return nil
}
