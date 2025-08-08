package console

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/cli/contracts"
)

type RunServer struct {
	server *fiber.App
	addr   string
}

func NewRunServer(server *fiber.App, addr string) *RunServer {
	return &RunServer{
		server: server,
		addr:   addr,
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
	return contracts.Extend{}
}

// Handle Execute the console command.
func (receiver *RunServer) Handle(ctx contracts.Context) error {
	if receiver.server == nil {
		return errors.New("server is not configured")
	}
	if err := receiver.server.Listen(receiver.addr); err != nil {
		return errors.New("Failed to start server: " + err.Error())
	}
	return nil
}
