package console

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/cli/contracts"
)

type RunApiHandler struct {
	server *fiber.App
	addr   string
}

func NewRunApiHandler(server *fiber.App, addr string) *RunApiHandler {
	return &RunApiHandler{
		server: server,
		addr:   addr,
	}
}

// Signature The name and signature of the console command.
func (receiver *RunApiHandler) Signature() string {
	return "run:api-server"
}

// Description The console command description.
func (receiver *RunApiHandler) Description() string {
	return "Run API Server"
}

// Extend The console command extend.
func (receiver *RunApiHandler) Extend() contracts.Extend {
	return contracts.Extend{}
}

// Handle Execute the console command.
func (receiver *RunApiHandler) Handle(ctx contracts.Context) error {
	if receiver.server == nil {
		return errors.New("API server is not configured")
	}
	if err := receiver.server.Listen(receiver.addr); err != nil {
		return errors.New("Failed to start API server: " + err.Error())
	}
	return nil
}
