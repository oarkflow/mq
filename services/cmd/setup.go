package cmd

import (
	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/mq/services"
)

func Setup(loader *services.Loader, serverApp *fiber.App, brokerAddr string) *fiber.App {
	if loader.UserConfig == nil {
		return nil
	}
	services.SetupServices(loader.Prefix(), serverApp, brokerAddr)
	return serverApp
}
