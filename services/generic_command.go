package services

import (
	"github.com/oarkflow/cli/contracts"
	"github.com/oarkflow/errors"
)

type Flag struct {
	Name     string   `json:"name"`
	Value    string   `json:"value"`
	Usage    string   `json:"usage"`
	Aliases  []string `json:"aliases"`
	Required bool     `json:"required"`
}
type GenericCommand struct {
	handler    func(ctx contracts.Context) error
	Command    string  `json:"signature"`
	Desc       string  `json:"description"`
	Handler    Handler `json:"handler"`
	HandlerKey string  `json:"handler_key"`
	Flags      []Flag  `json:"flags"`
}

// Signature The name and signature of the console command.
func (receiver *GenericCommand) Signature() string {
	return receiver.Command
}

// Description The console command description.
func (receiver *GenericCommand) Description() string {
	return receiver.Desc
}

// Extend The console command extend.
func (receiver *GenericCommand) Extend() contracts.Extend {
	var flags []contracts.Flag
	for _, flag := range receiver.Flags {
		flags = append(flags, contracts.Flag{
			Name:     flag.Name,
			Aliases:  flag.Aliases,
			Usage:    flag.Usage,
			Required: flag.Required,
			Value:    flag.Value,
		})
	}
	return contracts.Extend{Flags: flags}
}

// Handle Execute the console command.
func (receiver *GenericCommand) Handle(ctx contracts.Context) error {
	if receiver.handler == nil {
		return errors.New("Handler not found")
	}
	return receiver.handler(ctx)
}

// SetHandler Execute the console command.
func (receiver *GenericCommand) SetHandler(handler func(ctx contracts.Context) error) {
	receiver.handler = handler
}
