package services

import "github.com/gofiber/fiber/v2"

type Option func(map[string]any)
type Validation interface {
	Make(ctx *fiber.Ctx, data any, rules map[string]string, options ...Option) (Validator, error)
	AddRules([]Rule) error
	Rules() []Rule
}
type Validator interface {
	Bind(ptr any) error
	Errors() Errors
	Fails() bool
}
type Errors interface {
	One(key ...string) string
	Get(key string) map[string]string
	All() map[string]map[string]string
	Has(key string) bool
}

type ValidationData interface {
	Get(key string) (val any, exist bool)
	Set(key string, val any) error
}

type Rule interface {
	Signature() string
	Passes(ctx *fiber.Ctx, data ValidationData, val any, options ...any) bool
	Message() string
}
