package responses

import (
	"os"
	"strings"

	"github.com/andeya/goutil"
	"github.com/gofiber/fiber/v2"
)

type Response struct {
	Additional any    `json:"additional,omitempty"`
	Data       any    `json:"data"`
	Message    string `json:"message,omitempty"`
	StackTrace string `json:"stack_trace,omitempty"`
	Code       int    `json:"code"`
	Success    bool   `json:"success"`
}

func getResponse(code int, message string, additional any, stackTrace ...string) Response {
	var trace string
	response := Response{
		Code:       code,
		Message:    message,
		Success:    false,
		Additional: additional,
	}

	if len(stackTrace) > 0 {
		dir, _ := os.Getwd()
		trace = stackTrace[0]
		trace = strings.ReplaceAll(trace, dir, "/root")
		for _, t := range goutil.GetGopaths() {
			trace = strings.ReplaceAll(trace, t+"pkg/mod/", "/root/")
			trace = strings.ReplaceAll(trace, t, "/root/")
		}
		response.StackTrace = trace
	}
	return response
}

func Abort(ctx *fiber.Ctx, code int, message string, additional any, stackTrace ...string) error {
	return ctx.Status(fiber.StatusOK).JSON(getResponse(code, message, additional, stackTrace...))
}

func Failed(ctx *fiber.Ctx, code int, message string, additional any, stackTrace ...string) error {
	return ctx.Status(fiber.StatusOK).JSON(getResponse(code, message, additional, stackTrace...))
}

func Success(ctx *fiber.Ctx, code int, data any, message ...string) error {
	response := Response{
		Code:    code,
		Data:    data,
		Success: true,
	}
	if len(message) > 0 {
		response.Message = message[0]
	}
	return ctx.Status(fiber.StatusOK).JSON(response)
}
