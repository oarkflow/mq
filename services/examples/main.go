package main

import (
	"fmt"

	"github.com/oarkflow/mq/services"
)

func main() {
	loader := services.NewLoader("config")
	loader.Load()
	fmt.Println(loader.UserConfig)
}
