package main

import (
	"fmt"

	"github.com/oarkflow/mq/utils"
)

func main() {
	queryString := []byte("fields[0][method]=GET&fields[0][path]=/user/:id&fields[0][handlerMsg]=User Profile&fields[1][method]=POST&fields[1][path]=/user/create&fields[1][handlerMsg]=Create User")
	fmt.Println(utils.DecodeForm(queryString))
}
