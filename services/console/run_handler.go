package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/oarkflow/cli/contracts"
	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/services"
)

type RunHandler struct {
	userConfig *services.UserConfig
	configPath string
	brokerAddr string
}

func NewRunHandler(userConfig *services.UserConfig, configPath, brokerAddr string) *RunHandler {
	return &RunHandler{
		userConfig: userConfig,
		configPath: configPath,
		brokerAddr: brokerAddr,
	}
}

// Signature The name and signature of the console command.
func (receiver *RunHandler) Signature() string {
	return "run:handler"
}

// Description The console command description.
func (receiver *RunHandler) Description() string {
	return "Run Handler"
}

// Extend The console command extend.
func (receiver *RunHandler) Extend() contracts.Extend {
	return contracts.Extend{
		Flags: []contracts.Flag{
			{
				Name:    "name",
				Value:   "",
				Aliases: []string{"n"},
				Usage:   "Name of handler to be running",
			},
			{
				Name:    "data",
				Value:   "",
				Aliases: []string{"d"},
				Usage:   "Data to be passed to the handler",
			},
			{
				Name:    "data-file",
				Value:   "",
				Aliases: []string{"f"},
				Usage:   "Data File to be passed to the handler",
			},
			{
				Name:    "header",
				Value:   "",
				Aliases: []string{"o"},
				Usage:   "Header to be passed to the handler",
			},
			{
				Name:    "header-file",
				Value:   "",
				Aliases: []string{"hf"},
				Usage:   "Header to be passed to the handler",
			},
		},
	}
}

// Handle Execute the console command.
func (receiver *RunHandler) Handle(ctx contracts.Context) error {
	name := ctx.Option("name")
	if name == "" {
		return errors.New("Handler name has to be provided")
	}
	handler := receiver.userConfig.GetHandler(name)
	if handler == nil {
		return errors.New("Handler not found")
	}
	flow := services.SetupHandler(*handler, receiver.brokerAddr)
	if flow.Error != nil {
		panic(flow.Error)
	}
	data, err := receiver.getData(ctx, "data", "data-file", "test/data", false)
	if err != nil {
		return err
	}
	headerData, err := receiver.getData(ctx, "header", "header-file", "test/header", true)
	if err != nil {
		return err
	}
	c := context.Background()
	if headerData == nil {
		headerData = make(map[string]any)
	}
	c = context.WithValue(c, "header", headerData)
	fmt.Println("Running Handler: ", name)
	rs := send(c, flow, data)
	if rs.Error == nil {
		fmt.Println("Handler response", string(rs.Payload))
	}
	return rs.Error
}

func (receiver *RunHandler) getData(ctx contracts.Context, dataOption, fileOption, path string, unmarshal bool) (any, error) {
	var err error
	var data any
	var jsonData map[string]any
	var requestData []map[string]any
	data = ctx.Option(dataOption)
	if data == "" {
		data = nil
	}
	if fileOption == "" {
		return data, nil
	}
	file := ctx.Option(fileOption)
	if file != "" {
		path := filepath.Join(receiver.configPath, path, file)
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	}
	if data == nil {
		return nil, nil
	}
	if unmarshal {
		err = Unmarshal(data, &jsonData)
		if err == nil {
			return jsonData, nil
		}
		err = Unmarshal(data, &requestData)
		if err == nil {
			return requestData, nil
		}
		return nil, err
	}
	return data, nil
}

func send(c context.Context, flow *dag.DAG, data any) mq.Result {
	switch data := data.(type) {
	case string:
		return flow.Process(c, []byte(data))
	case []byte:
		return flow.Process(c, data)
	case nil:
		return flow.Process(c, nil)
	}
	return mq.Result{}
}

func Unmarshal(data any, dst any) error {
	switch data := data.(type) {
	case string:
		return json.NewDecoder(bytes.NewBuffer([]byte(data))).Decode(dst)
	case []byte:
		return json.NewDecoder(bytes.NewBuffer(data)).Decode(dst)
	}
	return nil
}
