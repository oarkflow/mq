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
				Name:    "serve",
				Value:   "false",
				Aliases: []string{"s"},
				Usage:   "Run the handler as a server",
			},
			{
				Name:    "port",
				Value:   "",
				Aliases: []string{"p"},
				Usage:   "Port for the handler to be running",
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
			{
				Name:    "enhanced",
				Value:   "false",
				Aliases: []string{"e"},
				Usage:   "Run as enhanced handler with workflow engine support",
			},
			{
				Name:    "workflow",
				Value:   "false",
				Aliases: []string{"w"},
				Usage:   "Enable workflow engine features",
			},
		},
	}
} // Handle Execute the console command.
func (receiver *RunHandler) Handle(ctx contracts.Context) error {
	name := ctx.Option("name")
	serve := ctx.Option("serve")
	enhanced := ctx.Option("enhanced")
	workflow := ctx.Option("workflow")

	if serve == "" {
		serve = "false"
	}
	if enhanced == "" {
		enhanced = "false"
	}
	if workflow == "" {
		workflow = "false"
	}

	if name == "" {
		return errors.New("Handler name has to be provided")
	}

	// Check if enhanced handler is requested or if handler is configured as enhanced
	isEnhanced := enhanced == "true" || receiver.userConfig.IsEnhancedHandler(name)

	var flow *dag.DAG
	var err error

	if isEnhanced {
		// Try to get enhanced handler first
		enhancedHandler := receiver.userConfig.GetEnhancedHandler(name)
		if enhancedHandler != nil {
			fmt.Printf("Setting up enhanced handler: %s\n", name)
			flow, err = services.SetupEnhancedHandler(*enhancedHandler, receiver.brokerAddr)
			if err != nil {
				return fmt.Errorf("failed to setup enhanced handler: %w", err)
			}
		} else {
			// Fallback to traditional handler
			handler := receiver.userConfig.GetHandler(name)
			if handler == nil {
				return errors.New("Handler not found")
			}
			flow = services.SetupHandler(*handler, receiver.brokerAddr)
		}
	} else {
		// Traditional handler
		handler := receiver.userConfig.GetHandler(name)
		if handler == nil {
			return errors.New("Handler not found")
		}
		flow = services.SetupHandler(*handler, receiver.brokerAddr)
	}

	if flow.Error != nil {
		panic(flow.Error)
	}

	port := ctx.Option("port")
	if port == "" {
		port = "8080"
	}

	if serve != "false" {
		fmt.Printf("Starting %s handler server on port %s\n",
			func() string {
				if isEnhanced {
					return "enhanced"
				} else {
					return "traditional"
				}
			}(), port)
		if err := flow.Start(context.Background(), ":"+port); err != nil {
			return fmt.Errorf("error starting handler: %w", err)
		}
		return nil
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

	// Convert headerData to map[string]any if it's not already
	var headerMap map[string]any
	switch h := headerData.(type) {
	case map[string]any:
		headerMap = h
	default:
		headerMap = make(map[string]any)
	}

	// Add enhanced context information if workflow is enabled
	if workflow == "true" || isEnhanced {
		headerMap["workflow_enabled"] = true
		headerMap["enhanced_mode"] = isEnhanced
	}

	c = context.WithValue(c, "header", headerMap)
	fmt.Printf("Running %s Handler: %s\n",
		func() string {
			if isEnhanced {
				return "Enhanced"
			} else {
				return "Traditional"
			}
		}(), name)
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

// Enhanced helper functions

// getHandlerInfo returns information about the handler (traditional or enhanced)
func (receiver *RunHandler) getHandlerInfo(name string) (interface{}, bool) {
	// Check enhanced handlers first
	if enhancedHandler := receiver.userConfig.GetEnhancedHandler(name); enhancedHandler != nil {
		return *enhancedHandler, true
	}

	// Check traditional handlers
	if handler := receiver.userConfig.GetHandler(name); handler != nil {
		return *handler, false
	}

	return nil, false
}

// listAvailableHandlers lists all available handlers (both traditional and enhanced)
func (receiver *RunHandler) listAvailableHandlers() {
	fmt.Println("Available Traditional Handlers:")
	for _, handler := range receiver.userConfig.Policy.Handlers {
		fmt.Printf("  - %s (%s)\n", handler.Name, handler.Key)
	}

	if len(receiver.userConfig.Policy.EnhancedHandlers) > 0 {
		fmt.Println("\nAvailable Enhanced Handlers:")
		for _, handler := range receiver.userConfig.Policy.EnhancedHandlers {
			status := "disabled"
			if handler.WorkflowEnabled {
				status = "workflow enabled"
			}
			fmt.Printf("  - %s (%s) [%s]\n", handler.Name, handler.Key, status)
		}
	}
}
