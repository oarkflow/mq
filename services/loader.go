package services

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/oarkflow/filters"
	"github.com/oarkflow/jenv"
	"github.com/oarkflow/json"
	"github.com/oarkflow/metadata"
	"gopkg.in/yaml.v3"
)

var userConfig *UserConfig

type Loader struct {
	path       string
	configFile string
	ParsedPath string
	UserConfig *UserConfig
}

func NewLoader(path string, configFiles ...string) *Loader {
	var configFile string
	if len(configFiles) > 0 {
		configFile = configFiles[0]
	}
	return &Loader{
		path:       path,
		configFile: configFile,
	}
}

func (l *Loader) Prefix() string {
	return ""
}

func (l *Loader) Load() {
	l.ParsedPath = l.prepareConfigPath()
	cfg, err := l.loadConfig()
	if err != nil {
		panic(err)
	}
	l.UserConfig = cfg
	userConfig = cfg // Set the global userConfig variable
}

func (l *Loader) prepareConfigPath() string {
	path := l.path
	if !filepath.IsAbs(path) {
		b, err := filepath.Abs(path)
		if err == nil {
			path = b
		}
	}
	return path
}

func (l *Loader) loadConfig() (*UserConfig, error) {
	cfg := &UserConfig{}
	configFile := l.configFile
	if configFile != "" {
		err := readFile(configFile, cfg)
		if err != nil {
			return nil, err
		}
		initializeConfig(cfg)
	}
	if l.ParsedPath != "" {
		err := readPath(l.ParsedPath, cfg)
		if err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func readPath(path string, cfg *UserConfig) error {
	readers := []func(string, *UserConfig) error{
		readConfig, readCredentials, readConditions, readHandlers,
		readModels, readApplicationRules, readCommands, readBackgroundTasks,
		readWeb, readRenderer, readApis,
	}

	for _, read := range readers {
		if err := read(path, cfg); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func readFile(path string, cfg any) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return unmarshalConfig(content, path, cfg)
}

func unmarshalConfig(data []byte, path string, cfg any) error {
	ext := filepath.Ext(path)
	switch ext {
	case ".json":
		return jenv.UnmarshalJSON(data, cfg)
	case ".yaml", ".yml":
		return jenv.UnmarshalYAML(data, cfg)
	default:
		return errors.New("unsupported file format. Only yaml and json supported")
	}
}

func initializeConfig(cfg *UserConfig) {
	// Initialize Application Rules
	for i, applicationRule := range cfg.Policy.ApplicationRules {
		if applicationRule.Rule != nil {
			applicationRule.BuildRuleFromRequest(cfg.GetCondition)
			cfg.Policy.ApplicationRules[i] = applicationRule
		}
	}
	// Initialize Background Handlers
	for i, command := range cfg.Policy.BackgroundHandlers {
		if command.HandlerKey != "" {
			if handler := cfg.GetHandler(command.HandlerKey); handler != nil {
				command.Handler = *handler
				cfg.Policy.BackgroundHandlers[i] = command
			}
		}
	}
	// Initialize API Handlers
	for i, api := range cfg.Policy.Web.Apis {
		for j, route := range api.Routes {
			if route.HandlerKey != "" {
				if handler := cfg.GetHandler(route.HandlerKey); handler != nil {
					route.Handler = *handler
					api.Routes[j] = route
					cfg.Policy.Web.Apis[i] = api
				}
			}
		}
	}
}

// Helper function to read either JSON or YAML config files
func readConfigFile[T any](path string, appendFn func(T)) error {
	var out T
	var err error
	if content, readErr := os.ReadFile(path + ".json"); readErr == nil {
		err = json.Unmarshal(content, &out)
	} else if content, readErr := os.ReadFile(path + ".yaml"); readErr == nil {
		err = yaml.Unmarshal(content, &out)
	} else {
		err = readErr // Set to the most recent read error if both files are missing
	}
	if err != nil {
		return err
	}
	appendFn(out)
	return nil
}

func unmarshalContent[T any](content []byte, dataType string, appendFn func(T)) error {
	var out T
	var err error
	switch dataType {
	case "json":
		err = jenv.UnmarshalJSON(content, &out)
	case "yaml":
		err = jenv.UnmarshalYAML(content, &out)
	}
	if err != nil {
		return err
	}
	appendFn(out)
	return nil
}

// Helper function to read either JSON or YAML config files
func readArrayConfigFile[T any](path string, appendFn func(T)) error {
	var err error
	var data []json.RawMessage
	var dataType string
	if content, readErr := os.ReadFile(path + ".json"); readErr == nil {
		dataType = "json"
		err = json.Unmarshal(content, &data)
	} else if content, readErr := os.ReadFile(path + ".yaml"); readErr == nil {
		dataType = "yaml"
		err = yaml.Unmarshal(content, &data)
	} else {
		err = readErr // Set to the most recent read error if both files are missing
	}
	if err != nil {
		return err
	}
	for _, d := range data {
		err = unmarshalContent(d, dataType, appendFn)
		if err != nil {
			return err
		}
	}
	return nil
}

// Sample read function with YAML/JSON support
func readConditions(path string, cfg *UserConfig) error {
	path = filepath.Join(path, "policies", "conditions")
	return readConfigFile(path, func(data []*filters.Filter) {
		cfg.Policy.Conditions = append(cfg.Policy.Conditions, data...)
	})
}

func readApplicationRules(path string, cfg *UserConfig) error {
	modelsPath := filepath.Join(path, "policies", "application_rules")
	entries, err := os.ReadDir(modelsPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && isSupportedExt(filepath.Ext(entry.Name())) {
			file := filepath.Join(modelsPath, entry.Name())
			var applicationRule *filters.ApplicationRule
			content, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			err = unmarshalConfig(content, file, &applicationRule)
			if err != nil {
				return err
			}
			if applicationRule.Rule != nil {
				applicationRule.BuildRuleFromRequest(cfg.GetCondition)
				cfg.Policy.ApplicationRules = append(cfg.Policy.ApplicationRules, applicationRule)
			}
		}
	}
	return nil
}

func readCommands(path string, cfg *UserConfig) error {
	modelsPath := filepath.Join(path, "policies", "commands")
	entries, err := os.ReadDir(modelsPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && isSupportedExt(filepath.Ext(entry.Name())) {
			file := filepath.Join(modelsPath, entry.Name())
			var command GenericCommand
			content, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			err = unmarshalConfig(content, file, &command)
			if err != nil {
				return err
			}
			if command.HandlerKey != "" {
				if handler := cfg.GetHandler(command.HandlerKey); handler != nil {
					command.Handler = *handler
				}
			}
			cfg.Policy.Commands = append(cfg.Policy.Commands, &command)
		}
	}
	return nil
}

func readDatabases(path string, cfg *UserConfig) error {
	path = filepath.Join(path, "credentials", "databases")
	return readArrayConfigFile(path, func(data metadata.Config) {
		cfg.Core.Credentials.Databases = append(cfg.Core.Credentials.Databases, data)
	})
}

func readStorages(path string, cfg *UserConfig) error {
	path = filepath.Join(path, "credentials", "storages")
	return readArrayConfigFile(path, func(data Storage) {
		cfg.Core.Credentials.Storages = append(cfg.Core.Credentials.Storages, data)
	})
}

func readCaches(path string, cfg *UserConfig) error {
	path = filepath.Join(path, "credentials", "caches")
	return readArrayConfigFile(path, func(data Cache) {
		cfg.Core.Credentials.Caches = append(cfg.Core.Credentials.Caches, data)
	})
}

func readCredentials(path string, cfg *UserConfig) error {
	if err := readDatabases(path, cfg); err != nil {
		return err
	}
	if err := readStorages(path, cfg); err != nil {
		return err
	}
	return readCaches(path, cfg)
}

func readConfig(path string, cfg *UserConfig) error {
	path = filepath.Join(path, "conf")
	return readConfigFile(path, func(coreData Core) {
		if cfg.Core.Enums == nil {
			cfg.Core.Enums = make(map[string]map[string]any)
		}
		for key, val := range coreData.Enums {
			cfg.Core.Enums[key] = val
		}
		if cfg.Core.Consts == nil {
			cfg.Core.Consts = make(map[string]any)
		}
		for key, val := range coreData.Consts {
			cfg.Core.Consts[key] = val
		}
	})
}

func readModels(path string, cfg *UserConfig) error {
	modelsPath := filepath.Join(path, "policies", "models")
	entries, err := os.ReadDir(modelsPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && isSupportedExt(filepath.Ext(entry.Name())) {
			file := filepath.Join(modelsPath, entry.Name())
			var model Model
			content, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			err = unmarshalConfig(content, file, &model)
			if err != nil {
				return err
			}
			fileName := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
			if model.Name == "" {
				model.Name = fileName
			}
			if cfg.GetModel(model.Name) == nil {
				cfg.Policy.Models = append(cfg.Policy.Models, model)
			}
		}
	}
	return nil
}

func readHandlers(path string, cfg *UserConfig) error {
	modelsPath := filepath.Join(path, "policies", "handlers")
	entries, err := os.ReadDir(modelsPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && isSupportedExt(filepath.Ext(entry.Name())) {
			file := filepath.Join(modelsPath, entry.Name())
			var handler Handler
			content, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			err = unmarshalConfig(content, file, &handler)
			if err != nil {
				return err
			}
			cfg.Policy.Handlers = append(cfg.Policy.Handlers, handler)
		}
	}
	return nil
}

func readApis(path string, cfg *UserConfig) error {
	modelsPath := filepath.Join(path, "policies", "apis")
	entries, err := os.ReadDir(modelsPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && isSupportedExt(filepath.Ext(entry.Name())) {
			file := filepath.Join(modelsPath, entry.Name())
			var api Api
			content, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			err = unmarshalConfig(content, file, &api)
			if err != nil {
				return err
			}
			for i, route := range api.Routes {
				if route.HandlerKey != "" {
					if handler := cfg.GetHandler(route.HandlerKey); handler != nil {
						route.Handler = *handler
						api.Routes[i] = route
					}
				}
			}
			cfg.Policy.Web.Apis = append(cfg.Policy.Web.Apis, api)
		}
	}
	return nil
}

func readBackgroundTasks(path string, cfg *UserConfig) error {
	modelsPath := filepath.Join(path, "policies", "background")
	entries, err := os.ReadDir(modelsPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && isSupportedExt(filepath.Ext(entry.Name())) {
			file := filepath.Join(modelsPath, entry.Name())
			var background BackgroundHandler
			content, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			err = unmarshalConfig(content, file, &background)
			if err != nil {
				return err
			}
			cfg.Policy.BackgroundHandlers = append(cfg.Policy.BackgroundHandlers, &background)
		}
	}
	return nil
}

func readRenderer(path string, cfg *UserConfig) error {
	modelsPath := filepath.Join(path, "policies", "renderer")
	entries, err := os.ReadDir(modelsPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && isSupportedExt(filepath.Ext(entry.Name())) {
			file := filepath.Join(modelsPath, entry.Name())
			var background RenderConfig
			content, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			err = unmarshalConfig(content, file, &background)
			if err != nil {
				return err
			}
			cfg.Policy.Web.Render = append(cfg.Policy.Web.Render, &background)
		}
	}
	return nil
}

func readWeb(path string, cfg *UserConfig) error {
	path = filepath.Join(path, "policies", "web")
	return readConfigFile(path, func(data Web) {
		cfg.Policy.Web = data
	})
}

func isSupportedExt(ext string) bool {
	return slices.Contains([]string{".json", ".yaml"}, ext)
}
