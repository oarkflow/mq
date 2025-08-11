package renderer

import (
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/oarkflow/jet"

	"github.com/oarkflow/mq/services/middlewares/rewrite"
)

type Config struct {
	Prefix   string `json:"prefix"`
	Root     string `json:"root"`
	Index    string `json:"index"`
	UseIndex bool   `json:"use_index"`
	Compress bool   `json:"compress"`
}

func New(router fiber.Router, cfg ...Config) error {
	var config Config
	if len(cfg) > 0 {
		config = cfg[0]
	}
	if config.Root == "" {
		config.Root = "./"
	}
	if config.Prefix == "/" {
		config.Prefix = ""
	}
	if config.UseIndex && config.Index == "" {
		config.Index = "index.html"
	}
	if config.Compress {
		router.Use(compress.New(compress.Config{
			Level: compress.LevelBestSpeed,
		}))
	}
	rules := make(map[string]string)
	root := filepath.Clean(config.Root)
	err := filepath.WalkDir(config.Root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			path = strings.TrimPrefix(path, root)
			rules[path] = filepath.Join(config.Prefix, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	router.Use(rewrite.New(rewrite.Config{Rules: rules}))
	router.Get(config.Prefix+"/*", handleStaticFile(config))
	return nil
}

func handleStaticFile(config Config) fiber.Handler {
	return func(c *fiber.Ctx) error {
		fullPath := c.Params("*")
		filePath := filepath.Join(config.Root, fullPath)
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return c.Status(fiber.StatusNotFound).SendString(fmt.Sprintf("File %s not found", filePath))
		}
		isIndex := false
		if fileInfo.IsDir() {
			if !config.UseIndex {
				return c.Status(fiber.StatusNotFound).SendString("Invalid file")
			}
			isIndex = true
			filePath = filepath.Join(filePath, config.Index)
		}
		fileContent, err := os.ReadFile(filePath)
		if err != nil {
			return c.Status(fiber.StatusNotFound).SendString(fmt.Sprintf("File %s not found", filePath))
		}
		if isIndex {
			parser := jet.NewWithMemory(jet.WithDelims("{{", "}}"))
			content, err := parser.ParseTemplate(string(fileContent), map[string]any{
				"json_data": map[string]any{},
			})
			if err != nil {
				return c.Status(fiber.StatusNotFound).SendString(err.Error())
			}
			fileContent = []byte(content)
		}
		ext := filepath.Ext(filePath)
		mimeType := mime.TypeByExtension(ext)
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		c.Set("Content-Type", mimeType)
		c.Set("Cache-Control", "public, max-age=31536000")
		return c.Send(fileContent)
	}
}
