package main

import (
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/rewrite"
)

type Config struct {
	Prefix   string `json:"prefix"`
	Root     string `json:"root"`
	Index    string `json:"index"`
	UseIndex bool   `json:"use_index"`
	Compress bool   `json:"compress"`
}

func main() {
	app := fiber.New()
	config := Config{
		Prefix:   "/data",
		Root:     "/Users/sujit/Sites/mq/examples/webroot",
		UseIndex: true,
		Compress: true,
	}
	New(app, config)
	log.Fatal(app.Listen(":3000"))
}

func New(router fiber.Router, cfg ...Config) {
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
	filepath.WalkDir(config.Root, func(path string, d os.DirEntry, err error) error {
		if !d.IsDir() {
			path = strings.TrimPrefix(path, root)
			rules[path] = filepath.Join(config.Prefix, path)
		}
		return nil
	})
	router.Use(rewrite.New(rewrite.Config{Rules: rules}))
	router.Get(config.Prefix+"/*", handleStaticFile(config))
}

func handleStaticFile(config Config) fiber.Handler {
	return func(c *fiber.Ctx) error {
		fullPath := c.Params("*")
		filePath := filepath.Join(config.Root, fullPath)
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return c.Status(fiber.StatusNotFound).SendString("File not found")
		}
		if fileInfo.IsDir() {
			if !config.UseIndex {
				return c.Status(fiber.StatusNotFound).SendString("Invalid file")
			}
			filePath = filepath.Join(filePath, config.Index)
		}
		fileContent, err := os.ReadFile(filePath)
		if err != nil {
			return c.Status(fiber.StatusNotFound).SendString("File not found")
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
