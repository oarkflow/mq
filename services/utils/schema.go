package utils

import (
	"log"
	"sync"

	"github.com/oarkflow/json"
	v2 "github.com/oarkflow/jsonschema"
)

type Schema struct {
	m     sync.RWMutex
	items map[string]*v2.Schema
}

var (
	CompiledSchemas *Schema
	compiler        *v2.Compiler
)

func init() {
	compiler = v2.NewCompiler()
	CompiledSchemas = &Schema{items: make(map[string]*v2.Schema)}
}

func AddSchema(key string, schema *v2.Schema) {
	CompiledSchemas.m.Lock()
	defer CompiledSchemas.m.Unlock()
	CompiledSchemas.items[key] = schema
}

func GetSchema(key string) (*v2.Schema, bool) {
	CompiledSchemas.m.Lock()
	defer CompiledSchemas.m.Unlock()
	schema, ok := CompiledSchemas.items[key]
	return schema, ok
}

func CompileSchema(uri, method string, schema json.RawMessage) (*v2.Schema, error) {
	s, err := compiler.Compile(schema)
	if err != nil {
		log.Printf("Error compiling schema for %s %s: %v", method, uri, err)
		return nil, err
	}
	key := method + ":" + uri
	AddSchema(key, s)
	return s, nil
}
