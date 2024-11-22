package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Context struct {
	Query map[string]any
}

func (ctx *Context) Get(key string) string {
	if val, ok := ctx.Query[key]; ok {
		switch val := val.(type) {
		case []string:
			return val[0]
		case string:
			return val
		}
	}
	return ""
}

func parse(r *http.Request) (context.Context, []byte, error) {
	ctx := r.Context()
	userContext := &Context{Query: make(map[string]any)}
	queryParams := r.URL.Query()
	for key, values := range queryParams {
		if len(values) > 1 {
			userContext.Query[key] = values
		} else {
			userContext.Query[key] = values[0]
		}
	}
	ctx = context.WithValue(ctx, "UserContext", userContext)
	contentType := r.Header.Get("Content-Type")
	var result any
	switch {
	case contentType == "application/json":
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return ctx, nil, err
		}
		defer r.Body.Close()

		if len(body) == 0 {
			return ctx, nil, nil
		}
		var temp any
		if err := json.Unmarshal(body, &temp); err != nil {
			return ctx, nil, fmt.Errorf("failed to parse body: %v", err)
		}
		switch v := temp.(type) {
		case map[string]any:
			result = v
		case []any:
			parsedArray := make([]map[string]any, len(v))
			for i, item := range v {
				obj, ok := item.(map[string]any)
				if !ok {
					return ctx, nil, fmt.Errorf("invalid JSON array item at index %d", i)
				}
				parsedArray[i] = obj
			}
			result = parsedArray
		default:
			return ctx, nil, fmt.Errorf("unsupported JSON structure: %T", v)
		}
	case contentType == "application/x-www-form-urlencoded":
		if err := r.ParseForm(); err != nil {
			return ctx, nil, err
		}
		formResult := make(map[string]any)
		for key, values := range r.PostForm {
			if len(values) > 1 {
				formResult[key] = values
			} else {
				formResult[key] = values[0]
			}
		}
		result = formResult
	default:
		return ctx, nil, nil
	}
	bt, err := json.Marshal(result)
	if err != nil {
		return ctx, nil, err
	}
	return ctx, bt, err
}

func UserContext(ctx context.Context) *Context {
	if userContext, ok := ctx.Value("UserContext").(*Context); ok {
		return userContext
	}
	return &Context{Query: make(map[string]any)}
}
