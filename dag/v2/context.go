package v2

import (
	"context"
	"encoding/json"
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
	result := make(map[string]any)
	queryParams := r.URL.Query()
	for key, values := range queryParams {
		if len(values) > 1 {
			userContext.Query[key] = values // Handle multiple values
		} else {
			userContext.Query[key] = values[0] // Single value
		}
	}
	ctx = context.WithValue(ctx, "UserContext", userContext)
	contentType := r.Header.Get("Content-Type")
	switch {
	case contentType == "application/json":
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return ctx, nil, err
		}
		defer r.Body.Close()
		if body == nil {
			return ctx, nil, nil
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return ctx, nil, err
		}

	case contentType == "application/x-www-form-urlencoded":
		if err := r.ParseForm(); err != nil {
			return ctx, nil, err
		}
		result = make(map[string]any)
		for key, values := range r.PostForm {
			if len(values) > 1 {
				result[key] = values
			} else {
				result[key] = values[0]
			}
		}
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
