package middlewares

import (
	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/json"
	"github.com/oarkflow/jsonschema"
	"github.com/oarkflow/jsonschema/request"
)

var ServerApp *fiber.App

func MatchRoute(pattern, path string) (bool, map[string]string) {
	params := make(map[string]string)
	pi, ti := 0, 0
	pLen, tLen := len(pattern), len(path)
	skipSlash := func(s string, i int) int {
		for i < len(s) && s[i] == '/' {
			i++
		}
		return i
	}
	pi = skipSlash(pattern, pi)
	ti = skipSlash(path, ti)
	for pi < pLen && ti < tLen {
		switch pattern[pi] {
		case ':':
			startName := pi + 1
			for pi < pLen && pattern[pi] != '/' {
				pi++
			}
			paramName := pattern[startName:pi]
			startVal := ti
			for ti < tLen && path[ti] != '/' {
				ti++
			}
			paramVal := path[startVal:ti]
			params[paramName] = paramVal
		case '*':
			pi++
			if pi < pLen && pattern[pi] == '/' {
				pi++
			}
			paramName := pattern[pi:]
			paramVal := path[ti:]
			params[paramName] = paramVal
			ti = tLen
			pi = pLen
			break
		default:
			for pi < pLen && ti < tLen && pattern[pi] != '/' && path[ti] != '/' {
				if pattern[pi] != path[ti] {
					return false, nil
				}
				pi++
				ti++
			}
		}
		pi = skipSlash(pattern, pi)
		ti = skipSlash(path, ti)
	}
	if pi == pLen && ti == tLen {
		return true, params
	}
	return false, nil
}

func MatchRouterPath(method, path string) (fiber.Route, bool, map[string]string) {
	if ServerApp == nil {
		return fiber.Route{}, false, nil
	}
	for _, route := range ServerApp.GetRoutes() {
		if route.Method == method {
			matched, params := MatchRoute(route.Path, path)
			if matched {
				return route, matched, params
			}
		}
	}
	return fiber.Route{}, false, nil
}

// ValidateRequestBySchema - validates each request that has schema validation
func ValidateRequestBySchema(schema *jsonschema.Schema, c *fiber.Ctx) error {
	body := c.Body()
	if len(body) == 0 {
		return c.Next()
	}
	var intermediate any
	if err := request.UnmarshalFiberCtx(schema, c, &intermediate); err != nil {
		return err
	}
	mergedBytes, err := json.Marshal(intermediate)
	if err != nil {
		return err
	}
	c.Request().SetBody(mergedBytes)
	return c.Next()
}
