package utils

import (
	"context"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/oarkflow/squealx"
)

type QueryParams struct {
	Fields    string `json:"fields" query:"fields"`
	Except    string `json:"except" query:"except"`
	Join      string `json:"join" query:"join"`
	GroupBy   string `json:"group_by" query:"group_by"`
	Having    string `json:"having" query:"having"`
	SortField string `json:"sort_field" query:"sort_field"`
	SortDir   string `json:"sort_dir" query:"sort_dir"`
	Limit     int    `json:"limit" query:"limit"`
	Offset    int    `json:"offset" query:"offset"`
}

func ParseQueryParams(c *fiber.Ctx) {
	var query QueryParams
	var fields, except, join, groupBy []string
	if err := c.QueryParser(&query); err == nil {
		if query.Fields != "" {
			fields = strings.Split(query.Fields, ",")
		}
		if query.Except != "" {
			except = strings.Split(query.Except, ",")
		}
		if query.Join != "" {
			join = strings.Split(query.Join, ",")
		}
		if query.GroupBy != "" {
			groupBy = strings.Split(query.GroupBy, ",")
		}
		params := squealx.QueryParams{
			Fields:  fields,
			Except:  except,
			Join:    join,
			GroupBy: groupBy,
			Having:  query.Having,
			Sort: squealx.Sort{
				Field: query.SortField,
				Dir:   query.SortDir,
			},
			Limit:  query.Limit,
			Offset: query.Offset,
		}
		ctx := context.WithValue(c.UserContext(), "query_params", params)
		c.SetUserContext(ctx)
		c.Locals("query_params", params)
	}
}
