package services

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/oarkflow/filters"
	"github.com/oarkflow/form"
	"github.com/oarkflow/json"
	"github.com/oarkflow/log"
	"github.com/oarkflow/protocol/utils/str"

	"github.com/oarkflow/mq"
	"github.com/oarkflow/mq/consts"
	"github.com/oarkflow/mq/dag"
	"github.com/oarkflow/mq/services/http/responses"
	"github.com/oarkflow/mq/services/middlewares"
	"github.com/oarkflow/mq/services/renderer"
	"github.com/oarkflow/mq/services/utils"
)

var ValidationInstance Validation

func Setup(loader *Loader, serverApp *fiber.App, brokerAddr string) error {
	if loader.UserConfig == nil {
		return nil
	}
	return SetupServices(loader.Prefix(), serverApp, brokerAddr)
}

func SetupHandler(handler Handler, brokerAddr string, async ...bool) *dag.DAG {
	syncMode := true
	if len(async) > 0 {
		syncMode = async[0]
	}
	key := handler.Key
	existingDAG := dag.GetDAG(key)
	if existingDAG != nil {
		return existingDAG
	}
	opts := []mq.Option{
		mq.WithSyncMode(syncMode),
		mq.WithBrokerURL(brokerAddr),
	}
	if handler.DisableLog {
		opts = append(opts, mq.WithLogger(nil))
	}
	flow := dag.NewDAG(handler.Name, handler.Key, nil, opts...)
	flow.SetDebug(handler.Debug)
	for _, node := range handler.Nodes {
		if node.Node == "" && node.NodeKey == "" {
			flow.Error = errors.New("Node not defined " + node.ID)
			return flow
		}
		if node.Node != "" {
			err := prepareNode(flow, node)
			if err != nil {
				flow.Error = err
				return flow
			}
		} else if node.NodeKey != "" {
			newDag := dag.GetDAG(node.NodeKey)
			if newDag == nil {
				flow.Error = errors.New("DAG not found " + node.NodeKey + " inside flow " + flow.GetKey())
				return flow
			}
			nodeType := dag.Function
			if newDag.HasPageNode() {
				nodeType = dag.Page
			}
			flow.AddDAGNode(nodeType, node.Name, node.ID, newDag, node.FirstNode)
		}
	}
	for _, edge := range handler.Edges {
		if edge.Label == "" {
			edge.Label = fmt.Sprintf("edge-%s", edge.Source)
		}
		flow.AddEdge(dag.Simple, edge.Label, edge.Source, edge.Target...)
		if flow.Error != nil {
			return flow
		}
	}
	for _, edge := range handler.Loops {
		if edge.Label == "" {
			edge.Label = fmt.Sprintf("loop-%s", edge.Source)
		}
		flow.AddEdge(dag.Iterator, edge.Label, edge.Source, edge.Target...)
	}
	err := flow.Validate()
	if err != nil {
		flow.Error = err
		return flow
	}
	dag.AddDAG(key, flow)
	return flow
}

type FilterGroup struct {
	Operator string            `json:"operator"`
	Reverse  bool              `json:"reverse"`
	Filters  []*filters.Filter `json:"filters"`
}

type Filter struct {
	Filter      *filters.Filter `json:"condition"`
	FilterGroup *FilterGroup    `json:"group"`
	Node        string          `json:"node"`
	ID          string          `json:"id"`
}

func prepareNode(flow *dag.DAG, node Node) error {
	newHandler := dag.GetHandler(node.Node)
	if newHandler == nil {
		return errors.New("Handler not found " + node.Node)
	}
	nodeHandler := newHandler(node.ID)
	providers := mapProviders(node.Data.Providers)
	schemaFile, ok := node.Data.AdditionalData["schema_file"].(string)
	if ok && schemaFile != "" {
		schema := GetUserConfig().GetSchemaInstance(schemaFile)
		if schema == nil {
			return fmt.Errorf("schema file %s not found", schemaFile)
		}
		node.Data.AdditionalData["__schema"] = schema.Instance
	}
	switch nodeHandler := nodeHandler.(type) {
	case dag.ConditionProcessor:
		nodeHandler.SetConfig(dag.Payload{
			Mapping:         node.Data.Mapping,
			Data:            node.Data.AdditionalData,
			GeneratedFields: node.Data.GeneratedFields,
			Providers:       providers,
		})
		if s, ok := node.Data.AdditionalData["conditions"]; ok {
			var fil map[string]*Filter
			err := Map(&fil, s)
			if err != nil {
				return err
			}
			condition := make(map[string]string)
			conditions := make(map[string]dag.Condition)
			for key, cond := range fil {
				condition[key] = cond.Node
				if cond.Filter != nil {
					conditions[key] = cond.Filter
				} else if cond.FilterGroup != nil {
					cond.FilterGroup.Operator = strings.ToUpper(cond.FilterGroup.Operator)
					if !slices.Contains([]string{"AND", "OR"}, cond.FilterGroup.Operator) {
						cond.FilterGroup.Operator = "AND"
					}
					var fillers []filters.Condition
					for _, f := range cond.FilterGroup.Filters {
						if f != nil {
							fillers = append(fillers, f)
						}
					}
					conditions[key] = &filters.FilterGroup{
						Operator: filters.Boolean(cond.FilterGroup.Operator),
						Reverse:  cond.FilterGroup.Reverse,
						Filters:  fillers,
					}
				} else {
					conditions[key] = nil
				}
			}
			flow.AddCondition(node.ID, condition)
			nodeHandler.SetConditions(conditions)
		}
	case dag.Processor:
		nodeHandler.SetConfig(dag.Payload{
			Mapping:         node.Data.Mapping,
			Data:            node.Data.AdditionalData,
			GeneratedFields: node.Data.GeneratedFields,
			Providers:       providers,
		})
	}
	var nodeType dag.NodeType
	if nodeHandler.GetType() == "Function" {
		nodeType = dag.Function
	} else if nodeHandler.GetType() == "Page" {
		nodeType = dag.Page
	}
	if node.Name == "" {
		node.Name = node.ID
	}
	flow.AddNodeWithDebug(nodeType, node.Name, node.ID, nodeHandler, node.Debug, node.FirstNode)
	return nil
}

func mapProviders(dataProviders interface{}) []dag.Provider {
	var providers []dag.Provider
	err := Map(&providers, dataProviders)
	if err != nil {
		log.Warn().Err(err).Msg("Unable to map providers")
	}
	return providers
}

func setupBackgroundHandlers(brokerAddress string) {
	for _, cmd := range userConfig.Policy.BackgroundHandlers {
		if cmd.Handler.Key == "" && cmd.HandlerKey != "" {
			handler := userConfig.GetHandler(cmd.HandlerKey)
			if handler == nil {
				panic(fmt.Sprintf("Handler not found %s", cmd.HandlerKey))
			}
			cmd.Handler = *handler
		}
		flow := SetupHandler(cmd.Handler, brokerAddress)
		if flow.Error != nil {
			panic(flow.Error)
		}
		flow.AssignTopic(cmd.Queue)
		if cmd.Schedule != nil && cmd.Schedule.Enable {
			duration, err := utils.ParseDuration(cmd.Schedule.Interval)
			if err != nil {
				duration = time.Minute
			}
			go func() {
				time.Sleep(5 * time.Second)
				flow.ScheduleTask(context.Background(), cmd.Payload, mq.WithInterval(duration))
			}()
		}
		go func() {
			err := flow.Consume(context.Background())
			if err != nil {
				panic(err)
			}
		}()
	}
}

func SetupServices(prefix string, router fiber.Router, brokerAddr string) error {
	if router == nil {
		return nil
	}
	err := SetupHandlers(userConfig.Policy.Handlers, brokerAddr)
	if err != nil {
		return err
	}
	setupBackgroundHandlers(brokerAddr)
	static := userConfig.Policy.Web.Static
	if static != nil && static.Dir != "" {
		router.Static(
			static.Prefix,
			static.Dir,
			fiber.Static{
				Compress:  static.Options.Compress,
				ByteRange: static.Options.ByteRange,
				Browse:    static.Options.Browse,
				Index:     static.Options.IndexFile,
			},
		)
	}
	err = setupRender(prefix, router)
	if err != nil {
		return fmt.Errorf("failed to setup render: %w", err)
	}
	return SetupAPI(prefix, router, brokerAddr)
}

func SetupAPI(prefix string, router fiber.Router, brokerAddr string) error {
	if prefix != "" {
		prefix = "/" + prefix
	}
	api := router.Group(prefix)
	for _, configRoute := range userConfig.Policy.Web.Apis {
		routeGroup := api.Group(configRoute.Prefix)
		mws := setupMiddlewares(configRoute.Middlewares...)
		if len(mws) > 0 {
			routeGroup.Use(mws...)
		}
		for _, route := range configRoute.Routes {
			switch route.Operation {
			case "custom":
				flow := setupFlow(route, routeGroup, brokerAddr)
				routeMiddlewares := setupMiddlewares(route.Middlewares...)
				if len(routeMiddlewares) > 0 {
					routeGroup.Use(routeMiddlewares...)
				}
				routeGroup.Add("GET", CleanAndMergePaths(route.Uri, "/metadata"), func(ctx *fiber.Ctx) error {
					return getDAGPage(ctx, flow)
				})
				mw := []fiber.Handler{
					requestMiddleware(CleanAndMergePaths(prefix, configRoute.Prefix), route),
					ruleMiddleware(route.Rules),
					customRuleMiddleware(route, route.CustomRules),
				}
				if flow.HasPageNode() {
					mw = append(mw, flow.RenderFiber)
					routeGroup.All(route.Uri, mw...)

				} else {
					mw = append(mw, customHandler(flow))
					routeGroup.Add(strings.ToUpper(route.Method), route.Uri, mw...)
				}
			}
		}
	}
	return nil
}

// GetRulesFromKeys returns the custom rules from the provided keys.
// It is used by the CustomRuleMiddleware to get the custom rules from the provided keys.
func GetRulesFromKeys(ruleKeys []string) (rulesArray []*filters.RuleRequest) {
	for _, ruleKey := range ruleKeys {
		appRules := userConfig.GetApplicationRule(ruleKey)
		if appRules == nil {
			panic(fmt.Sprintf("Rule %v not found", ruleKey))
		}
		if appRules.Rule != nil {
			rulesArray = append(rulesArray, appRules.Rule)
		}
	}
	return
}

// customRuleMiddleware validates the request body with the provided custom rules.
// It is passed after the ruleMiddleware to validate the request body with the custom rules.
func customRuleMiddleware(route *Route, ruleKeys []string) fiber.Handler {
	rules := GetRulesFromKeys(ruleKeys)
	return func(ctx *fiber.Ctx) error {
		c, requestData, err := getLessRequestData(ctx, route)
		ctx.SetUserContext(c)
		if err != nil {
			return responses.Abort(ctx, 400, "invalid request", err.Error())
		}
		if len(requestData) > 0 {
			header, ok := ctx.Context().Value("header").(map[string]any)
			if ok {
				requestData["header"] = header
			}
			data := map[string]any{
				"data": requestData,
			}
			for _, r := range rules {
				_, err := r.Validate(data)
				if err != nil {
					var errResponse *filters.ErrorResponse
					errors.As(err, &errResponse)
					if slices.Contains([]string{"DENY", "DENY_WITH_WARNING"}, errResponse.ErrorAction) {
						return responses.Abort(ctx, 400, "Invalid data for the request", err.Error())
					} else {
						ctx.Set("error_msg", errResponse.ErrorMsg)
					}
				}
			}
		}
		return ctx.Next()
	}
}

// getLessRequestData returns request data with param, query, body, enums, consts except
// restricted_field, scopes and queues
func getLessRequestData(ctx *fiber.Ctx, route *Route) (context.Context, map[string]any, error) {
	request, header, err := prepareHeader(ctx, route)
	if header != nil {
		header["route_model"] = route.Model
	}
	ctx.Set("route_model", route.Model)
	if err != nil {
		return ctx.UserContext(), nil, err
	}
	c := context.WithValue(ctx.UserContext(), "header", header)
	return c, request, nil
}

func prepareHeader(ctx *fiber.Ctx, route *Route) (map[string]any, map[string]any, error) {
	var request map[string]any
	bodyRaw := ctx.BodyRaw()
	if str.FromByte(bodyRaw) != "" {
		err := json.Unmarshal(bodyRaw, &request)
		if err != nil {
			form, err := ctx.MultipartForm()
			if err == nil || form != nil {
				return nil, nil, errors.New("invalid json request")
			}
		}
	}
	if request == nil {
		request = make(map[string]any)
	}
	requiredBody := make(map[string]bool)
	header := make(map[string]any)
	param := make(map[string]any)
	query := make(map[string]any)
	schema := route.GetSchema()
	if schema != nil {
		if schema.Properties != nil {
			for key, property := range *schema.Properties {
				if property.In != nil {
					for _, in := range property.In {
						switch in {
						case "param":
							param[key] = ctx.Params(key)
						case "query":
							query[key] = ctx.Query(key)
						case "body":
							requiredBody[key] = true
						}
					}
				}
			}
		}
	}
	header["param"] = param
	header["query"] = query
	header["route_model"] = route.Model
	ctx.Set("route_model", route.Model)
	for k := range requiredBody {
		if _, ok := request[k]; !ok {
			delete(request, k)
		}
	}
	header["request_id"] = ctx.Get("X-Schema-Id")
	// add consts and enums to request
	header["consts"] = userConfig.Core.Consts
	header["enums"] = userConfig.Core.Enums
	return request, header, nil
}

func customHandler(flow *dag.DAG) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		result := flow.Process(ctx.UserContext(), ctx.BodyRaw())
		if result.Error != nil {
			return result.Error
		}
		contentType := result.Ctx.Value(consts.ContentType)
		if contentType == nil {
			return ctx.JSON(result)
		}
		if contentType == fiber.MIMEApplicationJSON || contentType == fiber.MIMEApplicationJSONCharsetUTF8 {
			return ctx.JSON(result)
		}
		var resultData map[string]any
		if err := json.Unmarshal(result.Payload, &resultData); err != nil {
			return ctx.JSON(fiber.Map{"success": false, "error": "Invalid response payload"})
		}
		ctx.Set(consts.ContentType, contentType.(string))
		html, _ := resultData["html_content"].(string)
		return ctx.SendString(html)
	}
}

func getDAGPage(ctx *fiber.Ctx, flow *dag.DAG) error {
	image := fmt.Sprintf("%s.svg", mq.NewID())
	if err := flow.SaveSVG(image); err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(image)
	}()
	svgBytes, err := os.ReadFile(image)
	if err != nil {
		return err
	}
	htmlContent := flow.SVGViewerHTML(string(svgBytes))
	ctx.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
	return ctx.SendString(htmlContent)
}

// ruleMiddleware validates the request body with the provided rules.
// It is passed after the requestMiddleware to ensure that the request body is valid.
func ruleMiddleware(rules map[string]string) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		body := ctx.Body()
		if len(body) == 0 {
			return ctx.Next()
		}
		requestData, err := form.DecodeForm(body)
		if err != nil && body != nil {
			return responses.Abort(ctx, 400, "Invalid request bind", nil)
		}
		if len(rules) > 0 && ValidationInstance != nil {
			validator, err := ValidationInstance.Make(ctx, requestData, rules)
			if err != nil {
				return responses.Abort(ctx, 400, "Validation Error", err.Error())
			}
			if validator.Fails() {
				return responses.Abort(ctx, 400, "Validation Error", validator.Errors().All())
			}
		}
		return ctx.Next()
	}
}

func setupRender(_ string, clientRoutes fiber.Router) error {
	for _, cfg := range userConfig.Policy.Web.Render {
		if cfg.Root != "" && cfg.ID != "" {
			err := renderer.New(clientRoutes, renderer.Config{
				Prefix:   cfg.Prefix,
				Root:     cfg.Root,
				Index:    cfg.Index,
				UseIndex: cfg.UseIndex,
				Compress: cfg.Compress,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// requestMiddleware validates the request body in the original form of byte array
// against the provided request JSON schema to ensure that the request body is valid.
func requestMiddleware(prefix string, route *Route) fiber.Handler {
	return func(ctx *fiber.Ctx) error {
		schema := route.GetSchema()
		if schema == nil {
			return ctx.Next()
		}
		requestSchema := ctx.Query("request-schema")
		if requestSchema != "" {
			return ctx.JSON(fiber.Map{
				"success": true,
				"code":    200,
				"data": fiber.Map{
					"schema": schema,
					"rules":  route.Rules,
				},
			})
		}
		for _, r := range userConfig.Policy.Models {
			if r.Name == route.Model {
				db := r.Database
				source := route.Model
				ctx.Locals("database_connection", db)
				ctx.Locals("database_source", source)
				break
			}
		}
		multipartForm, _ := ctx.MultipartForm()
		if multipartForm != nil {
			return ctx.Next()
		}
		return middlewares.ValidateRequestBySchema(schema, ctx)
	}
}

func setupMiddlewares(middlewares ...Middleware) (mid []any) {
	for _, middleware := range middlewares {
		switch middleware.Name {
		case "cors":
			mid = append(mid, cors.New(cors.Config{ExposeHeaders: "frame-session"}))
		case "basic-auth":
			options := struct {
				Users map[string]string `json:"users"`
			}{}
			err := json.Unmarshal(middleware.Options, &options)
			if err != nil {
				panic(err)
			}
			mid = append(mid, basicauth.New(basicauth.Config{Users: options.Users}))
		case "rate-limit":
			options := struct {
				Max        int    `json:"max"`
				Expiration string `json:"expiration"`
			}{}
			err := json.Unmarshal(middleware.Options, &options)
			if err != nil {
				panic(err)
			}

			expiration, err := utils.ParseDuration(options.Expiration)
			if err != nil {
				panic(err)
			}
			throttle := limiter.New(limiter.Config{Max: options.Max, Expiration: expiration})
			mid = append(mid, throttle)
		}
	}
	return
}

func setupFlow(route *Route, _ fiber.Router, brokerAddr string) *dag.DAG {
	if route.Handler.Key == "" && route.HandlerKey != "" {
		handler := userConfig.GetHandler(route.HandlerKey)
		if handler == nil {
			panic(fmt.Sprintf("Handler not found %s", route.HandlerKey))
		}
		route.Handler = *handler
	}
	flow := SetupHandler(route.Handler, brokerAddr)
	if flow.Error != nil {
		panic(flow.Error)
	}
	return flow
}

func CleanAndMergePaths(uri ...string) string {
	paths := make([]string, 0)
	for _, u := range uri {
		if u != "" {
			paths = append(paths, strings.TrimPrefix(u, "/"))
		}
	}
	return "/" + filepath.Clean(strings.Join(paths, "/"))
}

// HandlerInfo holds handler data and its dependencies
type HandlerInfo struct {
	Handler      Handler
	Dependencies []string
}

func SetupHandlers(availableHandlers []Handler, brokerAddr string) error {
	handlerInfos, err := PrepareDependencies(availableHandlers)
	if err != nil {
		return fmt.Errorf("failed to prepare handler dependencies: %w", err)
	}
	setupOrder, err := TopologicalSort(handlerInfos)
	if err != nil {
		return fmt.Errorf("failed to resolve handler dependencies: %w", err)
	}
	for _, file := range setupOrder {
		info := handlerInfos[file]
		fmt.Printf("Setting up handler: %s (key: %s)\n", file, info.Handler.Key)
		flow := SetupHandler(info.Handler, brokerAddr)
		if flow.Error != nil {
			return fmt.Errorf("failed to setup handler %s: %w", file, flow.Error)
		}
	}
	return nil
}

func PrepareDependencies(availableHandlers []Handler) (map[string]*HandlerInfo, error) {
	handlerInfos := make(map[string]*HandlerInfo)
	for _, handler := range availableHandlers {
		dependencies := GetDependencies(handler)
		handlerInfos[handler.Key] = &HandlerInfo{
			Handler:      handler,
			Dependencies: dependencies,
		}
	}
	return handlerInfos, nil
}

// GetDependencies extracts node_key dependencies from a handler
func GetDependencies(handler Handler) []string {
	var deps []string
	for _, node := range handler.Nodes {
		if node.NodeKey != "" {
			deps = append(deps, node.NodeKey)
		}
	}
	return deps
}

// TopologicalSort performs dependency-based ordering of handlers
func TopologicalSort(handlers map[string]*HandlerInfo) ([]string, error) {
	inDegree := make(map[string]int)
	adjList := make(map[string][]string)
	for key := range handlers {
		inDegree[key] = 0
		adjList[key] = []string{}
	}
	for key, info := range handlers {
		for _, dep := range info.Dependencies {
			for depKey, depInfo := range handlers {
				if depInfo.Handler.Key == dep {
					adjList[depKey] = append(adjList[depKey], key)
					inDegree[key]++
					break
				}
			}
		}
	}
	queue := []string{}
	result := []string{}
	for key, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, key)
		}
	}
	sort.Strings(queue)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)
		neighbors := adjList[current]
		sort.Strings(neighbors)
		for _, neighbor := range neighbors {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
				sort.Strings(queue)
			}
		}
	}
	if len(result) != len(handlers) {
		return nil, fmt.Errorf("circular dependency detected in handlers")
	}
	return result, nil
}
