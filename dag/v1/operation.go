package v1

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/date"
	"github.com/oarkflow/dipper"
	"github.com/oarkflow/errors"
	"github.com/oarkflow/expr"
	"github.com/oarkflow/xid"
	"golang.org/x/exp/maps"

	"github.com/oarkflow/mq"
)

type Processor interface {
	mq.Processor
	SetConfig(Payload)
}

type Condition interface {
	Match(data any) bool
}

type ConditionProcessor interface {
	Processor
	SetConditions(map[string]Condition)
}

type Provider struct {
	Mapping       map[string]any `json:"mapping"`
	UpdateMapping map[string]any `json:"update_mapping"`
	InsertMapping map[string]any `json:"insert_mapping"`
	Defaults      map[string]any `json:"defaults"`
	ProviderType  string         `json:"provider_type"`
	Database      string         `json:"database"`
	Source        string         `json:"source"`
	Query         string         `json:"query"`
}

type Payload struct {
	Data            map[string]any    `json:"data"`
	Mapping         map[string]string `json:"mapping"`
	GeneratedFields []string          `json:"generated_fields"`
	Providers       []Provider        `json:"providers"`
}

type Operation struct {
	ID              string   `json:"id"`
	Type            string   `json:"type"`
	Key             string   `json:"key"`
	RequiredFields  []string `json:"required_fields"`
	OptionalFields  []string `json:"optional_fields"`
	GeneratedFields []string `json:"generated_fields"`
	Payload         Payload
}

func (e *Operation) Consume(_ context.Context) error {
	return nil
}

func (e *Operation) Pause(_ context.Context) error {
	return nil
}

func (e *Operation) Resume(_ context.Context) error {
	return nil
}

func (e *Operation) Stop(_ context.Context) error {
	return nil
}

func (e *Operation) Close() error {
	return nil
}

func (e *Operation) ProcessTask(_ context.Context, task *mq.Task) mq.Result {
	return mq.Result{Payload: task.Payload}
}

func (e *Operation) SetConfig(payload Payload) {
	e.Payload = payload
	e.GeneratedFields = slices.Compact(append(e.GeneratedFields, payload.GeneratedFields...))
}

func (e *Operation) GetType() string {
	return e.Type
}

func (e *Operation) GetKey() string {
	return e.Key
}

func (e *Operation) SetKey(key string) {
	e.Key = key
}

func (e *Operation) ValidateFields(c context.Context, payload []byte) (map[string]any, error) {
	var keys []string
	var data map[string]any
	err := json.Unmarshal(payload, &data)
	if err != nil {
		return nil, err
	}
	for k, v := range e.Payload.Mapping {
		_, val := GetVal(c, v, data)
		if val != nil {
			keys = append(keys, k)
		}
	}
	for k := range e.Payload.Data {
		keys = append(keys, k)
	}
	for _, k := range e.RequiredFields {
		if !slices.Contains(keys, k) {
			return nil, errors.New("Required field doesn't exist")
		}
	}
	return data, nil
}

func GetVal(c context.Context, v string, data map[string]any) (key string, val any) {
	key, val = getVal(c, v, data)
	if val == nil {
		if strings.Contains(v, "+") {
			vPartsG := strings.Split(v, "+")
			var value []string
			for _, v := range vPartsG {
				key, val = getVal(c, strings.TrimSpace(v), data)
				if val == nil {
					continue
				}
				value = append(value, val.(string))
			}
			val = strings.Join(value, "")
		} else {
			key, val = getVal(c, v, data)
		}
	}

	return
}

func Header(c context.Context, headerKey string) (val map[string]any, exists bool) {
	header := c.Value("header")
	switch header := header.(type) {
	case map[string]any:
		if p, exist := header[headerKey]; exist && p != nil {
			val = p.(map[string]any)
			exists = exist
		}
	}
	return
}

func HeaderVal(c context.Context, headerKey string, key string) (val any) {
	header := c.Value("header")
	switch header := header.(type) {
	case map[string]any:
		if p, exists := header[headerKey]; exists && p != nil {
			headerField := p.(map[string]any)
			if v, e := headerField[key]; e {
				val = v
			}
		}
	}
	return
}

func getVal(c context.Context, v string, data map[string]any) (key string, val any) {
	var param, query, consts map[string]any
	var enums map[string]map[string]any
	headerData := make(map[string]any)
	header := c.Value("header")
	switch header := header.(type) {
	case map[string]any:
		if p, exists := header["param"]; exists && p != nil {
			param = p.(map[string]any)
		}
		if p, exists := header["query"]; exists && p != nil {
			query = p.(map[string]any)
		}
		if p, exists := header["consts"]; exists && p != nil {
			consts = p.(map[string]any)
		}
		if p, exists := header["enums"]; exists && p != nil {
			enums = p.(map[string]map[string]any)
		}
		params := []string{"param", "query", "consts", "enums", "scopes"}
		// add other data in header, other than param, query, consts, enums to data
		for k, v := range header {
			if !slices.Contains(params, k) {
				headerData[k] = v
			}
		}
	}
	v = strings.TrimPrefix(v, "header.")
	vParts := strings.Split(v, ".")
	switch vParts[0] {
	case "body":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range data {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := data[v]; ok {
				val = vd
				key = v
			}
		}
	case "param":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range param {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := param[v]; ok {
				val = vd
				key = v
			}
		}
	case "query":
		v := vParts[1]
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range query {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			if vd, ok := query[v]; ok {
				val = vd
				key = v
			}
		}
	case "eval":
		// connect string except the first one if more than two parts exist
		var v string
		if len(vParts) > 2 {
			v = strings.Join(vParts[1:], ".")
		} else {
			v = vParts[1]
		}
		// remove '{{' and '}}'
		v = v[2 : len(v)-2]

		// parse the expression
		p, err := expr.Parse(v)
		if err != nil {
			return "", nil
		}
		// evaluate the expression
		val, err := p.Eval(data)
		if err != nil {
			val, err := p.Eval(headerData)
			if err == nil {
				return v, val
			}
			return "", nil
		} else {
			return v, val
		}
	case "eval_raw", "gorm_eval":
		// connect string except the first one if more than two parts exist
		var v string
		if len(vParts) > 2 {
			v = strings.Join(vParts[1:], ".")
		} else {
			v = vParts[1]
		}
		// remove '{{' and '}}'
		v = v[2 : len(v)-2]

		// parse the expression
		p, err := expr.Parse(v)
		if err != nil {
			return "", nil
		}
		dt := map[string]any{
			"header": header,
		}
		for k, vt := range data {
			dt[k] = vt
		}
		// evaluate the expression
		val, err := p.Eval(dt)
		if err != nil {
			val, err := p.Eval(headerData)
			if err == nil {
				return v, val
			}
			return "", nil
		} else {
			return v, val
		}
	case "consts":
		constG := vParts[1]
		if constVal, ok := consts[constG]; ok {
			val = constVal
			key = v
		}
	case "enums":
		enumG := vParts[1]
		if enumGVal, ok := enums[enumG]; ok {
			if enumVal, ok := enumGVal[vParts[2]]; ok {
				val = enumVal
				key = v
			}
		}
	default:
		if strings.Contains(v, "*_") {
			fieldSuffix := strings.ReplaceAll(v, "*", "")
			for k, vt := range data {
				if strings.HasSuffix(k, fieldSuffix) {
					val = vt
					key = k
				}
			}
		} else {
			vd, err := dipper.Get(data, v)
			if err == nil {
				val = vd
				key = v
			} else {
				vd, err := dipper.Get(headerData, v)
				if err == nil {
					val = vd
					key = v
				}
			}
		}
	}
	return
}

func init() {
	// define custom functions for use in config
	expr.AddFunction("trim", func(params ...interface{}) (interface{}, error) {
		if len(params) == 0 || len(params) > 1 || params[0] == nil {
			return nil, errors.New("Invalid number of arguments")
		}
		val, ok := params[0].(string)
		if !ok {
			return nil, errors.New("Invalid argument type")
		}
		return strings.TrimSpace(val), nil
	})
	expr.AddFunction("upper", func(params ...interface{}) (interface{}, error) {
		if len(params) == 0 || len(params) > 1 || params[0] == nil {
			return nil, errors.New("Invalid number of arguments")
		}
		val, ok := params[0].(string)
		if !ok {
			return nil, errors.New("Invalid argument type")
		}
		return strings.ToUpper(val), nil
	})
	expr.AddFunction("lower", func(params ...interface{}) (interface{}, error) {
		if len(params) == 0 || len(params) > 1 || params[0] == nil {
			return nil, errors.New("Invalid number of arguments")
		}
		val, ok := params[0].(string)
		if !ok {
			return nil, errors.New("Invalid argument type")
		}
		return strings.ToLower(val), nil
	})
	expr.AddFunction("date", func(params ...interface{}) (interface{}, error) {
		if len(params) == 0 || len(params) > 1 || params[0] == nil {
			return nil, errors.New("Invalid number of arguments")
		}
		val, ok := params[0].(string)
		if !ok {
			return nil, errors.New("Invalid argument type")
		}
		t, err := date.Parse(val)
		if err != nil {
			return nil, err
		}
		return t.Format("2006-01-02"), nil
	})
	expr.AddFunction("datetime", func(params ...interface{}) (interface{}, error) {
		if len(params) == 0 || len(params) > 1 || params[0] == nil {
			return nil, errors.New("Invalid number of arguments")
		}
		val, ok := params[0].(string)
		if !ok {
			return nil, errors.New("Invalid argument type")
		}
		t, err := date.Parse(val)
		if err != nil {
			return nil, err
		}
		return t.Format(time.RFC3339), nil
	})
	expr.AddFunction("addSecondsToNow", func(params ...interface{}) (interface{}, error) {
		if len(params) == 0 || len(params) > 1 || params[0] == nil {
			return nil, errors.New("Invalid number of arguments")
		}
		// if type of params[0] is not float64 or int, return error
		tt, isFloat := params[0].(float64)
		if !isFloat {
			if _, ok := params[0].(int); !ok {
				return nil, errors.New("Invalid argument type")
			}
		}
		// add expiry to the current time
		// convert parms[0] to int from float64
		if isFloat {
			params[0] = int(tt)
		}
		t := time.Now().UTC()
		t = t.Add(time.Duration(params[0].(int)) * time.Second)
		return t, nil
	})
	expr.AddFunction("values", func(params ...interface{}) (interface{}, error) {
		if len(params) == 0 || len(params) > 2 {
			return nil, errors.New("Invalid number of arguments")
		}
		// get values from map
		mapList, ok := params[0].([]any)
		if !ok {
			return nil, errors.New("Invalid argument type")
		}
		keyToGet, hasKey := params[1].(string)
		var values []any
		if hasKey {
			for _, m := range mapList {
				mp := m.(map[string]any)
				if val, ok := mp[keyToGet]; ok {
					values = append(values, val)
				}
			}
		} else {
			for _, m := range mapList {
				mp := m.(map[string]any)
				vals := maps.Values(mp)
				values = append(values, vals...)
			}
		}
		return values, nil
	})
	expr.AddFunction("uniqueid", func(params ...interface{}) (interface{}, error) {
		// create a new xid
		return xid.New().String(), nil
	})
	expr.AddFunction("now", func(params ...interface{}) (interface{}, error) {
		// get the current time in UTC
		return time.Now().UTC(), nil
	})
	expr.AddFunction("toString", func(params ...interface{}) (interface{}, error) {
		if len(params) == 0 || len(params) > 1 || params[0] == nil {
			return nil, errors.New("Invalid number of arguments")
		}
		// convert to string
		return fmt.Sprint(params[0]), nil
	})
}
