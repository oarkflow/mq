package utils

import (
	"github.com/oarkflow/json"
	"github.com/oarkflow/json/jsonparser"
)

func RemoveFromJSONBye(jsonStr json.RawMessage, key ...string) json.RawMessage {
	return jsonparser.Delete(jsonStr, key...)
}

func RemoveRecursiveFromJSON(jsonStr json.RawMessage, key ...string) json.RawMessage {
	var data any
	if err := json.Unmarshal(jsonStr, &data); err != nil {
		return jsonStr
	}

	for _, k := range key {
		data = removeKeyRecursive(data, k)
	}

	result, err := json.Marshal(data)
	if err != nil {
		return jsonStr
	}
	return result
}

func removeKeyRecursive(data any, key string) any {
	switch v := data.(type) {
	case map[string]any:
		delete(v, key)
		for k, val := range v {
			v[k] = removeKeyRecursive(val, key)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = removeKeyRecursive(item, key)
		}
		return v
	default:
		return v
	}
}
