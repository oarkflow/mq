package utils

import (
	"net/url"
	"strings"
)

type form struct {
	raw       string
	pathCache map[string]int
}

func newForm(raw string) *form {
	return &form{
		raw:       raw,
		pathCache: make(map[string]int),
	}
}

func (f *form) decode() (map[string]interface{}, error) {
	vals := make(map[string]interface{})
	keyBuffer := make([]byte, 0, len(f.raw))
	valBuffer := make([]byte, 0, len(f.raw))

	for _, v := range strings.Split(f.raw, "&") {
		if v == "" {
			continue
		}

		eqIndex := strings.IndexByte(v, '=')
		if eqIndex > 0 {
			key := v[:eqIndex]
			val := v[eqIndex+1:]
			keyBuffer = append(keyBuffer[:0], key...)
			valBuffer = append(valBuffer[:0], val...)
			f.insertValue(&vals, string(keyBuffer), string(valBuffer))
		} else {
			keyBuffer = append(keyBuffer[:0], v...)
			f.insertValue(&vals, string(keyBuffer), "")
		}
	}
	return f.parseArray(vals), nil
}

func (f *form) insertValue(destP *map[string]interface{}, key, val string) {
	key, _ = url.PathUnescape(key)
	var path []string
	if strings.IndexByte(key, '[') >= 0 || strings.IndexByte(key, ']') >= 0 {
		start := 0
		for i := 0; i < len(key); i++ {
			switch key[i] {
			case '[':
				if i > start {
					path = append(path, key[start:i])
				}
				start = i + 1
			case ']':
				if i > start {
					path = append(path, key[start:i])
				}
				start = i + 1
			}
		}
		if start < len(key) {
			path = append(path, key[start:])
		}
	} else {
		path = append(path, key)
	}
	dest := *destP
	for i, k := range path {
		if i == len(path)-1 {
			break
		}
		if k == "" {
			c := strings.Join(path, ",")
			k = string(rune(f.pathCache[c] + '0'))
			f.pathCache[c]++
		}
		if next, ok := dest[k].(map[string]interface{}); !ok {
			next = make(map[string]interface{})
			dest[k] = next
			dest = next
		} else {
			dest = next
		}
	}
	last := path[len(path)-1]
	if last == "" {
		last = string(rune(len(dest)))
	}
	val, _ = url.QueryUnescape(val)
	dest[last] = val
}

func (f *form) parseArrayItem(dest map[string]interface{}) interface{} {
	var arr []interface{}
	for i := 0; ; i++ {
		key := string(rune(i + '0'))
		item, ok := dest[key]
		if !ok {
			break
		}
		arr = append(arr, item)
	}
	if len(arr) == 0 {
		return dest
	}
	return arr
}

func (f *form) parseArray(dest map[string]interface{}) map[string]interface{} {
	for k, v := range dest {
		if mv, ok := v.(map[string]interface{}); ok {
			dest[k] = f.parseArrayItem(f.parseArray(mv))
		}
	}
	return dest
}

func DecodeForm(src []byte) (map[string]interface{}, error) {
	return newForm(string(src)).decode()
}
