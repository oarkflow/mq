package utils

import (
	"fmt"
	"net/url"
	"strings"
)

type form struct {
	dest      map[string]any
	raw       string
	pathCache map[string]int
}

func newForm(raw string) *form {
	f := new(form)
	f.raw = raw
	f.pathCache = make(map[string]int)
	return f
}

func (f *form) reset() {
	f.dest = make(map[string]any)
}

func (f *form) decode() (map[string]any, error) {
	f.reset()
	vals := make(map[string]any)
	for _, v := range strings.Split(f.raw, "&") {
		if v == "" {
			continue
		}
		index := strings.Index(v, "=")
		if index > 0 {
			key := v[:index]
			val := v[index+1:]

			f.insertValue(&vals, key, val)
		} else {
			f.insertValue(&vals, v, "")
		}
	}
	return f.parseArray(vals), nil
}

func (f *form) insertValue(destP *map[string]any, key string, val string) {
	key, _ = url.PathUnescape(key)
	var path []string
	if strings.Contains(key, "[") || strings.Contains(key, "]") {
		var current string
		for _, c := range key {
			switch c {
			case '[':
				if len(current) > 0 {
					path = append(path, current)
					current = ""
				}
			case ']':
				path = append(path, current)
				current = ""
				continue
			default:
				current += string(c)
			}
		}
		if len(current) > 0 {
			path = append(path, current)
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
			k = fmt.Sprint(f.pathCache[c])
			f.pathCache[c] = f.pathCache[c] + 1
		}
		if _, ok := dest[k].(map[string]any); !ok {
			dest[k] = make(map[string]any)
		}
		dest = dest[k].(map[string]any)
	}
	p := path[len(path)-1]
	if p == "" {
		p = fmt.Sprint(len(dest))
	}
	val, _ = url.QueryUnescape(val)
	dest[p] = val
}

func (f *form) parseArrayItem(dest map[string]any) any {
	var arr []any
	for i := 0; i < len(dest); i++ {
		item, ok := dest[fmt.Sprint(i)]
		if !ok {
			return dest
		}
		arr = append(arr, item)
	}
	return arr
}

func (f *form) parseArray(dest map[string]any) map[string]any {
	for k, v := range dest {
		mv, ok := v.(map[string]any)
		if ok {
			f.parseArray(mv)
			dest[k] = f.parseArrayItem(mv)
		}
	}
	return dest
}

func DecodeForm(src []byte) (map[string]any, error) {
	return newForm(FromByte(src)).decode()
}
