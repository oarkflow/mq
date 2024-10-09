package utils

import (
	"math/rand"
	"reflect"
	"time"
)

func CalculateJitter(baseDelay time.Duration, percent float64) time.Duration {
	jitter := time.Duration(rand.Float64()*percent*float64(baseDelay)) - time.Duration(percent*float64(baseDelay)/2)
	return baseDelay + jitter
}

func SizeOf(v any) uintptr {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	switch val.Kind() {
	case reflect.Slice:
		return uintptr(val.Len()) * val.Type().Elem().Size()
	case reflect.Map:
		return uintptr(val.Len()) * (val.Type().Key().Size() + val.Type().Elem().Size())
	default:
		return val.Type().Size()
	}
}
