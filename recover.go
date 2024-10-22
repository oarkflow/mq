package mq

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"

	"github.com/oarkflow/mq/metrics"
)

func RecoverPanic(labelGenerator func() string) {
	if r := recover(); r != nil {
		pc, file, line, ok := runtime.Caller(2)
		funcName := "unknown"
		if ok {
			fn := runtime.FuncForPC(pc)
			if fn != nil {
				funcName = fn.Name()
			}
		}
		log.Printf("[PANIC] - recovered from panic in %s (%s:%d): %v\nStack trace: %s", funcName, file, line, r, debug.Stack())
		label := labelGenerator()
		metrics.TasksErrors.WithLabelValues(label).Inc()
	}
}

func RecoverTitle() string {
	pc, _, line, ok := runtime.Caller(1)
	if !ok {
		return "Unknown"
	}
	fn := runtime.FuncForPC(pc)
	funcName := "unknown"
	if fn != nil {
		funcName = fn.Name()
	}
	return fmt.Sprintf("%s:%d", funcName, line)
}
