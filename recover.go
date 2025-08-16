package mq

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
)

func RecoverPanic(labelGenerator func() string) {
	if r := recover(); r != nil {
		defer func() {
			if rr := recover(); rr != nil {
				// If logging or labelGenerator panics, just print a minimal message
				fmt.Printf("[PANIC] - error during panic recovery: %v\n", rr)
			}
		}()
		pc, file, line, ok := runtime.Caller(2)
		funcName := "unknown"
		if ok {
			fn := runtime.FuncForPC(pc)
			if fn != nil {
				funcName = fn.Name()
			}
		}
		log.Printf("[PANIC] - recovered from panic in %s (%s:%d): %v\nStack trace: %s", funcName, file, line, r, debug.Stack())
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
