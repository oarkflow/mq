// phuslulog.go
package logger

import "github.com/oarkflow/log"

// DefaultLogger implements the Logger interface using phuslu/log.
type DefaultLogger struct {
	logger *log.Logger
}

func NewDefaultLogger(loggers ...*log.Logger) *DefaultLogger {
	var logger *log.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	} else {
		logger = &log.DefaultLogger
	}
	return &DefaultLogger{logger: logger}
}

// Debug logs a debug-level message.
func (l *DefaultLogger) Debug(msg string, fields ...Field) {
	if l.logger == nil {
		return
	}
	l.logger.Debug().Map(flattenFields(fields)).Msg(msg)
}

// Info logs an info-level message.
func (l *DefaultLogger) Info(msg string, fields ...Field) {
	if l.logger == nil {
		return
	}
	l.logger.Info().Map(flattenFields(fields)).Msg(msg)
}

// Warn logs a warn-level message.
func (l *DefaultLogger) Warn(msg string, fields ...Field) {
	if l.logger == nil {
		return
	}
	l.logger.Warn().Map(flattenFields(fields)).Msg(msg)
}

// Error logs an error-level message.
func (l *DefaultLogger) Error(msg string, fields ...Field) {
	if l.logger == nil {
		return
	}
	l.logger.Error().Map(flattenFields(fields)).Msg(msg)
}

// flattenFields converts a slice of Field into a slice of any key/value pairs.
func flattenFields(fields []Field) map[string]any {
	kv := make(map[string]any)
	for _, field := range fields {
		kv[field.Key] = field.Value
	}
	return kv
}
