package logger

import "context"

type NullLogger struct{}

func (l *NullLogger) Debug(msg string, fields ...Field) {}

func (l *NullLogger) Info(msg string, fields ...Field) {}

func (l *NullLogger) Warn(msg string, fields ...Field) {}

func (l *NullLogger) Error(msg string, fields ...Field) {}

func NewNullLogger() *NullLogger {
	return &NullLogger{}
}
func (l *NullLogger) WithFields(fields ...Field) Logger {
	return l
}

func (l *NullLogger) WithContext(ctx context.Context) Logger {
	return l
}
