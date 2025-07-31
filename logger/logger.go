package logger

// Field represents a key-value pair used for structured logging.
type Field struct {
	Key   string
	Value any
}

// Logger is an interface that provides logging at various levels.
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}
