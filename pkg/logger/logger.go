package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger is a wrapper around zap.Logger
type Logger struct {
	zap *zap.Logger
}

// Config holds logger configuration
type Config struct {
	Level       string
	Environment string // "development" or "production"
	ServiceName string
}

// New creates a new Logger instance
func New(cfg Config) (*Logger, error) {
	var zapConfig zap.Config
	if cfg.Environment == "production" {
		zapConfig = zap.NewProductionConfig()
	} else {
		zapConfig = zap.NewDevelopmentConfig()
	}

	// Set log level
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		level = zapcore.InfoLevel
	}
	zapConfig.Level = zap.NewAtomicLevelAt(level)

	// Configure encoder
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapConfig.EncoderConfig.TimeKey = "timestamp"

	logger, err := zapConfig.Build()
	if err != nil {
		return nil, err
	}

	// Add service name and initial fields
	logger = logger.With(zap.String("service", cfg.ServiceName))

	return &Logger{zap: logger}, nil
}

// Info logs a message at InfoLevel
func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.zap.Info(msg, fields...)
}

// Error logs a message at ErrorLevel with stack trace
func (l *Logger) Error(msg string, err error, fields ...zap.Field) {
	allFields := append(fields, zap.Error(err))
	l.zap.Error(msg, allFields...)
}

// Debug logs a message at DebugLevel
func (l *Logger) Debug(msg string, fields ...zap.Field) {
	l.zap.Debug(msg, fields...)
}

// Warn logs a message at WarnLevel
func (l *Logger) Warn(msg string, fields ...zap.Field) {
	l.zap.Warn(msg, fields...)
}

// With creates a child logger and adds structured context to it
func (l *Logger) With(fields ...zap.Field) *Logger {
	return &Logger{zap: l.zap.With(fields...)}
}

// Sync flushes any buffered log entries
func (l *Logger) Sync() error {
	return l.zap.Sync()
}

// ParseLevel parses the log level string
func ParseLevel(level string) zapcore.Level {
	l, err := zapcore.ParseLevel(level)
	if err != nil {
		return zapcore.InfoLevel
	}
	return l
}
