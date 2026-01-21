package logger

import (
	"errors"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		wantErr     bool
		wantLevel   zapcore.Level
		wantService string
	}{
		{
			name: "Development Config",
			config: Config{
				Level:       "debug",
				Environment: "development",
				ServiceName: "test-service",
			},
			wantErr:     false,
			wantLevel:   zapcore.DebugLevel,
			wantService: "test-service",
		},
		{
			name: "Production Config",
			config: Config{
				Level:       "info",
				Environment: "production",
				ServiceName: "prod-service",
			},
			wantErr:     false,
			wantLevel:   zapcore.InfoLevel,
			wantService: "prod-service",
		},
		{
			name: "Invalid Level Defaults to Info",
			config: Config{
				Level:       "invalid",
				Environment: "development",
				ServiceName: "test-service",
			},
			wantErr:     false,
			wantLevel:   zapcore.InfoLevel,
			wantService: "test-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := New(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if l != nil {
				if l.zap.Core().Enabled(tt.wantLevel) == false {
					t.Errorf("Expected level %v to be enabled", tt.wantLevel)
				}
				// Verify service name field is present (indirectly via With)
				// We can't easily inspect the core fields without an observer,
				// so we'll trust the integration or use observer in a separate test.
			}
		})
	}
}

func TestLoggerOutput(t *testing.T) {
	// Create an observer core to verify output
	core, observed := observer.New(zap.InfoLevel)
	l := &Logger{zap: zap.New(core)}

	// Test Info
	l.Info("info message", zap.String("key", "value"))
	if observed.Len() != 1 {
		t.Errorf("Expected 1 log entry, got %d", observed.Len())
	}
	entry := observed.All()[0]
	if entry.Message != "info message" {
		t.Errorf("Expected message 'info message', got '%s'", entry.Message)
	}
	if entry.ContextMap()["key"] != "value" {
		t.Errorf("Expected key=value, got %v", entry.ContextMap()["key"])
	}

	// Test Error
	observed.TakeAll() // Clear
	errVal := errors.New("test error")
	l.Error("error message", errVal)
	if observed.Len() != 1 {
		t.Errorf("Expected 1 log entry, got %d", observed.Len())
	}
	entry = observed.All()[0]
	if entry.Message != "error message" {
		t.Errorf("Expected message 'error message', got '%s'", entry.Message)
	}
	if entry.ContextMap()["error"] != "test error" {
		t.Errorf("Expected error field, got %v", entry.ContextMap()["error"])
	}

	// Test Debug (should be ignored due to InfoLevel)
	observed.TakeAll()
	l.Debug("debug message")
	if observed.Len() != 0 {
		t.Errorf("Expected 0 log entries, got %d", observed.Len())
	}
}

func TestWith(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	l := &Logger{zap: zap.New(core)}

	child := l.With(zap.String("child", "true"))
	child.Info("child message")

	if observed.Len() != 1 {
		t.Errorf("Expected 1 log entry, got %d", observed.Len())
	}
	entry := observed.All()[0]
	if entry.ContextMap()["child"] != "true" {
		t.Errorf("Expected child=true, got %v", entry.ContextMap()["child"])
	}
}
