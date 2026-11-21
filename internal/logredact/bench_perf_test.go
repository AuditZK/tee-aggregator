package logredact

import (
	"errors"
	"io"
	"testing"

	"go.uber.org/zap/zapcore"
)

// PERF-AUDIT: probe the cost of the redaction wrapper. Every log line
// allocates a fresh fields slice (redactFields) and lower-cases the
// message + every field name. With cmd/enclave/main.go wrapping the
// redact core twice (once direct, once inside BroadcastCore), the
// per-line overhead is paid twice — these benchmarks size that cost.

type discardCore struct{}

func (discardCore) Enabled(zapcore.Level) bool                            { return true }
func (d discardCore) With([]zapcore.Field) zapcore.Core                   { return d }
func (d discardCore) Check(_ zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(zapcore.Entry{}, d)
}
func (discardCore) Write(zapcore.Entry, []zapcore.Field) error { return nil }
func (discardCore) Sync() error                                { return nil }

func BenchmarkRedactCore_Write_8Fields(b *testing.B) {
	core := NewRedactCore(discardCore{})
	entry := zapcore.Entry{
		Level:      zapcore.InfoLevel,
		LoggerName: "bench",
		Message:    "sync completed for user",
	}
	fields := []zapcore.Field{
		{Key: "method", Type: zapcore.StringType, String: "GET"},
		{Key: "path", Type: zapcore.StringType, String: "/api/v1/sync"},
		{Key: "duration", Type: zapcore.StringType, String: "12ms"},
		{Key: "user_uid", Type: zapcore.StringType, String: "u_abcdef"},
		{Key: "exchange", Type: zapcore.StringType, String: "binance"},
		{Key: "trade_count", Type: zapcore.Int64Type, Integer: 17},
		{Key: "equity", Type: zapcore.Float64Type, Integer: 0},
		{Key: "error", Type: zapcore.ErrorType, Interface: errors.New("upstream timeout")},
	}

	_ = io.Discard
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = core.Write(entry, fields)
	}
}

func BenchmarkScrubMessage_NoSecret(b *testing.B) {
	msg := "sync completed for user u_abc on binance: 17 trades persisted in 12ms"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scrubMessage(msg)
	}
}

func BenchmarkScrubMessage_FastPath(b *testing.B) {
	// Simple INFO log with no '=', ':' or '/' — the common shape coming
	// out of zap structured fields where the message is a fixed string
	// and all parameters are emitted as separate fields.
	msg := "sync completed"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scrubMessage(msg)
	}
}

func BenchmarkShouldRedact_SafeField(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = shouldRedact("duration")
	}
}
