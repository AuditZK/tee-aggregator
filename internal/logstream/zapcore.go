package logstream

import (
	"time"

	"github.com/trackrecord/enclave/internal/logredact"
	"go.uber.org/zap/zapcore"
)

// BroadcastCore is a zapcore.Core that tees log entries to both the
// inner core and a LogStreamServer for SSE broadcasting.
type BroadcastCore struct {
	inner  zapcore.Core
	server *Server
}

// NewBroadcastCore wraps a zapcore.Core and broadcasts to the log stream server.
func NewBroadcastCore(inner zapcore.Core, server *Server) zapcore.Core {
	return &BroadcastCore{
		inner:  inner,
		server: server,
	}
}

func (c *BroadcastCore) Enabled(level zapcore.Level) bool {
	return c.inner.Enabled(level)
}

func (c *BroadcastCore) With(fields []zapcore.Field) zapcore.Core {
	return &BroadcastCore{
		inner:  c.inner.With(fields),
		server: c.server,
	}
}

func (c *BroadcastCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.inner.Enabled(entry.Level) {
		checked = checked.AddCore(entry, c)
	}
	return checked
}

func (c *BroadcastCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	// Write to inner core first.
	if err := c.inner.Write(entry, fields); err != nil {
		return err
	}

	// LOG-AUDIT-001: scrub the entry and fields before broadcasting to SSE
	// clients. Even when the caller wired BroadcastCore on top of a redacted
	// inner core, BroadcastCore observes the ORIGINAL entry/fields here
	// (inner.Write only mutates its own local copy). Without this scrub the
	// broadcast and the in-memory log buffer would leak credentials in
	// clear, even though stderr stays redacted.
	scrubbedMsg := logredact.ScrubMessage(entry.Message)
	scrubbedFields := logredact.RedactFields(fields)

	logEntry := LogEntry{
		Timestamp: entry.Time.Format(time.RFC3339),
		Level:     entry.Level.String(),
		Message:   scrubbedMsg,
	}

	if len(scrubbedFields) > 0 {
		logEntry.Fields = make(map[string]interface{})
		enc := zapcore.NewMapObjectEncoder()
		for _, f := range scrubbedFields {
			f.AddTo(enc)
		}
		for k, v := range enc.Fields {
			logEntry.Fields[k] = v
		}
	}

	c.server.Broadcast(logEntry)

	return nil
}

func (c *BroadcastCore) Sync() error {
	return c.inner.Sync()
}
