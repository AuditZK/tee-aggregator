package errtrack

import (
	"sync/atomic"

	"go.uber.org/zap/zapcore"
)

// Core is a zapcore.Core wrapper that captures every entry at
// CaptureLevel or above into the supplied Store, after running it
// through the sanitization pipeline. Entries below CaptureLevel are
// passed through to the inner Core unchanged.
//
// Wiring order: place errtrack.Core OUTSIDE logredact.Core in the chain
// so the entry it observes has already been redacted at the field
// level. errtrack still re-sanitizes everything as defense in depth,
// but ordering keeps the redaction guarantees of the existing
// `redactedLogger` intact.
//
// Failure-safety: if anything panics during capture (a malformed stack,
// a custom MarshalJSON that recurses, …) the Core swallows the panic
// and falls back to the inner Core's normal Write. The application
// must NEVER crash because of an instrumentation failure.
type Core struct {
	zapcore.Core
	store        *Store
	captureLevel zapcore.Level

	// inFlight is per-Core (not per-call) and is used purely to break
	// recursion: if the act of capturing an entry itself triggers
	// another log emission that would re-enter Write on the SAME core
	// instance, the second call will skip capture and pass through.
	// We use atomic.Bool so With() can clone the Core while still
	// sharing the recursion guard.
	inFlight *atomic.Bool
}

// NewCore wraps an inner Core with capture-and-store semantics for
// level >= captureLevel. Pass zapcore.ErrorLevel to capture errors and
// fatals only (the recommended setting); pass zapcore.WarnLevel if you
// also want warnings tracked. Levels below WarnLevel will produce a lot
// of noise and are accepted only to keep the API symmetric with zap.
func NewCore(inner zapcore.Core, store *Store, captureLevel zapcore.Level) *Core {
	if captureLevel < zapcore.WarnLevel {
		captureLevel = zapcore.ErrorLevel
	}
	return &Core{
		Core:         inner,
		store:        store,
		captureLevel: captureLevel,
		inFlight:     &atomic.Bool{},
	}
}

// With clones the wrapping Core, preserving the same store and
// recursion guard, while delegating field accumulation to the inner
// core (which may itself be a redact core, a broadcast core, …).
func (c *Core) With(fields []zapcore.Field) zapcore.Core {
	return &Core{
		Core:         c.Core.With(fields),
		store:        c.store,
		captureLevel: c.captureLevel,
		inFlight:     c.inFlight,
	}
}

// Check is delegated unchanged. We must not gate on captureLevel here
// because that would suppress non-captured (but enabled) entries.
func (c *Core) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Core.Enabled(entry.Level) {
		return ce.AddCore(entry, c)
	}
	return ce
}

// Write delegates to the inner Core unconditionally, then conditionally
// captures the entry into the store. Capture is best-effort: failures
// (panics, bad input) are silently swallowed so the inner write path
// is never disturbed.
func (c *Core) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	innerErr := c.Core.Write(entry, fields)
	if entry.Level < c.captureLevel {
		return innerErr
	}
	c.capture(entry, fields)
	return innerErr
}

// capture is the safety-wrapped part of Write. It MUST never panic
// (defer/recover catches anything we missed), and it MUST never write
// back to a logger (no zap.* calls here — that would risk recursion
// even with the inFlight guard, because a downstream Core may use a
// different logger instance).
func (c *Core) capture(entry zapcore.Entry, fields []zapcore.Field) {
	if !c.inFlight.CompareAndSwap(false, true) {
		// Already capturing on this Core instance — drop to break
		// recursion. We do not panic, do not log; we silently skip.
		return
	}
	defer c.inFlight.Store(false)
	defer func() {
		// Last-line defense: anything that panics inside capture is
		// caught here and discarded. The inner Write already
		// succeeded above so the caller's log line is preserved.
		_ = recover()
	}()

	msg := SanitizeMessage(entry.Message)
	flds := SanitizeFields(fields)

	frames := SanitizeStack(entry.Stack)
	caller := ""
	if entry.Caller.Defined {
		caller = trimFilePath(entry.Caller.File) + ":" + itoa(entry.Caller.Line)
	}

	ev := SanitizedEvent{
		Level:   entry.Level.String(),
		Message: msg,
		Caller:  caller,
		Frames:  frames,
		Fields:  flds,
	}
	if rid, ok := flds["request_id"]; ok {
		ev.RequestID = rid
	}

	var id string
	if len(frames) > 0 {
		id = Fingerprint(entry.Level, msg, frames)
	} else {
		id = FingerprintFromMessage(entry.Level, msg)
	}
	c.store.Record(id, entry.Level, ev)
}

// Sync delegates to the inner core. The store has no flush concept of
// its own (it's pure in-memory), so Sync is just inner.Sync.
func (c *Core) Sync() error {
	return c.Core.Sync()
}

// itoa is a tiny dependency-free integer formatter so capture.go does
// not pull strconv into its hot path. Always positive in practice (line
// numbers).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
