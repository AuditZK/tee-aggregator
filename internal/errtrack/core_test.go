package errtrack

import (
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// buildLogger returns a zap.Logger whose pipeline is:
//
//	user → errtrack.Core → observer (in-memory zap)
//
// AddStacktrace(ErrorLevel) populates entry.Stack so we can exercise
// the frame-capture path.
func buildLogger(t *testing.T, store *Store) (*zap.Logger, *observer.ObservedLogs) {
	t.Helper()
	obsCore, logs := observer.New(zapcore.DebugLevel)
	core := NewCore(obsCore, store, zapcore.ErrorLevel)
	return zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel)), logs
}

func TestCore_CapturesErrorEntry(t *testing.T) {
	store := NewStore(10, 100)
	logger, logs := buildLogger(t, store)

	logger.Error("boom: api_key=AKIAIOSFODNN7EXAMPLE",
		zap.String("user_id", "u-1"),
		zap.String("path", "/api/x"),
	)

	// Inner core still saw the entry.
	if logs.Len() != 1 {
		t.Fatalf("inner observer didn't see the entry: %d", logs.Len())
	}
	stats := store.Stats()
	if stats.TotalEvents != 1 {
		t.Fatalf("store didn't capture: %d events", stats.TotalEvents)
	}
	if stats.ActiveGroups != 1 {
		t.Fatalf("expected 1 group, got %d", stats.ActiveGroups)
	}

	// And the captured sample must not contain the secret.
	groups := store.ListGroups(0)
	if strings.Contains(groups[0].Sample.Message, "AKIAIOSFODNN7EXAMPLE") {
		t.Fatalf("secret leaked into stored sample: %q", groups[0].Sample.Message)
	}
	if v := groups[0].Sample.Fields["user_id"]; strings.Contains(v, "u-1") {
		t.Fatalf("user_id leaked: %q", v)
	}
}

func TestCore_IgnoresLowLevelEntries(t *testing.T) {
	store := NewStore(10, 100)
	logger, _ := buildLogger(t, store)

	logger.Info("just info")
	logger.Debug("just debug")
	logger.Warn("warning")

	if store.Stats().TotalEvents != 0 {
		t.Fatalf("captured below threshold: %d", store.Stats().TotalEvents)
	}
}

func TestCore_CapturesWarnIfConfigured(t *testing.T) {
	store := NewStore(10, 100)
	obsCore, _ := observer.New(zapcore.DebugLevel)
	core := NewCore(obsCore, store, zapcore.WarnLevel)
	logger := zap.New(core, zap.AddCaller())

	logger.Warn("flap")
	logger.Info("ignored")

	if store.Stats().TotalEvents != 1 {
		t.Fatalf("expected 1 (warn), got %d", store.Stats().TotalEvents)
	}
}

func TestCore_GroupsIdenticalErrors(t *testing.T) {
	store := NewStore(10, 100)
	logger, _ := buildLogger(t, store)

	for i := 0; i < 50; i++ {
		emitSampleError(logger)
	}
	if got := store.Stats().ActiveGroups; got != 1 {
		t.Fatalf("expected 1 group for identical site, got %d", got)
	}
}

// emitSampleError is a fixed call site so all 50 calls produce the
// same fingerprint. Defined as its own function so the stack frame is
// stable.
func emitSampleError(logger *zap.Logger) {
	logger.Error("repeated failure", zap.String("path", "/x"))
}

func TestCore_RecursionGuardSkipsReentrantCapture(t *testing.T) {
	// Direct test of the inFlight guard: when capture is already
	// running on the same Core instance and Write is invoked again
	// (synthetic re-entry), the second capture must be a no-op.
	store := NewStore(10, 100)
	obsCore, _ := observer.New(zapcore.DebugLevel)
	core := NewCore(obsCore, store, zapcore.ErrorLevel)

	// Pretend a capture is in flight by flipping the guard manually.
	if !core.inFlight.CompareAndSwap(false, true) {
		t.Fatal("inFlight should start false")
	}
	defer core.inFlight.Store(false)

	// Now invoke Write directly — capture() must skip and the store
	// must not grow.
	entry := zapcore.Entry{
		Level:   zapcore.ErrorLevel,
		Message: "would normally be captured",
	}
	if err := core.Write(entry, nil); err != nil {
		t.Fatalf("Write returned err: %v", err)
	}
	if got := store.Stats().TotalEvents; got != 0 {
		t.Fatalf("recursion guard didn't skip capture: %d events stored", got)
	}
}

func TestCore_PanicInCaptureIsSwallowed(t *testing.T) {
	// Build a logger and feed it an entry whose stack contains a
	// pathological format that the parser must not panic on. This
	// indirectly tests the deferred recover() in capture().
	store := NewStore(10, 100)
	logger, _ := buildLogger(t, store)

	// A long, deliberately-malformed message and many fields.
	fields := make([]zapcore.Field, 0, 20)
	for i := 0; i < 20; i++ {
		fields = append(fields, zap.String("k", strings.Repeat("a", 100)))
	}
	// Must not panic.
	logger.Error(strings.Repeat("\n", 50)+"weird", fields...)
}
