package errtrack

import (
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestFingerprint_DeterministicAcrossCalls(t *testing.T) {
	frames := []Frame{
		{Function: "pkg.Foo", File: "internal/foo.go:1"},
		{Function: "pkg.Bar", File: "internal/bar.go:2"},
	}
	a := Fingerprint(zapcore.ErrorLevel, "msg", frames)
	b := Fingerprint(zapcore.ErrorLevel, "msg", frames)
	if a != b {
		t.Fatalf("non-deterministic: %s vs %s", a, b)
	}
	if len(a) != fingerprintIDLen {
		t.Fatalf("unexpected length %d", len(a))
	}
}

func TestFingerprint_StableAcrossLineNumbers(t *testing.T) {
	// Same functions, different line numbers in File — fingerprint
	// must be the same (line moves due to refactors should not split
	// groups).
	a := Fingerprint(zapcore.ErrorLevel, "msg", []Frame{
		{Function: "pkg.Foo", File: "internal/foo.go:10"},
		{Function: "pkg.Bar", File: "internal/bar.go:20"},
	})
	b := Fingerprint(zapcore.ErrorLevel, "msg", []Frame{
		{Function: "pkg.Foo", File: "internal/foo.go:99"},
		{Function: "pkg.Bar", File: "internal/bar.go:200"},
	})
	if a != b {
		t.Fatalf("fingerprint shifted on line change: %s vs %s", a, b)
	}
}

func TestFingerprint_StableAcrossBuildPaths(t *testing.T) {
	// Frames with different *file* paths but same *function* must
	// hash identically. (File is excluded from the hash on purpose.)
	a := Fingerprint(zapcore.ErrorLevel, "msg", []Frame{
		{Function: "pkg.Foo", File: "/build/host-a/foo.go:1"},
	})
	b := Fingerprint(zapcore.ErrorLevel, "msg", []Frame{
		{Function: "pkg.Foo", File: "/build/host-b/foo.go:1"},
	})
	if a != b {
		t.Fatalf("fingerprint changed across build host: %s vs %s", a, b)
	}
}

func TestFingerprint_DifferentLevelsDiffer(t *testing.T) {
	frames := []Frame{{Function: "pkg.Foo"}}
	a := Fingerprint(zapcore.ErrorLevel, "msg", frames)
	b := Fingerprint(zapcore.WarnLevel, "msg", frames)
	if a == b {
		t.Fatal("expected level to be part of fingerprint")
	}
}

func TestFingerprint_IgnoresClosureSuffix(t *testing.T) {
	a := Fingerprint(zapcore.ErrorLevel, "msg", []Frame{{Function: "pkg.Foo"}})
	b := Fingerprint(zapcore.ErrorLevel, "msg", []Frame{{Function: "pkg.Foo.func1"}})
	c := Fingerprint(zapcore.ErrorLevel, "msg", []Frame{{Function: "pkg.Foo.func1.2"}})
	if a != b || a != c {
		t.Fatalf("closures fragmented: %s %s %s", a, b, c)
	}
}

func TestFingerprint_TopFramesOnly(t *testing.T) {
	// Build two stacks where frames 0..4 are identical and frame 5+
	// differs. They must hash to the same fingerprint.
	common := []Frame{
		{Function: "pkg.A"}, {Function: "pkg.B"}, {Function: "pkg.C"},
		{Function: "pkg.D"}, {Function: "pkg.E"},
	}
	a := append([]Frame(nil), common...)
	a = append(a, Frame{Function: "different.X"})
	b := append([]Frame(nil), common...)
	b = append(b, Frame{Function: "different.Y"})
	if Fingerprint(zapcore.ErrorLevel, "msg", a) != Fingerprint(zapcore.ErrorLevel, "msg", b) {
		t.Fatal("frames beyond top-N should not affect fingerprint")
	}
}

func TestFingerprintFromMessage_StripsVariableRuns(t *testing.T) {
	a := FingerprintFromMessage(zapcore.ErrorLevel, "sync 17 failed for connection abc123")
	b := FingerprintFromMessage(zapcore.ErrorLevel, "sync 19 failed for connection xyz789")
	if a != b {
		t.Fatalf("messages should hash identically after digit strip: %s vs %s", a, b)
	}
}

func TestFingerprintFromMessage_DifferentTextDiffers(t *testing.T) {
	a := FingerprintFromMessage(zapcore.ErrorLevel, "auth failed")
	b := FingerprintFromMessage(zapcore.ErrorLevel, "auth succeeded")
	if a == b {
		t.Fatal("structurally different messages must not collapse")
	}
}

func TestFingerprint_DifferentMessagesSameSiteDiffer(t *testing.T) {
	// Two distinct errors emitted from the same call site (same
	// frames) must produce different fingerprints. Without message-
	// in-fingerprint, both would collapse — which masks separate
	// bugs in the same function.
	frames := []Frame{{Function: "main.main"}, {Function: "runtime.main"}}
	a := Fingerprint(zapcore.ErrorLevel, "auth failed", frames)
	b := Fingerprint(zapcore.ErrorLevel, "upstream timeout", frames)
	if a == b {
		t.Fatalf("distinct messages should produce distinct fingerprints, both %s", a)
	}
}

func TestFingerprint_VariableMessageStillCollapses(t *testing.T) {
	// On the other hand, messages that only differ by digits/ids
	// (typical of "sync 17 / sync 19") must still collapse.
	frames := []Frame{{Function: "main.main"}}
	a := Fingerprint(zapcore.ErrorLevel, "sync 17 failed for connection abc123", frames)
	b := Fingerprint(zapcore.ErrorLevel, "sync 19 failed for connection xyz789", frames)
	if a != b {
		t.Fatalf("variable-only differences should collapse: %s vs %s", a, b)
	}
}

func TestFingerprint_HexOnly(t *testing.T) {
	id := Fingerprint(zapcore.ErrorLevel, "msg", []Frame{{Function: "x"}})
	for _, c := range id {
		ok := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
		if !ok {
			t.Fatalf("non-hex char %q in fingerprint %s", c, id)
		}
	}
}
