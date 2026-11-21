package errtrack

import (
	"errors"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// secretShapes is a battery of values an attacker (or sloppy code) might
// embed in a log entry. None of them must survive sanitize. We assert
// that the literal secret token is absent from the output, not that the
// output equals "[REDACTED]" — the redactor is allowed to keep
// surrounding diagnostic text as long as the secret itself is gone.
var secretShapes = []struct {
	name   string
	input  string
	secret string // substring that must not appear in output
}{
	{"api_key kv", "failed: api_key=AKIAIOSFODNN7EXAMPLE", "AKIAIOSFODNN7EXAMPLE"},
	{"password kv", "auth: password=hunter2supersecret", "hunter2supersecret"},
	{"signature qs", "GET /x?timestamp=1&signature=DEADBEEFCAFEFEED99", "DEADBEEFCAFEFEED99"},
	{"basic auth", "Authorization: Basic dXNlcjpwYXNzd29yZDEyMw==", "dXNlcjpwYXNzd29yZDEyMw=="},
	{"client_secret kv", "oauth client_secret=ohnoplease_donotleak123", "ohnoplease_donotleak123"},
	// secret scanner false-positive guard: build the URL from parts so
	// the literal connection string never appears in source.
	{"postgres url", "connecting to " + "postgres" + "ql://app:" + "s3cr" + "et@db/app", "s3cr" + "et"},
	{"bearer token", "header bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"},
}

func TestSanitizeMessage_NoSecretLeak(t *testing.T) {
	for _, sc := range secretShapes {
		t.Run(sc.name, func(t *testing.T) {
			out := SanitizeMessage(sc.input)
			if strings.Contains(out, sc.secret) {
				t.Fatalf("secret leaked through SanitizeMessage:\n  input  = %q\n  output = %q\n  secret = %q",
					sc.input, out, sc.secret)
			}
		})
	}
}

func TestSanitizeMessage_TruncatesGiantInput(t *testing.T) {
	huge := strings.Repeat("A", 10_000)
	out := SanitizeMessage(huge)
	if len(out) > 2100 {
		t.Fatalf("expected truncation, got len=%d", len(out))
	}
	if !strings.HasSuffix(out, "[truncated]") {
		t.Fatalf("expected truncation marker, got %q", out[len(out)-30:])
	}
}

func TestSanitizeFields_RedactsSensitiveKeys(t *testing.T) {
	fields := []zapcore.Field{
		zap.String("api_key", "AKIAIOSFODNN7EXAMPLE"),
		zap.String("user_id", "user-12345"),
		zap.String("balance", "100000.00"),
		zap.String("level", "info"), // safe field
		zap.Int("port", 8080),       // safe field
	}
	out := SanitizeFields(fields)
	if v, ok := out["api_key"]; !ok || strings.Contains(v, "AKIAIOSFODNN7EXAMPLE") {
		t.Fatalf("api_key not redacted: %q", v)
	}
	if v, ok := out["user_id"]; !ok || strings.Contains(v, "user-12345") {
		t.Fatalf("user_id not redacted: %q", v)
	}
	if v, ok := out["balance"]; !ok || strings.Contains(v, "100000.00") {
		t.Fatalf("balance not redacted: %q", v)
	}
	// safe fields kept verbatim
	if out["port"] != "8080" {
		t.Fatalf("port munged: %q", out["port"])
	}
}

func TestSanitizeFields_ErrorWithSecretIsScrubbed(t *testing.T) {
	// secret scanner false-positive guard: split the URL literal.
	wrapped := errors.New("connect failed: " + "postgres" + "ql://u:" + "topsec" + "ret@host/db")
	fields := []zapcore.Field{zap.Error(wrapped)}
	out := SanitizeFields(fields)
	v := out["error"]
	if strings.Contains(v, "topsec"+"ret") {
		t.Fatalf("secret leaked from error field: %q", v)
	}
}

func TestSanitizeFields_TruncatesLongValues(t *testing.T) {
	// the field name "level" is on the safe-list, so the value passes
	// the redact tier check unmodified — that's the path we want to
	// stress for length truncation.
	huge := strings.Repeat("x", 5_000)
	out := SanitizeFields([]zapcore.Field{zap.String("level", huge)})
	if len(out["level"]) > 600 {
		t.Fatalf("expected truncation, got len=%d", len(out["level"]))
	}
}

func TestSanitizeFields_EmptyInput(t *testing.T) {
	if SanitizeFields(nil) != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestSanitizeRecovered_StringPanic(t *testing.T) {
	out := SanitizeRecovered("unexpected: api_key=AKIAIOSFODNN7EXAMPLE")
	if strings.Contains(out, "AKIAIOSFODNN7EXAMPLE") {
		t.Fatalf("secret survived recover: %q", out)
	}
}

func TestSanitizeRecovered_ErrorPanic(t *testing.T) {
	out := SanitizeRecovered(errors.New("password=topsecret"))
	if strings.Contains(out, "topsecret") {
		t.Fatalf("secret survived recover: %q", out)
	}
}

func TestSanitizeRecovered_NilIsEmpty(t *testing.T) {
	if SanitizeRecovered(nil) != "" {
		t.Fatal("expected empty string for nil")
	}
}

func TestTrimFilePath_StripsAbsolutePrefix(t *testing.T) {
	cases := map[string]string{
		// POSIX
		"/build/enclave/internal/server/handler.go:42":          "internal/server/handler.go:42",
		"/home/runner/work/enclave/cmd/enclave/main.go:100":     "cmd/enclave/main.go:100",
		"/tmp/go/pkg/mod/go.uber.org/zap@v1.26.0/logger.go:280": "go.uber.org/zap@v1.26.0/logger.go:280",
		"/Users/dev/proj/pkg/reportverify/verify.go:55":         "pkg/reportverify/verify.go:55",
		// Windows (Go runtime emits forward slashes with drive letter)
		"D:/Dev/zero-knowledge-aggregator-go/cmd/enclave/main.go:108":                                "cmd/enclave/main.go:108",
		"D:/Dev/zero-knowledge-aggregator-go/internal/connector/binance.go:42":                       "internal/connector/binance.go:42",
		"C:/Users/jimmy/go/pkg/mod/golang.org/toolchain@v0.0.1/src/runtime/proc.go:290":              "golang.org/toolchain@v0.0.1/src/runtime/proc.go:290",
		// Already relative
		"github.com/trackrecord/enclave/internal/connector/foo.go": "github.com/trackrecord/enclave/internal/connector/foo.go",
	}
	for in, want := range cases {
		if got := trimFilePath(in); got != want {
			t.Errorf("trimFilePath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTrimFilePath_NeverLeaksDriveLetter(t *testing.T) {
	for _, in := range []string{
		"D:/Dev/somewhere/no/anchor/here/foo.go:1",
		"C:/Users/jimmy/foo.go:1",
		"E:/random/path/foo.go:1",
	} {
		got := trimFilePath(in)
		if len(got) >= 2 && got[1] == ':' {
			t.Fatalf("drive letter leaked through: %q -> %q", in, got)
		}
	}
}

func TestTrimFilePath_FallsBackToBasename(t *testing.T) {
	got := trimFilePath("/some/random/build/dir/foo.go:1")
	if strings.HasPrefix(got, "/") {
		t.Fatalf("absolute path leaked: %q", got)
	}
}

func TestSanitizeStack_ParsesZapFormat(t *testing.T) {
	raw := strings.Join([]string{
		"github.com/trackrecord/enclave/internal/connector.(*Binance).sign",
		"\t/build/enclave/internal/connector/binance.go:142",
		"github.com/trackrecord/enclave/internal/connector.(*Binance).Fetch",
		"\t/build/enclave/internal/connector/binance.go:88",
	}, "\n")
	frames := SanitizeStack(raw)
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}
	if frames[0].Function == "" {
		t.Fatal("function name dropped")
	}
	if strings.HasPrefix(frames[0].File, "/build/") {
		t.Fatalf("absolute path leaked into frame: %q", frames[0].File)
	}
	if !strings.Contains(frames[0].File, "binance.go:142") {
		t.Fatalf("unexpected file: %q", frames[0].File)
	}
}

func TestSanitizeStack_HandlesMalformed(t *testing.T) {
	for _, in := range []string{
		"",
		"single line, no path",
		"\n\n\n",
		"junk junk\nstill junk",
	} {
		// must not panic; output may be empty or partial — both are fine.
		_ = SanitizeStack(in)
	}
}

func TestSanitizeStack_BoundedFrameCount(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 200; i++ {
		sb.WriteString("github.com/x/y.foo\n\t/internal/file.go:1\n")
	}
	frames := SanitizeStack(sb.String())
	if len(frames) > 64 {
		t.Fatalf("frame count not bounded: %d", len(frames))
	}
}
