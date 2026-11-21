// Package errtrack implements an in-process, Sentry-like error aggregation
// layer designed for the TEE/zero-knowledge constraints of this enclave:
//
//   - No third-party telemetry. Everything stays inside the attested
//     measurement.
//   - All inbound data is sanitized via internal/logredact before it can
//     reach the in-memory store, so stored samples never contain
//     credentials, business data, or raw user input.
//   - Output paths re-sanitize as a defense-in-depth measure.
//   - The store is bounded (hard cap + LRU + rate-limit on new groups) to
//     prevent a pathological logger loop from exhausting enclave memory.
//
// SECURITY: sanitize.go is the single funnel through which every captured
// log entry must pass before being added to the store. There is no other
// write path. Adding a new field type or capture site MUST update this
// file.
package errtrack

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/trackrecord/enclave/internal/logredact"
	"go.uber.org/zap/zapcore"
)

// SanitizedEvent is the redacted snapshot of a log entry that we keep in
// the store. The shape is intentionally narrow: only fields useful for
// triage, none that could carry secrets.
type SanitizedEvent struct {
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Caller    string            `json:"caller,omitempty"`
	Frames    []Frame           `json:"frames,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
}

// SanitizeMessage redacts the entry message via the same rules as
// logredact, then truncates to a hard ceiling so a megabyte-long error
// from a misbehaving connector cannot blow up the store.
func SanitizeMessage(s string) string {
	const maxMsgLen = 2048
	out := logredact.ScrubMessage(s)
	if len(out) > maxMsgLen {
		out = out[:maxMsgLen] + "...[truncated]"
	}
	return out
}

// SanitizeFields converts a slice of zapcore.Field into a flat
// map[string]string after running every value through the same redactor
// that protects stderr. Only stable, scalar field types are kept — object
// and array marshalers are dropped because they encode piecewise into the
// zap encoder and we cannot scrub them safely here (mirrors the policy in
// logredact.maskFieldValue).
//
// The output values are always strings to keep JSON marshalling
// deterministic and to dodge any edge case where a custom MarshalJSON
// could emit unscrubbed content.
func SanitizeFields(fields []zapcore.Field) map[string]string {
	if len(fields) == 0 {
		return nil
	}
	scrubbed := logredact.RedactFields(fields)
	out := make(map[string]string, len(scrubbed))
	for _, f := range scrubbed {
		v, ok := fieldToString(f)
		if !ok {
			continue
		}
		// Defense-in-depth: re-run the scrub on the stringified value.
		// Most cases are already redacted by RedactFields above; this
		// catches anything that might have slipped through a future
		// regression.
		v = logredact.ScrubMessage(v)
		out[f.Key] = truncate(v, 512)
	}
	return out
}

// fieldToString renders the supported scalar zap field types as a string.
// Returns ok=false for unsupported types so the caller drops them.
func fieldToString(f zapcore.Field) (string, bool) {
	switch f.Type {
	case zapcore.StringType:
		return f.String, true
	case zapcore.BoolType:
		if f.Integer == 1 {
			return "true", true
		}
		return "false", true
	case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type:
		return fmt.Sprintf("%d", f.Integer), true
	case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type:
		return fmt.Sprintf("%d", uint64(f.Integer)), true
	case zapcore.Float32Type, zapcore.Float64Type:
		// zap stores floats by reinterpreting their bit pattern as int64
		// in f.Integer; cast it back through math.Float64frombits.
		return fmt.Sprintf("%g", math.Float64frombits(uint64(f.Integer))), true
	case zapcore.DurationType:
		return fmt.Sprintf("%dns", f.Integer), true
	case zapcore.ErrorType:
		if err, ok := f.Interface.(error); ok && err != nil {
			return err.Error(), true
		}
		return "", false
	case zapcore.ReflectType:
		// Marshal once to JSON and treat as a string. logredact already
		// did the same and replaced the field if it contained secrets;
		// if we land here it means RedactFields decided it was clean.
		raw, err := json.Marshal(f.Interface)
		if err != nil {
			return "", false
		}
		return string(raw), true
	}
	return "", false
}

// SanitizeRecovered turns a recover() payload into a string and scrubs it.
// Panic payloads are arbitrary interface{}, so we cap them aggressively.
func SanitizeRecovered(v interface{}) string {
	if v == nil {
		return ""
	}
	var s string
	switch t := v.(type) {
	case string:
		s = t
	case error:
		s = t.Error()
	default:
		s = fmt.Sprintf("%v", v)
	}
	return SanitizeMessage(s)
}

// SanitizeStack parses a zap-style stacktrace string (the format zap emits
// when AddStacktrace is configured) and returns a slice of frames with
// absolute filesystem paths trimmed to the last meaningful segment. Lines
// that cannot be parsed are dropped silently — we never want a malformed
// trace to crash capture.
//
// Format produced by zap:
//
//	github.com/owner/pkg.FuncName
//	    /abs/path/file.go:42
//	github.com/owner/pkg.OtherFunc
//	    /abs/path/file2.go:13
//
// Indentation in the path lines is a tab.
func SanitizeStack(raw string) []Frame {
	if raw == "" {
		return nil
	}
	lines := strings.Split(raw, "\n")
	frames := make([]Frame, 0, len(lines)/2)
	const maxFrames = 64
	for i := 0; i+1 < len(lines) && len(frames) < maxFrames; i++ {
		fnLine := strings.TrimSpace(lines[i])
		if fnLine == "" {
			continue
		}
		// Path line is the next non-empty line; some zap configs emit
		// it on the immediately-following line indented with a tab.
		pathLine := strings.TrimSpace(lines[i+1])
		if pathLine == "" || !strings.Contains(pathLine, ":") {
			continue
		}
		// Sanity check: function lines never start with `/` or contain
		// `.go:` (those belong to the path line). If both lines look
		// like paths or both look like functions, skip.
		if strings.HasPrefix(fnLine, "/") || strings.Contains(fnLine, ".go:") {
			continue
		}
		f := Frame{
			Function: truncate(fnLine, 256),
			File:     trimFilePath(pathLine),
		}
		frames = append(frames, f)
		i++ // consume the path line
	}
	return frames
}

// Frame is one normalized stack frame. File is intentionally relative
// (last few path segments) so we never leak the build host's directory
// layout in attestation-bound output.
type Frame struct {
	Function string `json:"function"`
	File     string `json:"file"`
}

// trimFilePath collapses an absolute build path into a stable,
// relative-looking suffix. Two concrete examples:
//
//	/build/enclave/internal/server/handler.go:42
//	  -> internal/server/handler.go:42
//	D:/Dev/zero-knowledge-aggregator-go/cmd/enclave/main.go:108
//	  -> cmd/enclave/main.go:108
//
// Stripping the leading prefix has two purposes:
//
//  1. Avoid leaking the builder's filesystem layout in stored samples.
//     This matters for both Linux container builds (`/build/...`) and
//     local Windows dev paths (`D:/Dev/...`, `C:/Users/...`).
//  2. Keep fingerprints stable across builds that may have been compiled
//     in different working directories.
//
// Inputs that are already relative (no leading slash and no Windows
// drive letter) are returned unchanged so legitimate package paths like
// `github.com/trackrecord/...` survive intact.
func trimFilePath(s string) string {
	if !isAbsoluteFSPath(s) {
		return s
	}
	// Go module cache: /...somewhere.../pkg/mod/<host>/<module>/...
	if idx := strings.Index(s, "/pkg/mod/"); idx >= 0 {
		return s[idx+len("/pkg/mod/"):]
	}
	// Project anchors. Order matters: /internal/, /cmd/, /api/ are
	// more specific than /pkg/, so check them first.
	for _, anchor := range []string{"/internal/", "/cmd/", "/api/", "/pkg/"} {
		if idx := strings.Index(s, anchor); idx >= 0 {
			return strings.TrimPrefix(s[idx:], "/")
		}
	}
	// Fallback: basename only. We never want an absolute path to
	// reach the store unfiltered.
	if idx := strings.LastIndex(s, "/"); idx >= 0 {
		return s[idx+1:]
	}
	return s
}

// isAbsoluteFSPath reports whether s looks like an absolute path on any
// supported host: POSIX (`/foo`) or Windows-style as Go's runtime emits
// it (forward-slash separators with a drive letter, e.g. `D:/foo` or
// `C:\foo`). UNC paths (`\\server\...`) are also treated as absolute.
func isAbsoluteFSPath(s string) bool {
	if len(s) == 0 {
		return false
	}
	if s[0] == '/' || s[0] == '\\' {
		return true
	}
	// Windows drive letter: `[A-Za-z]:[/\\]`
	if len(s) >= 3 && s[1] == ':' && (s[2] == '/' || s[2] == '\\') {
		c := s[0]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			return true
		}
	}
	return false
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...[truncated]"
}
