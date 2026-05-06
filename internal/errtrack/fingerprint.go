package errtrack

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"go.uber.org/zap/zapcore"
)

// fingerprintTopFrames is how many frames participate in the grouping
// hash. Sentry uses a similar truncation; the goal is that two
// occurrences of the same bug produce the same fingerprint even if the
// deepest application code is identical but the leaf framework call
// differs slightly between runs.
const fingerprintTopFrames = 5

// fingerprintIDLen is the hex length of the visible group id. 16 hex =
// 64 bits of fingerprint, plenty to avoid collisions for the small
// number of distinct error sites a single enclave can produce.
const fingerprintIDLen = 16

// Fingerprint computes the grouping key for a captured error. It is
// derived from:
//
//   - the log level,
//   - the (function-only) projection of the top frames,
//   - a de-variabilized version of the sanitized message.
//
// Paths and line numbers are intentionally excluded so the fingerprint
// is stable across builds and across small refactors that only move
// code within the same function. The message is included so two
// distinct errors emitted from the same call site (very common in a
// `main` function or a top-level handler) don't collapse into one
// group — Sentry uses the same approach with its `type + value + stack`
// triple.
//
// Function names fed into the hash are normalized via
// normalizeFunction so that anonymous wrappers and goroutine launchers
// don't fragment otherwise-identical errors into many groups.
//
// The sanitizedMsg is expected to have already passed through
// SanitizeMessage; its content is then run through stripVariableRuns
// to drop counters, ids, and other variable fragments before hashing.
func Fingerprint(level zapcore.Level, sanitizedMsg string, frames []Frame) string {
	h := sha256.New()
	h.Write([]byte(level.String()))
	h.Write([]byte{0})
	n := len(frames)
	if n > fingerprintTopFrames {
		n = fingerprintTopFrames
	}
	for i := 0; i < n; i++ {
		h.Write([]byte(normalizeFunction(frames[i].Function)))
		h.Write([]byte{0})
	}
	h.Write([]byte(stripVariableRuns(sanitizedMsg)))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)[:fingerprintIDLen]
}

// FingerprintFromMessage is the fallback used when no frames are
// available (entry was emitted below ErrorLevel without a stack, or
// AddStacktrace was disabled). It hashes the level plus a coarse,
// already-sanitized message digest so we still produce a stable group
// id without leaking message content into the id.
//
// We hash the WHOLE sanitized message: because the message has already
// been scrubbed of secrets, the hash of secret-free text is also
// secret-free, but could still vary on every call if the message
// contains varying parts (request IDs, durations, …). To prevent
// fragmentation we strip digits and quoted runs before hashing.
func FingerprintFromMessage(level zapcore.Level, sanitizedMsg string) string {
	h := sha256.New()
	h.Write([]byte(level.String()))
	h.Write([]byte{0})
	h.Write([]byte(stripVariableRuns(sanitizedMsg)))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)[:fingerprintIDLen]
}

// normalizeFunction collapses dynamically-generated function name
// suffixes that Go appends to closures, methods, and generics-instantiated
// functions. Without this, every closure call site gets its own
// fingerprint even when the surrounding error is identical.
func normalizeFunction(fn string) string {
	// Closures: "pkg.Foo.func1.2" -> "pkg.Foo"
	if idx := strings.Index(fn, ".func"); idx >= 0 {
		fn = fn[:idx]
	}
	// Generic instantiations: "pkg.Foo[...]" -> "pkg.Foo"
	if idx := strings.Index(fn, "["); idx >= 0 {
		fn = fn[:idx]
	}
	return fn
}

// stripVariableRuns drops fragments that typically vary between
// invocations of the same logical error so messages like
//
//	"sync 17 failed for connection abc123"
//	"sync 19 failed for connection xyz789"
//
// hash to the same fingerprint. We:
//
//   - drop quoted runs entirely (often arbitrary user input),
//   - drop any "word" token that contains a digit (numeric counters,
//     uuids, hex blobs, alphanumeric ids),
//   - collapse whitespace runs to a single space.
//
// The function operates byte-by-byte. ASCII-only is fine here because
// the only fragments we want to preserve are punctuation and
// alphabetic words; any non-ASCII run is treated as part of a word
// and kept-or-dropped with the surrounding token.
func stripVariableRuns(s string) string {
	var (
		out strings.Builder
		buf []byte
		inQ bool
	)
	out.Grow(len(s))

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\'':
			buf = flushWordToken(&out, buf)
			inQ = !inQ
		case inQ:
			// drop quoted content
		case isWordByte(c):
			buf = append(buf, c)
		default:
			buf = flushWordToken(&out, buf)
			out.WriteByte(c)
		}
	}
	flushWordToken(&out, buf)
	return strings.Join(strings.Fields(out.String()), " ")
}

// flushWordToken writes buf to out if and only if buf contains no
// digits, then returns an empty slice (reusing buf's backing array).
// Tokens that contain digits are treated as identifiers/counters and
// dropped.
func flushWordToken(out *strings.Builder, buf []byte) []byte {
	if len(buf) == 0 {
		return buf
	}
	for _, c := range buf {
		if c >= '0' && c <= '9' {
			return buf[:0]
		}
	}
	out.Write(buf)
	return buf[:0]
}

// isWordByte returns true for ASCII letters, digits, underscore, and
// any non-ASCII byte (treated as part of an identifier).
func isWordByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z':
		return true
	case c >= 'A' && c <= 'Z':
		return true
	case c >= '0' && c <= '9':
		return true
	case c == '_':
		return true
	case c >= 0x80:
		return true
	}
	return false
}
