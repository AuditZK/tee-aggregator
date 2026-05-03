package logredact

// AUDIT — LOG-001 regression test.
//
// Original finding: doc/audit/findings/LOG-001-zap-any-bypasses-redaction.md.
// `maskFieldValue` used to inspect only StringType / ErrorType. A field
// emitted via zap.Any / zap.Reflect with a credential-bearing struct value
// (signed URLs, OAuth client_secret, HTTP Basic headers, signed query
// params) would render unscrubbed in the log output.
//
// Status: **fixed**. ReflectType is now marshalled to JSON and run through
// the same maskValue / scrubSensitiveSubstrings pipeline. This test asserts
// the new behaviour — it must keep passing as long as the redactor is
// correct.

import (
	"strings"
	"testing"

	"go.uber.org/zap"
)

// TestAuditLogRedactZapAnyScrubsSignature proves that a struct field
// containing a `signature` URL parameter is redacted when emitted via
// zap.Any. Pre-fix this would have rendered the literal signature.
func TestAuditLogRedactZapAnyScrubsSignature(t *testing.T) {
	type apiResponse struct {
		Status string `json:"status"`
		URL    string `json:"url"`
	}

	val := apiResponse{
		Status: "ok",
		URL:    "https://example.com/cb?ts=1&signature=abcdef0123456789",
	}
	field := zap.Any("response", val)

	got := maskFieldValue(field)
	rendered := got.String

	if rendered == "" {
		t.Fatalf("expected scrubbed string output, got empty (field type=%v)", got.Type)
	}
	if strings.Contains(rendered, "abcdef0123456789") {
		t.Fatalf("AUDIT LOG-001: signature value leaked through zap.Any: %q", rendered)
	}
	if !strings.Contains(rendered, redacted) {
		t.Fatalf("expected %q marker in scrubbed output, got %q", redacted, rendered)
	}
}

// TestAuditLogRedactZapAnyScrubsBasicAuth proves the same protection
// applies to HTTP Basic Authorization headers nested in struct values.
func TestAuditLogRedactZapAnyScrubsBasicAuth(t *testing.T) {
	type httpDump struct {
		Method  string `json:"method"`
		Headers string `json:"headers"`
	}
	val := httpDump{
		Method:  "POST",
		Headers: "Authorization: Basic dXNlcjpwYXNzd29yZA==",
	}
	field := zap.Any("dump", val)
	got := maskFieldValue(field)

	if strings.Contains(got.String, "dXNlcjpwYXNzd29yZA") {
		t.Fatalf("AUDIT LOG-001: basic-auth payload leaked through zap.Any: %q", got.String)
	}
}

// TestAuditLogRedactZapAnyApiKeyKVScrubbed proves that an inline
// `api_key=...` substring is dropped (the rule kicks in via
// containsSensitiveValue).
func TestAuditLogRedactZapAnyApiKeyKVScrubbed(t *testing.T) {
	type echoBody struct {
		Body string `json:"body"`
	}
	val := echoBody{Body: "request: api_key=sk_live_abc123"}
	field := zap.Any("payload", val)
	got := maskFieldValue(field)

	if strings.Contains(got.String, "sk_live_abc123") {
		t.Fatalf("AUDIT LOG-001: api_key value leaked through zap.Any: %q", got.String)
	}
	if got.String != redacted {
		t.Fatalf("expected entire payload to collapse to %q, got %q", redacted, got.String)
	}
}
