package logredact

import (
	"strings"
	"testing"
)

// SEC-11 defence in depth: the IBKR Flex token can only travel as ?t=<token>,
// which no tier-1 field-name prefix catches — the leak arrives inside a free
// text error message, not in a field named after a credential.
func TestScrubFlexTokenInQueryString(t *testing.T) {
	const token = "SYNTHETIC1234567890TOKEN"
	raw := `Get "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest?q=999999&t=` +
		token + `&v=3": dial tcp: lookup failed`

	got := scrubSensitiveSubstrings(raw)
	if strings.Contains(got, token) {
		t.Fatalf("flex token survived the scrubber: %s", got)
	}
	if !strings.Contains(got, "q=999999") {
		t.Fatalf("scrub ate the surrounding diagnostic text: %s", got)
	}
}

// The delimiter anchor exists so a longer parameter ending in "t" is not
// mistaken for the Flex token.
func TestScrubFlexTokenLeavesOtherParams(t *testing.T) {
	raw := "https://example.test/x?account=A1&at=keepme&start=2026-01-01"
	if got := scrubSensitiveSubstrings(raw); !strings.Contains(got, "at=keepme") {
		t.Fatalf("unrelated parameter redacted: %s", got)
	}
}
