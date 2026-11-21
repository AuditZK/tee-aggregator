package service

import (
	"errors"
	"strings"
	"testing"
)

// E-H5: classifySyncError already told an OAuth death from a network blip,
// but only in the log. What reached sync_statuses (and from there the gateway
// and the dashboard) was "get balance: sync failed" for both. These are the
// error strings the cTrader connector actually produces.
func TestEgressAndMarker_OAuthDeathIsActionable(t *testing.T) {
	cases := []string{
		"cTrader error ACCESS_DENIED: token is not valid",
		"token refresh rejected: invalid_grant - refresh token already used",
		"token refresh failed (HTTP 400): {\"errorCode\":\"invalid_grant\"}",
		"token refresh failed (HTTP 401): unauthorized",
		"cTrader error CH_ACCESS_TOKEN_INVALID: Access token expired",
		"persist rotated cTrader tokens after 3 attempts (the stored refresh token is now dead; the connection must be re-authorized): pool closed",
	}
	for _, raw := range cases {
		t.Run(raw[:min(40, len(raw))], func(t *testing.T) {
			egress := egressSyncError("get balance", errors.New(raw))
			if !strings.Contains(egress, "broker authorization expired") {
				t.Fatalf("egress = %q, want the re-authorization category", egress)
			}
			if got := syncStatusMarker(egress); got != "reauth_required" {
				t.Fatalf("marker = %q, want reauth_required", got)
			}
			// The log fingerprint keeps saying the same thing it always did.
			if got := classifySyncError(raw); got != "sync: OAuth refresh failed" {
				t.Logf("log fingerprint for %q is %q", raw, got)
			}
		})
	}
}

func TestEgressAndMarker_RateLimitIsNotACredentialProblem(t *testing.T) {
	cases := []string{
		"cTrader error BLOCKED_PAYLOAD_TYPE: You are being rate limited",
		"cTrader token refresh rate-limited (429), retry later",
		"cTrader rate limited on payloadType 2133 after 5 attempts: blocked",
	}
	for _, raw := range cases {
		t.Run(raw[:min(40, len(raw))], func(t *testing.T) {
			egress := egressSyncError("get balance", errors.New(raw))
			if !strings.Contains(egress, "rate limited by the broker") {
				t.Fatalf("egress = %q, want the rate-limit category", egress)
			}
			if got := syncStatusMarker(egress); got != "rate_limited" {
				t.Fatalf("marker = %q, want rate_limited", got)
			}
		})
	}
}

// A genuinely wrong API key must still read as a credential problem, and an
// unclassified failure must carry no marker at all — a marker downstream keys
// off must never be guessed.
func TestSyncStatusMarker_LeavesOtherFailuresAlone(t *testing.T) {
	for _, tc := range []struct{ raw, wantIn string }{
		{"binance: invalid api key for this action", "invalid API key"},
		{"dial tcp: no such host", "could not reach the exchange endpoint"},
		{"pgx: relation does not exist", "sync failed"},
	} {
		egress := egressSyncError("get balance", errors.New(tc.raw))
		if !strings.Contains(egress, tc.wantIn) {
			t.Errorf("egress(%q) = %q, want it to contain %q", tc.raw, egress, tc.wantIn)
		}
		if got := syncStatusMarker(egress); got != "" {
			t.Errorf("egress(%q) got marker %q, want none", tc.raw, got)
		}
	}
}
