package service

import "testing"

// isRateLimitError is the single predicate that (a) arms the 6h deferred
// retry and (b) flips the recorded status to "pending" instead of "error".
// All its producers must keep matching it: IBKR's own 1018 code, its prose
// variant, the local token gate's typed error, and any statement IBKR
// declined to generate.
func TestIsRateLimitError(t *testing.T) {
	matching := []string{
		"get balance: transient connector error: flex request failed: 1018 - Too many requests have been made from this token. Please try again shortly.",
		"flex request failed: 1018",
		"Too many requests",
		"get balance: transient connector error: shared flex token cooling down (retry after ~Sep 17 06:00 UTC)",
		// A refusal to generate. It used to be excluded because it "clears in
		// seconds": true during a trading day, false at 00:00 UTC, which is
		// the middle of IBKR's own end-of-day processing and when the daily
		// pass runs. Excluded, the connection skipped the 6h retry that
		// recovers every other IBKR account and waited a full day for a pass
		// that landed in the same window and refused again.
		"get balance: transient connector error: flex request failed: 1001 - Statement could not be generated at this time. Please try again shortly.",
		"get balance: transient connector error: flex request failed: 1019 - Statement generation in progress",
	}
	for _, s := range matching {
		if !isRateLimitError(s) {
			t.Errorf("must match, did not: %q", s)
		}
	}

	// The match is the connector's own judgement: it wraps only the refusals
	// it considers transient in ErrTransient. A credential problem, an
	// unsupported query or anything from another venue stays out, and no
	// second list of Flex codes is kept here to drift from that one.
	nonMatching := []string{
		"",
		"token refresh rejected: ACCESS_DENIED",
		"get balance: invalid credentials",
		"get balance: flex request failed: 1015 - Token is invalid",
		"get balance: transient connector error: okx request failed: 50011",
	}
	for _, s := range nonMatching {
		if isRateLimitError(s) {
			t.Errorf("must not match, did: %q", s)
		}
	}
}
