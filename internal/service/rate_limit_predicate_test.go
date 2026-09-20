package service

import (
	"errors"
	"fmt"
	"testing"

	"github.com/trackrecord/enclave/internal/connector"
)

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
		// What the same failures look like once egressSyncError has run.
		"get balance: rate limited by the broker, retrying later",
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

// The deferred retry is armed from the stored result, and the stored result
// carries a sanitized Error. Between 2026-09-09 and 2026-09-20 that text was
// "get balance: sync failed" for the loser of a shared Flex token race, the
// predicate saw nothing to match, and the loser waited for the next midnight
// pass, where it lost or won by turns. The flag is set where the raw error
// still exists; the text is a fallback, and now names the class too.
func TestSyncResultRateLimited_SurvivesSanitizing(t *testing.T) {
	raw := []error{
		fmt.Errorf("%w (retry after ~Sep 20 06:00 UTC)", connector.ErrFlexTokenBusy),
		fmt.Errorf("%w: flex request failed: 1001 - Statement could not be generated at this time. Please try again shortly.", connector.ErrTransient),
		fmt.Errorf("%w: flex request failed: 1018 - Too many requests have been made from this token.", connector.ErrTransient),
	}
	for _, err := range raw {
		r := &SyncResult{Error: egressSyncError("get balance", err), RateLimited: isRateLimitError(err.Error())}
		if !r.RateLimited {
			t.Fatalf("raw %q must be recognised at the failure site", err)
		}
		if !r.rateLimited() {
			t.Fatalf("result with Error=%q must arm the retry", r.Error)
		}
		if got := syncStatusMarker(r.Error); got != "rate_limited" {
			t.Fatalf("marker for %q = %q, want rate_limited", r.Error, got)
		}
	}

	// A failure that is not pacing stays out, flag and text alike.
	r := &SyncResult{Error: egressSyncError("get balance", errors.New("invalid credentials"))}
	if r.rateLimited() {
		t.Fatalf("a credential failure must not arm the retry: %q", r.Error)
	}
}
