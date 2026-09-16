package connector

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The bug this gate exists for: two IBKR connections sharing one Flex token
// (CTO+PEA, two query IDs) raced into IBKR within the same millisecond every
// midnight — observed at 00:00:18.185 and 00:00:18.422 on 2026-08-27 — and
// the loser burned a second request on its follow-up GetBalance. IBKR's 1018
// limit is per TOKEN; the report cache and singleflight are keyed
// token:queryID, so neither protected this case.

func TestClaimFlexToken_SecondClaimWithinCooldownDenied(t *testing.T) {
	token := "gate-test-token-A"
	now := time.Now()

	if _, ok := claimFlexToken(token, now); !ok {
		t.Fatal("first claim on a fresh token must be granted")
	}
	retryAt, ok := claimFlexToken(token, now.Add(237*time.Millisecond))
	if ok {
		t.Fatal("second claim 237ms later must be denied — this is the exact CTO/PEA race")
	}
	if want := now.Add(flexTokenCooldown); !retryAt.Equal(want) {
		t.Fatalf("denial must say when the token reopens, got %v want %v", retryAt, want)
	}
}

func TestClaimFlexToken_DistinctTokensDoNotInterfere(t *testing.T) {
	now := time.Now()
	if _, ok := claimFlexToken("gate-test-token-B1", now); !ok {
		t.Fatal("first token must claim")
	}
	if _, ok := claimFlexToken("gate-test-token-B2", now); !ok {
		t.Fatal("a different token must not be blocked by the first one's claim")
	}
}

func TestClaimFlexToken_ReopensAfterCooldown(t *testing.T) {
	token := "gate-test-token-C"
	now := time.Now()

	if _, ok := claimFlexToken(token, now); !ok {
		t.Fatal("initial claim must be granted")
	}
	if _, ok := claimFlexToken(token, now.Add(flexTokenCooldown-time.Second)); ok {
		t.Fatal("claim just inside the cooldown must be denied")
	}
	// The denied attempt must NOT have refreshed the claim: the window is
	// measured from the granted request, or the deferred retry could be
	// pushed back forever by its own probes.
	if _, ok := claimFlexToken(token, now.Add(flexTokenCooldown+time.Second)); !ok {
		t.Fatal("claim just past the cooldown must be granted — the 6h deferred retry depends on it")
	}
}

func TestErrFlexTokenBusy_IsTransientAndMatchesRateLimitPredicate(t *testing.T) {
	// Shaped exactly as fetchFlexReport emits it.
	err := fmt.Errorf("%w (retry after ~Sep 16 03:00 UTC)", ErrFlexTokenBusy)

	if !errors.Is(err, ErrTransient) {
		t.Fatal("token-busy must be transient — it is pacing, not a credential failure")
	}
	if !errors.Is(err, ErrFlexTokenBusy) {
		t.Fatal("the sentinel must survive wrapping so the sync layer can downgrade its log")
	}
	// The sync layer matches on this substring (isRateLimitError) to arm the
	// deferred retry and record status "pending". Renaming the message
	// silently disarms both — this test is the tripwire.
	if !strings.Contains(err.Error(), "shared flex token cooling down") {
		t.Fatalf("error text must carry the machine-readable marker, got: %s", err.Error())
	}
}

// What follows pins the 2026-09-15 incident: nine attempts in forty minutes on
// a token answering 1001 earned error 1025 and took a paying customer's
// account off the air the day after he subscribed. IBKR counts failures, so a
// failure must cost more than the next minute.

func TestClaimFlexToken_WaitDoublesAfterEachFailure(t *testing.T) {
	token := "gate-test-token-D"
	start := time.Date(2026, 9, 15, 2, 0, 0, 0, time.UTC)

	if _, ok := claimFlexToken(token, start); !ok {
		t.Fatal("first claim must be granted")
	}
	noteFlexOutcome(token, start, errors.New("flex request failed: 1001"))

	// One failure in: the minute that paced a success is no longer enough.
	if _, ok := claimFlexToken(token, start.Add(90*time.Second)); ok {
		t.Fatal("90s after a failure must be denied — one minute is what let the loop run")
	}
	second := start.Add(2*time.Minute + time.Second)
	if _, ok := claimFlexToken(token, second); !ok {
		t.Fatal("two minutes after one failure must be granted")
	}
	noteFlexOutcome(token, second, errors.New("flex request failed: 1001"))

	if _, ok := claimFlexToken(token, second.Add(3*time.Minute)); ok {
		t.Fatal("three minutes after the second failure must be denied — the wait is now four")
	}
	if _, ok := claimFlexToken(token, second.Add(4*time.Minute+time.Second)); !ok {
		t.Fatal("four minutes after the second failure must be granted")
	}
}

func TestNoteFlexOutcome_SuccessClearsTheBackoff(t *testing.T) {
	token := "gate-test-token-E"
	start := time.Date(2026, 9, 15, 2, 0, 0, 0, time.UTC)

	for _, at := range []time.Time{start, start.Add(2 * time.Hour), start.Add(4 * time.Hour)} {
		if _, ok := claimFlexToken(token, at); !ok {
			t.Fatalf("claim at %v must be granted", at)
		}
		noteFlexOutcome(token, at, errors.New("flex request failed: 1001"))
	}

	success := start.Add(6 * time.Hour)
	if _, ok := claimFlexToken(token, success); !ok {
		t.Fatal("claim after the widened wait must be granted")
	}
	noteFlexOutcome(token, success, nil)

	// Back to the success cadence, not the four-failure one.
	if _, ok := claimFlexToken(token, success.Add(flexTokenCooldown+time.Second)); !ok {
		t.Fatal("a minute after a success must be granted again")
	}
}

func TestClaimFlexToken_DailyFailureBudgetStopsTheDay(t *testing.T) {
	token := "gate-test-token-F"
	start := time.Date(2026, 9, 15, 2, 0, 0, 0, time.UTC)

	// Spaced past the longest back-off, so only the budget can deny.
	at := start
	for n := 0; n < flexTokenDailyFailures; n++ {
		if _, ok := claimFlexToken(token, at); !ok {
			t.Fatalf("attempt %d must be granted, the budget is not spent yet", n+1)
		}
		noteFlexOutcome(token, at, errors.New("flex request failed: 1001"))
		at = at.Add(flexTokenMaxBackoff)
	}

	retryAt, ok := claimFlexToken(token, at)
	if ok {
		t.Fatalf("attempt %d must be denied, the day's budget is spent", flexTokenDailyFailures+1)
	}
	if want := nextUTCDay(at); !retryAt.Equal(want) {
		t.Fatalf("a spent budget must wait for the next UTC day, got %v want %v", retryAt, want)
	}
	// Tomorrow the scheduled midnight sync gets a fresh budget.
	if _, ok := claimFlexToken(token, nextUTCDay(at).Add(time.Minute)); !ok {
		t.Fatal("the next UTC day must grant again")
	}
}

func TestNoteFlexOutcome_RepeatedFailureCodeStopsUntilTomorrow(t *testing.T) {
	token := "gate-test-token-G"
	at := time.Date(2026, 9, 16, 0, 57, 0, 0, time.UTC)

	if _, ok := claimFlexToken(token, at); !ok {
		t.Fatal("first claim must be granted")
	}
	noteFlexOutcome(token, at, fmt.Errorf("%w: flex request failed: 1025 - Too many failed attempts", ErrFlexConfigLocked))

	retryAt, ok := claimFlexToken(token, at.Add(6*time.Hour))
	if ok {
		t.Fatal("1025 must stop every attempt for the rest of the day, whatever the back-off says")
	}
	if want := nextUTCDay(at); !retryAt.Equal(want) {
		t.Fatalf("1025 must hold until the next UTC day, got %v want %v", retryAt, want)
	}
	if _, ok := claimFlexToken(token, nextUTCDay(at).Add(time.Minute)); !ok {
		t.Fatal("the lock must lift on the next UTC day")
	}
}

func TestErrFlexConfigLocked_IsTransientNotACredentialProblem(t *testing.T) {
	err := fmt.Errorf("%w: flex request failed: 1025 - Too many failed attempts. Please review your configuration.", ErrFlexConfigLocked)

	if !errors.Is(err, ErrTransient) {
		t.Fatal("1025 is a lock we earned, not a bad token — sending the holder to regenerate would not clear it")
	}
	if !errors.Is(err, ErrFlexConfigLocked) {
		t.Fatal("the sentinel must survive wrapping, it is what arms the day-long stop")
	}
}

func TestFlexTokenWait_CapsAtTheCeiling(t *testing.T) {
	if got := flexTokenWait(0); got != flexTokenCooldown {
		t.Fatalf("no failure yet: got %v want %v", got, flexTokenCooldown)
	}
	if got := flexTokenWait(3); got != 8*time.Minute {
		t.Fatalf("three failures: got %v want 8m", got)
	}
	if got := flexTokenWait(40); got != flexTokenMaxBackoff {
		t.Fatalf("a long run of failures must not overflow into a negative wait: got %v want %v", got, flexTokenMaxBackoff)
	}
}
