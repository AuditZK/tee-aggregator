package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// stubOAuthConnector reports tokens that differ from the ones it was built
// with — what a connector looks like after a refresh happened during
// validation.
type stubOAuthConnector struct {
	access, refresh string
}

func (s *stubOAuthConnector) GetBalance(context.Context) (*connector.Balance, error) { return nil, nil }
func (s *stubOAuthConnector) GetPositions(context.Context) ([]*connector.Position, error) {
	return nil, nil
}
func (s *stubOAuthConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	return nil, nil
}
func (s *stubOAuthConnector) TestConnection(context.Context) error { return nil }
func (s *stubOAuthConnector) Exchange() string                     { return "ctrader" }
func (s *stubOAuthConnector) CurrentCredentials() (string, string) { return s.access, s.refresh }

// E-H4: validating a connection can refresh it, and cTrader rotates the
// refresh token every time. Storing the request's pair wrote a token the
// broker had just invalidated, so the connection was born dead.
func TestEffectiveOAuthCredentials_PrefersWhatTheConnectorHolds(t *testing.T) {
	conn := &stubOAuthConnector{access: "rotated-access", refresh: "rotated-refresh"}
	access, refresh := effectiveOAuthCredentials(conn, "submitted-access", "submitted-refresh")
	if access != "rotated-access" || refresh != "rotated-refresh" {
		t.Fatalf("got %q/%q, want the rotated pair", access, refresh)
	}
}

// A connector that reports nothing must not blank the stored credentials.
func TestEffectiveOAuthCredentials_FallsBackOnEmpty(t *testing.T) {
	conn := &stubOAuthConnector{access: "", refresh: "  "}
	access, refresh := effectiveOAuthCredentials(conn, "submitted-access", "submitted-refresh")
	if access != "submitted-access" || refresh != "submitted-refresh" {
		t.Fatalf("got %q/%q, want the submitted pair", access, refresh)
	}
}

// Non-OAuth connectors are untouched: same values in, same values out.
func TestEffectiveOAuthCredentials_IgnoresNonOAuthConnectors(t *testing.T) {
	type keyOnly struct{ connector.Connector }
	access, refresh := effectiveOAuthCredentials(keyOnly{}, "key", "secret")
	if access != "key" || refresh != "secret" {
		t.Fatalf("got %q/%q, want key/secret", access, refresh)
	}
}

// C3: re-authorizing must not overwrite a possibly-working authorization with
// one the broker refused, but must go through when the broker simply did not
// answer — the user has just come out of a consent screen and the stored pair
// is the one we suspect is dead.
func TestReauthValidationOutcome(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantStore bool
		wantIn    string
	}{
		{"clean validation", nil, true, ""},
		{"broker unreachable", errors.New("dial tcp: connection refused"), true, ""},
		{"broker throttling", errors.New("cTrader error BLOCKED_PAYLOAD_TYPE: rate limited"), true, ""},
		{"explicitly transient", fmt.Errorf("busy: %w", connector.ErrTransient), true, ""},
		{"broker refused the token", errors.New("cTrader error ACCESS_DENIED: token is not valid"), false, "invalid credentials"},
		{"enclave not configured", errors.New("cTrader requires CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET environment variables"), false, "CTRADER_CLIENT_ID"},
		{"no tradable account", errors.New("no cTrader accounts found"), false, "no cTrader accounts"},
		{"uncharacterised", errors.New("something new"), false, "invalid credentials"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reauthValidationOutcome(tc.err)
			if tc.wantStore {
				if got != nil {
					t.Fatalf("want the new tokens stored, got refusal %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("want a refusal, got nil (the good tokens would be overwritten)")
			}
			if !strings.Contains(got.Error(), tc.wantIn) {
				t.Fatalf("error %q should mention %q", got, tc.wantIn)
			}
		})
	}
}

// The re-authorization path is OAuth-only: for a key-based exchange,
// connecting the same label twice stays an already-exists conflict that the
// gateway turns into a 409.
func TestIsOAuthExchange(t *testing.T) {
	if !isOAuthExchange("ctrader") {
		t.Fatal("ctrader is the OAuth broker; re-auth must be routed for it")
	}
	for _, e := range []string{"binance", "ibkr", "mt5", "kraken", "okx", ""} {
		if isOAuthExchange(e) {
			t.Fatalf("%s must keep the already-exists behaviour", e)
		}
	}
}
