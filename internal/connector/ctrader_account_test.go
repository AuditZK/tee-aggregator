package connector

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/trackrecord/enclave/internal/errsanitize"
)

// A token granting two live accounts, with nothing saying which one the
// connection is, syncs the first; the sync status must say it guessed.
func TestCTraderGetBalance_WarnsWhenTheAccountIsAGuess(t *testing.T) {
	c := newCTraderBalanceServer(t, &ctraderBalanceOpts{
		accounts: []map[string]any{
			{"ctidTraderAccountId": 12345, "isLive": true},
			{"ctidTraderAccountId": 67890, "isLive": true},
		},
	})
	if _, err := c.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !slices.Contains(c.CapabilityWarnings(), "multiple_live_accounts_unpinned") {
		t.Fatalf("warnings %v, want multiple_live_accounts_unpinned", c.CapabilityWarnings())
	}
}

// The passphrase slot carries either the demo routing seed or, behind its
// prefix, the account the connection was created for. Connections created
// before 2026-09-09 hold the token lifetime there; read as an account, it
// failed every one of them in production as needing re-authorization.
func TestNewCTrader_ReadsThePassphrase(t *testing.T) {
	cases := []struct {
		passphrase string
		wantLive   bool
		wantPinned int64
	}{
		{"", true, 0},
		{"demo", false, 0},
		{" DEMO ", false, 0},
		{"ctid:41234567", true, 41234567},
		{"2628000", true, 0},
		{"41234567", true, 0},
		{"ctid:-5", true, 0},
		{"ctid:", true, 0},
		{"account-7", true, 0},
	}
	for _, tc := range cases {
		c := NewCTrader(&Credentials{Passphrase: tc.passphrase})
		if c.isLive != tc.wantLive || c.pinnedAccountID != tc.wantPinned {
			t.Errorf("passphrase %q: live=%v pinned=%d, want live=%v pinned=%d",
				tc.passphrase, c.isLive, c.pinnedAccountID, tc.wantLive, tc.wantPinned)
		}
	}
}

func TestSelectCTraderAccount(t *testing.T) {
	demo := cTraderAccount{CtidTraderAccountID: 1, IsLive: false}
	liveA := cTraderAccount{CtidTraderAccountID: 2, IsLive: true}
	liveB := cTraderAccount{CtidTraderAccountID: 3, IsLive: true}

	t.Run("pinned account wins over the live fallback", func(t *testing.T) {
		got, unpinned, err := selectCTraderAccount([]cTraderAccount{liveA, demo, liveB}, 3)
		if err != nil || got != liveB || unpinned {
			t.Fatalf("got %+v unpinned=%v err=%v, want the pinned account", got, unpinned, err)
		}
	})

	// Falling back to another account would splice its balance into this
	// connection's history.
	t.Run("pinned account no longer granted is refused as a re-authorization", func(t *testing.T) {
		_, _, err := selectCTraderAccount([]cTraderAccount{liveA, liveB}, 9)
		if err == nil {
			t.Fatal("a connection synced an account other than the one it was created for")
		}
		if got := errsanitize.Category(err.Error()); got != errsanitize.MsgBrokerReauthRequired {
			t.Fatalf("category %q, want the re-authorization message", got)
		}
	})

	t.Run("several live accounts and no pin is flagged", func(t *testing.T) {
		got, unpinned, err := selectCTraderAccount([]cTraderAccount{demo, liveA, liveB}, 0)
		if err != nil || got != liveA || !unpinned {
			t.Fatalf("got %+v unpinned=%v err=%v, want the first live account, flagged", got, unpinned, err)
		}
	})

	t.Run("one live account is not ambiguous", func(t *testing.T) {
		got, unpinned, err := selectCTraderAccount([]cTraderAccount{demo, liveA}, 0)
		if err != nil || got != liveA || unpinned {
			t.Fatalf("got %+v unpinned=%v err=%v", got, unpinned, err)
		}
	})

	t.Run("demo only falls back to the first account", func(t *testing.T) {
		got, unpinned, err := selectCTraderAccount([]cTraderAccount{demo}, 0)
		if err != nil || got != demo || unpinned {
			t.Fatalf("got %+v unpinned=%v err=%v", got, unpinned, err)
		}
	})

	t.Run("no account at all", func(t *testing.T) {
		_, _, err := selectCTraderAccount(nil, 0)
		if err == nil || !strings.Contains(err.Error(), "no cTrader accounts found") {
			t.Fatalf("err = %v, want the no-account error", err)
		}
	})
}
