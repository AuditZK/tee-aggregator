package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func alpacaProbeServer(t *testing.T, account, positions string) *Alpaca {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/account":
			_, _ = w.Write([]byte(account))
		case "/v2/positions":
			_, _ = w.Write([]byte(positions))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return &Alpaca{apiKey: "k", apiSecret: "s", baseURL: srv.URL, client: srv.Client()}
}

// A flat equity has two explanations that look identical from the stored
// series: an account holding nothing, and a sync that stopped asking. The
// probe separates them by showing the venue's own answer next to the derived
// one, in a single round trip.
func TestAlpacaProbeBalance_CashOnlyAccountSaysSo(t *testing.T) {
	a := alpacaProbeServer(t,
		`{"account_number":"PA123","status":"ACTIVE","currency":"USD","cash":"161129.27",
		  "equity":"161129.27","last_equity":"161129.27","buying_power":"322258.54",
		  "long_market_value":"0","short_market_value":"0","position_market_value":"0"}`,
		`[]`)

	p, err := a.ProbeBalance(context.Background())
	if err != nil {
		t.Fatalf("ProbeBalance: %v", err)
	}
	if p.Account["equity"] != "161129.27" || p.Account["long_market_value"] != "0" {
		t.Fatalf("account payload not verbatim: %v", p.Account)
	}
	if p.Derived.Equity != 161129.27 || p.Derived.Available != 161129.27 {
		t.Fatalf("derived = %+v", p.Derived)
	}
	if p.Derived.UnrealizedPnL != 0 {
		t.Fatalf("unrealized = %v, want 0 with no position", p.Derived.UnrealizedPnL)
	}
	if len(p.Currencies) != 0 {
		t.Fatalf("positions = %v, want none", p.Currencies)
	}
	found := false
	for _, n := range p.Notes {
		if n == "no open position: the account is entirely in cash" {
			found = true
		}
	}
	if !found {
		t.Fatalf("notes = %v, want the cash-only statement", p.Notes)
	}
}

// Alpaca publishes no aggregate unrealized field, so that number is summed
// from the positions. The probe must show the lines it was summed from, or it
// asks to be trusted on the one figure that cannot be checked elsewhere.
func TestAlpacaProbeBalance_UnrealizedIsShownWithItsPositions(t *testing.T) {
	a := alpacaProbeServer(t,
		`{"status":"ACTIVE","currency":"USD","cash":"1000","equity":"5200"}`,
		`[{"symbol":"AAPL","qty":"10","avg_entry_price":"400","current_price":"420","unrealized_pl":"200","asset_class":"us_equity"}]`)

	p, err := a.ProbeBalance(context.Background())
	if err != nil {
		t.Fatalf("ProbeBalance: %v", err)
	}
	if p.Derived.UnrealizedPnL != 200 {
		t.Fatalf("unrealized = %v, want 200", p.Derived.UnrealizedPnL)
	}
	if len(p.Currencies) != 1 || p.Currencies[0]["symbol"] != "AAPL" {
		t.Fatalf("positions = %v, want the AAPL line", p.Currencies)
	}
	if p.Currencies[0]["unrealized_pl"] != "200" {
		t.Fatalf("position line = %v", p.Currencies[0])
	}
}

func TestAlpaca_ImplementsBalanceProber(t *testing.T) {
	var _ BalanceProber = (*Alpaca)(nil)
}
