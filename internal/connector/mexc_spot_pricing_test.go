package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func mexcTestServer(t *testing.T, tickersDown bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case mexcPathAccount:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"balances": []map[string]string{
					{"asset": "USDT", "free": "100", "locked": "0"},
					{"asset": "BTC", "free": "1.5", "locked": "0"},
				},
			})
		case "/api/v3/ticker/price":
			if tickersDown {
				http.Error(w, "down", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]string{
				{"symbol": "BTCUSDT", "price": "60000"},
			})
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
}

// CONN-13: MEXC was the last connector valuing spot outside the fail-closed
// policy — a 429 on the per-altcoin ticker made the asset vanish from the
// equity with no error, and the resulting snapshot entered the signed report.
func TestMEXCGetBalance_PriceMapDownFailsTransient(t *testing.T) {
	srv := mexcTestServer(t, true)
	defer srv.Close()

	m := NewMEXC(&Credentials{APIKey: "k", APISecret: "s"})
	m.base.BaseURL = srv.URL

	if _, err := m.GetBalance(context.Background()); err == nil {
		t.Fatal("want an error when BTC is held and the ticker map is down — not a stables-only equity")
	} else {
		if !errors.Is(err, ErrSpotPricingUnavailable) {
			t.Errorf("error must wrap ErrSpotPricingUnavailable, got: %v", err)
		}
		if !errors.Is(err, ErrTransient) {
			t.Errorf("error must stay transient, got: %v", err)
		}
	}
}

func TestMEXCGetBalance_ValuesHoldingsFromPriceMap(t *testing.T) {
	srv := mexcTestServer(t, false)
	defer srv.Close()

	m := NewMEXC(&Credentials{APIKey: "k", APISecret: "s"})
	m.base.BaseURL = srv.URL

	bal, err := m.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	const wantEquity = 100 + 1.5*60000
	if bal.Equity != wantEquity {
		t.Errorf("equity = %v, want %v", bal.Equity, wantEquity)
	}
	if bal.Available != wantEquity {
		t.Errorf("available = %v, want %v (nothing is locked)", bal.Available, wantEquity)
	}
}

// CONN-13 constat B: the consumer sums Amount and ignores Currency, so a coin
// quantity booked raw made a 0.5 BTC deposit worth $0.50.
func TestMEXCGetCashflows_ConvertsToUSD(t *testing.T) {
	insert := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sapi/v1/capital/deposit/hisrec":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"amount": "0.5", "coin": "BTC", "insertTime": insert.UnixMilli(), "status": 1},
			})
		case "/sapi/v1/capital/withdraw/history":
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"amount": "250", "coin": "USDT", "applyTime": "2026-08-02 09:00:00", "status": 6},
			})
		case "/api/v3/ticker/price":
			_ = json.NewEncoder(w).Encode([]map[string]string{{"symbol": "BTCUSDT", "price": "60000"}})
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	m := NewMEXC(&Credentials{APIKey: "k", APISecret: "s"})
	m.base.BaseURL = srv.URL

	flows, err := m.GetCashflows(context.Background(), insert.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 2 {
		t.Fatalf("got %d flows, want 2: %+v", len(flows), flows)
	}
	for _, f := range flows {
		if f.Currency != "USDT" {
			t.Errorf("currency = %q, want USDT (the consumer ignores this field)", f.Currency)
		}
	}
	if flows[0].Amount != 30000 {
		t.Errorf("deposit = %v, want 30000 (0.5 BTC at 60000)", flows[0].Amount)
	}
	if flows[1].Amount != -250 {
		t.Errorf("withdrawal = %v, want -250", flows[1].Amount)
	}
}
