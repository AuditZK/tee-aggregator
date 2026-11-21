package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// A fake Kraken: a wallet of 1000 USD + 0.5 BTC (BTC at 100k → spot 51,000),
// and a TradeBalance answer the test controls.
func fakeKraken(t *testing.T, tradeBalance func(w http.ResponseWriter)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/0/private/Balance":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error":  []string{},
				"result": map[string]string{"ZUSD": "1000.0000", "XXBT": "0.5000000000"},
			})
		case "/0/public/Ticker":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": []string{},
				"result": map[string]interface{}{
					"XXBTZUSD": map[string]interface{}{"c": []string{"100000.0", "1.0"}},
				},
			})
		case "/0/private/TradeBalance":
			tradeBalance(w)
		default:
			http.NotFound(w, r)
		}
	}))
}

func newTestKraken(srvURL string) *Kraken {
	k := NewKraken(&Credentials{APIKey: "k", APISecret: "cw=="})
	k.baseURL = srvURL
	return k
}

// A margin trader: 250 USD of unrealized P&L on open positions locking 5,000
// of margin. Equity must carry the P&L, free margin must exclude what the
// positions lock. Before this, both read the bare spot value.
func TestKrakenGetBalance_MarginPositionsShapeEquityAndFreeMargin(t *testing.T) {
	srv := fakeKraken(t, func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": []string{},
			"result": map[string]string{
				"eb": "51000.0000", "tb": "51000.0000", "m": "5000.0000", "n": "250.0000",
				"c": "20000.0000", "v": "20250.0000", "e": "51250.0000", "mf": "46250.0000", "ml": "1025.00",
			},
		})
	})
	defer srv.Close()

	b, err := newTestKraken(srv.URL).GetBalance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Equity != 51250 {
		t.Errorf("equity = %v, want 51250 (spot 51000 + unrealized 250)", b.Equity)
	}
	if b.Available != 46000 {
		t.Errorf("available = %v, want 46000 (spot 51000 − margin used 5000)", b.Available)
	}
	if b.UnrealizedPnL != 250 {
		t.Errorf("unrealized = %v, want 250", b.UnrealizedPnL)
	}
}

// Margin disabled, or TradeBalance refused: the spot figures stand and the
// sync does not fail. A pure spot account must read exactly what it did
// before this change.
func TestKrakenGetBalance_TradeBalanceUnavailableKeepsSpotFigures(t *testing.T) {
	srv := fakeKraken(t, func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": []string{"EGeneral:Permission denied"},
		})
	})
	defer srv.Close()

	b, err := newTestKraken(srv.URL).GetBalance(context.Background())
	if err != nil {
		t.Fatalf("a refused TradeBalance must not fail the sync: %v", err)
	}
	if b.Equity != 51000 || b.Available != 51000 || b.UnrealizedPnL != 0 {
		t.Errorf("got equity=%v available=%v unrealized=%v, want 51000/51000/0", b.Equity, b.Available, b.UnrealizedPnL)
	}
}

// Kraken lists CHF only as USDCHF. The XBTCHF trader who was the first real
// Kraken account (2026-08-28) holds CHF whenever out of BTC; that balance was
// unpriceable and valued at zero.
func TestKrakenGetBalance_PricesUSDBaseFiatByInversion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/0/private/Balance":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error":  []string{},
				"result": map[string]string{"CHF": "8000.0000", "ZUSD": "100.0000"},
			})
		case "/0/public/Ticker":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": []string{},
				"result": map[string]interface{}{
					"USDCHF":   map[string]interface{}{"c": []string{"0.80000", "1.0"}},
					"XXBTZUSD": map[string]interface{}{"c": []string{"100000.0", "1.0"}},
				},
			})
		case "/0/private/TradeBalance":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"error": []string{"EGeneral:Permission denied"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	b, err := newTestKraken(srv.URL).GetBalance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 8000 CHF / 0.80 = 10,000 USD, plus 100 USD.
	if b.Equity < 10099.99 || b.Equity > 10100.01 {
		t.Errorf("equity = %v, want 10100 (CHF priced by inverting USDCHF)", b.Equity)
	}
}
