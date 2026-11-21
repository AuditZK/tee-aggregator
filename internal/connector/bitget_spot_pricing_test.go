package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// CONN-12 phase 0 on the connector listed as fixed: when the all-tickers
// fetch failed while non-stable coins were held, bitget silently degraded to
// a stables-only equity — the original bug replayed intermittently. It must
// fail the sync as transient instead.
func TestBitgetGetBalance_PriceMapDownFailsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v2/spot/account/assets":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": "00000",
				"data": []map[string]string{
					{"coin": "USDT", "available": "100", "frozen": "0"},
					{"coin": "BTC", "available": "1.5", "frozen": "0"},
				},
			})
		case "/api/v2/spot/market/tickers":
			http.Error(w, "down", http.StatusServiceUnavailable)
		default:
			// Futures legs are best-effort in GetBalance; empty is fine.
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": "00000", "data": []interface{}{}})
		}
	}))
	defer srv.Close()

	b := NewBitget(&Credentials{APIKey: "k", APISecret: "cw==", Passphrase: "p"})
	b.base.BaseURL = srv.URL

	_, err := b.GetBalance(context.Background())
	if err == nil {
		t.Fatal("want an error when BTC is held and the ticker map is down — not a stables-only equity")
	}
	if !errors.Is(err, ErrSpotPricingUnavailable) {
		t.Errorf("error must wrap ErrSpotPricingUnavailable, got: %v", err)
	}
	if !errors.Is(err, ErrTransient) {
		t.Errorf("error must stay transient, got: %v", err)
	}
}

// A pure-stable wallet must keep syncing when the ticker endpoint is down —
// no price is needed to value USDT.
func TestBitgetGetBalance_StableOnlyUnaffectedByTickerOutage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v2/spot/account/assets":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"code": "00000",
				"data": []map[string]string{{"coin": "USDT", "available": "100", "frozen": "0"}},
			})
		case "/api/v2/spot/market/tickers":
			http.Error(w, "down", http.StatusServiceUnavailable)
		default:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": "00000", "data": []interface{}{}})
		}
	}))
	defer srv.Close()

	b := NewBitget(&Credentials{APIKey: "k", APISecret: "cw==", Passphrase: "p"})
	b.base.BaseURL = srv.URL

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance must not need tickers for a stable-only wallet: %v", err)
	}
	if bal.Equity != 100 {
		t.Errorf("equity = %v, want 100", bal.Equity)
	}
}
