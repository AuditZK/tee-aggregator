package connector

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newBybitBalanceServer(t *testing.T, optionPositions string) *Bybit {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v5/account/wallet-balance":
			io.WriteString(w, `{"retCode":0,"result":{"list":[{"totalEquity":"1000","totalWalletBalance":"1030","totalPerpUPL":"20","totalAvailableBalance":"700"}]}}`)
		case "/v5/position/list":
			if r.URL.Query().Get("category") != "option" {
				t.Errorf("position category = %q, want option", r.URL.Query().Get("category"))
			}
			io.WriteString(w, optionPositions)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)
	b := NewBybitWithClient(
		&Credentials{Exchange: "bybit", APIKey: "key", APISecret: "secret"},
		&http.Client{Timeout: 5 * time.Second},
	)
	b.baseURL = srv.URL
	return b
}

func TestBybitGetBalance_UnrealizedCountsOpenOptions(t *testing.T) {
	b := newBybitBalanceServer(t, `{"retCode":0,"result":{"nextPageCursor":"","list":[{"unrealisedPnl":"-12.5"},{"unrealisedPnl":"4"}]}}`)
	bal, err := b.GetBalance(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(bal.UnrealizedPnL-11.5) > 1e-9 {
		t.Fatalf("unrealized = %v, want 20 - 12.5 + 4 = 11.5", bal.UnrealizedPnL)
	}
	if bal.Equity != 1000 || bal.Available != 700 {
		t.Fatalf("equity/available = %v/%v, want 1000/700 untouched", bal.Equity, bal.Available)
	}
}

func TestBybitGetBalance_KeepsPerpUnrealizedWhenOptionsUnreadable(t *testing.T) {
	b := newBybitBalanceServer(t, `{"retCode":10005,"retMsg":"Permission denied"}`)
	bal, err := b.GetBalance(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if bal.UnrealizedPnL != 20 {
		t.Fatalf("unrealized = %v, want the perpetual figure 20", bal.UnrealizedPnL)
	}
}
