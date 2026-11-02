package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
)

func newBinanceEarnTestServer(t *testing.T, earn func(w http.ResponseWriter, r *http.Request)) *Binance {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path
		switch {
		case strings.Contains(p, "/api/v3/ticker"):
			_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT","price":"100000"},{"symbol":"ZECUSDT","price":"50"},{"symbol":"LDOUSDT","price":"2"}]`))
		case strings.Contains(p, "/api/v3/account"):
			// The flexible USDT position also shows in spot as LDUSDT; LDO is a coin of its own.
			_, _ = w.Write([]byte(`{"balances":[{"asset":"USDT","free":"10","locked":"0"},{"asset":"LDUSDT","free":"1500","locked":"0"},{"asset":"LDO","free":"4","locked":"0"}]}`))
		case strings.Contains(p, "/sapi/v1/simple-earn/"):
			earn(w, r)
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)
}

// An account that parks its capital in Simple Earn read as empty: the
// positions are part of the equity, each valued as its own asset, and land in
// the spot bucket with the rest of the non-futures wallets.
func TestBinance_SimpleEarnCountsInEquity(t *testing.T) {
	b := newBinanceEarnTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/flexible/") {
			_, _ = w.Write([]byte(`{"rows":[{"asset":"USDT","totalAmount":"1500"}],"total":1}`))
			return
		}
		_, _ = w.Write([]byte(`{"rows":[{"asset":"ZEC","amount":"2"}],"total":1}`))
	})
	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// spot USDT + LDO, Earn USDT once (not again as spot LDUSDT), locked ZEC
	if bal.Equity != 10+4*2+1500+2*50 {
		t.Fatalf("equity %v, want 1618", bal.Equity)
	}
	byMarket, err := b.GetBalanceByMarket(context.Background())
	if err != nil {
		t.Fatalf("GetBalanceByMarket: %v", err)
	}
	if m := marketMap(t, byMarket); m[MarketSpot] == nil || m[MarketSpot].Equity != 1618 {
		t.Fatalf("spot bucket %+v, want 1618", m[MarketSpot])
	}
}

// Every page of positions is read.
func TestBinance_SimpleEarnFollowsPages(t *testing.T) {
	b := newBinanceEarnTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/flexible/") {
			_, _ = w.Write([]byte(`{"rows":[],"total":0}`))
			return
		}
		row := `{"asset":"USDT","totalAmount":"1"}`
		n := binanceEarnPageSize
		if r.URL.Query().Get("current") == "2" {
			n = 5
		}
		_, _ = w.Write([]byte(`{"rows":[` + strings.TrimSuffix(strings.Repeat(row+",", n), ",") + `],"total":105}`))
	})
	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 10+4*2+105 {
		t.Fatalf("equity %v, want 123", bal.Equity)
	}
}

// A key refused on Simple Earn keeps the sync going on what it covers, and
// says so.
func TestBinance_SimpleEarnRefusedIsAWarning(t *testing.T) {
	b := newBinanceEarnTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`))
	})
	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// Earn unread: spot is all there is, its LDUSDT line valued as spot values it (no price)
	if bal.Equity != 10+4*2 {
		t.Fatalf("equity %v, want the spot 18", bal.Equity)
	}
	if !slices.Contains(b.CapabilityWarnings(), "simple_earn_unreadable") {
		t.Fatalf("warnings %v, want simple_earn_unreadable", b.CapabilityWarnings())
	}
}
