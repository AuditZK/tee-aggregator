package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func newBinanceMarketTestServer(t *testing.T, reqCount *atomic.Int32, futuresMode string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path
		switch {
		case strings.Contains(p, "/api/v3/ticker"):
			_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT","price":"121000"}]`))
		case strings.Contains(p, "/api/v3/account"):
			_, _ = w.Write([]byte(`{"balances":[{"asset":"USDT","free":"1000","locked":"0"}]}`))
		case strings.Contains(p, "/fapi/v2/account"):
			switch futuresMode {
			case "empty":
				_, _ = w.Write([]byte(`{"totalMarginBalance":"0","totalUnrealizedProfit":"0","availableBalance":"0"}`))
			case "denied":
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`))
			default:
				_, _ = w.Write([]byte(`{"totalMarginBalance":"16000","totalUnrealizedProfit":"-200","availableBalance":"14400"}`))
			}
		case strings.Contains(p, "/fapi/v2/balance"):
			if futuresMode == "denied" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"code":-2015,"msg":"Invalid API-key"}`))
				return
			}
			_, _ = w.Write([]byte(`[{"asset":"USDT","balance":"16100","crossUnPnl":"-100","availableBalance":"14500"}]`))
		case strings.Contains(p, "/sapi/v1/margin/account"):
			// Cross margin: 2 BTC net × $121k = $242,000 — must land in the SPOT
			// bucket to match the rebuilder's two-bucket seam convention.
			_, _ = w.Write([]byte(`{"totalNetAssetOfBtc":"2"}`))
		default:
			// COIN-M, isolated margin, anything else: empty.
			_, _ = w.Write([]byte(`{}`))
		}
	}))
}

func marketMap(t *testing.T, balances []*MarketBalance) map[string]*MarketBalance {
	t.Helper()
	m := map[string]*MarketBalance{}
	for _, mb := range balances {
		m[mb.MarketType] = mb
	}
	return m
}

// The live sync calls GetBalance then GetBalanceByMarket. The split must
// mirror the history rebuilder's two-bucket convention — spot carries
// everything non-UM (incl. cross margin) with available=equity, swap carries
// UM futures with the true free margin — and the second call must not hit the
// API again (the midnight herd already rate-limits fapi).
func TestBinance_GetBalanceByMarket_SplitAndNoExtraCalls(t *testing.T) {
	var reqs atomic.Int32
	srv := newBinanceMarketTestServer(t, &reqs, "ok")
	defer srv.Close()

	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 1000+16000+242000 {
		t.Fatalf("total equity: got %v, want 259000", bal.Equity)
	}

	before := reqs.Load()
	mbs, err := b.GetBalanceByMarket(context.Background())
	if err != nil {
		t.Fatalf("GetBalanceByMarket: %v", err)
	}
	if got := reqs.Load(); got != before {
		t.Fatalf("GetBalanceByMarket made %d extra API calls, want 0", got-before)
	}

	// balance.Available is persisted as breakdown.global.available_margin —
	// the field the dashboard's free-margin line reads. It must mirror the
	// buckets, not the spot wallet's loose stablecoins (0 here).
	if want := (1000 + 242000) + 14400.0; bal.Available != want {
		t.Fatalf("balance.Available: got %v, want %v (spot bucket + swap free margin)", bal.Available, want)
	}

	m := marketMap(t, mbs)
	spot, swap := m[MarketSpot], m[MarketSwap]
	if spot == nil || swap == nil {
		t.Fatalf("want spot+swap buckets, got %+v", mbs)
	}
	if bal.Available != spot.AvailableMargin+swap.AvailableMargin {
		t.Fatalf("global available (%v) must equal spot+swap available (%v)",
			bal.Available, spot.AvailableMargin+swap.AvailableMargin)
	}
	// Seam convention: cross margin folds into spot, available = equity.
	if spot.Equity != 1000+242000 || spot.AvailableMargin != spot.Equity {
		t.Fatalf("spot bucket: got eq=%v avail=%v, want eq=243000 avail=eq", spot.Equity, spot.AvailableMargin)
	}
	if swap.Equity != 16000 || swap.AvailableMargin != 14400 {
		t.Fatalf("swap bucket: got eq=%v avail=%v, want eq=16000 avail=14400", swap.Equity, swap.AvailableMargin)
	}
	if _, hasMargin := m[MarketMargin]; hasMargin {
		t.Fatal("cross margin must fold into spot (rebuilder seam), not a margin bucket")
	}
}

// When /fapi/v2/account 200-empties, the /fapi/v2/balance fallback must still
// feed the swap bucket — equity AND free margin.
func TestBinance_GetBalanceByMarket_FallbackKeepsSwapMargin(t *testing.T) {
	var reqs atomic.Int32
	srv := newBinanceMarketTestServer(t, &reqs, "empty")
	defer srv.Close()

	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	m := marketMap(t, mustMarket(t, b))
	swap := m[MarketSwap]
	if swap == nil || swap.Equity != 16000 || swap.AvailableMargin != 14500 {
		t.Fatalf("swap via fallback: got %+v, want eq=16000 avail=14500", swap)
	}
}

// A key without futures permission yields a spot-only split — no zero-value
// swap bucket that would draw a bogus flat margin line.
func TestBinance_GetBalanceByMarket_PermissionDeniedSpotOnly(t *testing.T) {
	var reqs atomic.Int32
	srv := newBinanceMarketTestServer(t, &reqs, "denied")
	defer srv.Close()

	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	m := marketMap(t, mustMarket(t, b))
	if m[MarketSwap] != nil {
		t.Fatalf("want no swap bucket without futures permission, got %+v", m[MarketSwap])
	}
	if spot := m[MarketSpot]; spot == nil || spot.Equity != 1000+242000 {
		t.Fatalf("spot bucket: got %+v, want eq=243000", spot)
	}
}

func mustMarket(t *testing.T, b *Binance) []*MarketBalance {
	t.Helper()
	mbs, err := b.GetBalanceByMarket(context.Background())
	if err != nil {
		t.Fatalf("GetBalanceByMarket: %v", err)
	}
	return mbs
}

// Compile-time guard: the live sync only emits the per-market margin split for
// connectors implementing this — if Binance drops off it, live days silently
// lose the free-margin curve again (only rebuilds would carry it).
var _ BalanceByMarketFetcher = (*Binance)(nil)
