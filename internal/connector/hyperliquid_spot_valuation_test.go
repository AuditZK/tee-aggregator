package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

// CONN-12, hyperliquid half: GetBalance filtered spot to USDC/USDT and
// dropped every token (a HYPE holder read as poorer than reality), while
// GetBalanceByMarket summed raw token QUANTITIES with no price (1 HYPE
// counted as 1 USD) — two different wrong answers for the same wallet.
// Both now go through valueSpotBalancesUSD.

const hlPerpsFixture = `{"marginSummary":{"accountValue":"1000","totalMarginUsed":"200"},` +
	`"assetPositions":[{"position":{"unrealizedPnl":"50"}}]}`

// Spot meta: USDC index 0, HYPE index 150, PURR index 1, DUSTY index 7.
// Universe: PURR/USDC mid 10, HYPE/USDC mid 40. DUSTY has no USDC pair.
const hlSpotMetaFixture = `[` +
	`{"tokens":[{"name":"USDC","index":0},{"name":"PURR","index":1},{"name":"DUSTY","index":7},{"name":"HYPE","index":150}],` +
	`"universe":[{"name":"PURR/USDC","tokens":[1,0]},{"name":"@1","tokens":[150,0]}]},` +
	`[{"midPx":"10","markPx":"9.9"},{"midPx":"40","markPx":"39.8"}]` +
	`]`

func newHLTestServer(t *testing.T, spot string, metaCalls *int64, metaStatus int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Type {
		case "clearinghouseState":
			_, _ = io.WriteString(w, hlPerpsFixture)
		case "spotClearinghouseState":
			_, _ = io.WriteString(w, spot)
		case "spotMetaAndAssetCtxs":
			if metaCalls != nil {
				atomic.AddInt64(metaCalls, 1)
			}
			if metaStatus != 0 {
				http.Error(w, "down", metaStatus)
				return
			}
			_, _ = io.WriteString(w, hlSpotMetaFixture)
		default:
			http.Error(w, "unexpected info type", http.StatusBadRequest)
		}
	}))
}

func newHLAgainst(t *testing.T, srv *httptest.Server) *Hyperliquid {
	t.Helper()
	srvURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	h := NewHyperliquid(&Credentials{WalletAddress: "0x000000000000000000000000000000000000dead"})
	h.client = &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: srvURL}}
	return h
}

func TestHyperliquidGetBalance_ValuesSpotTokens(t *testing.T) {
	// 300 USDC (50 held as perp collateral) + 2 HYPE @40 + 5 PURR @10.
	spot := `{"balances":[` +
		`{"coin":"USDC","total":"300","hold":"50"},` +
		`{"coin":"HYPE","total":"2","hold":"0"},` +
		`{"coin":"PURR","total":"5"}]}`
	srv := newHLTestServer(t, spot, nil, 0)
	defer srv.Close()
	h := newHLAgainst(t, srv)

	bal, err := h.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// 1000 perps + (300−50) USDC free + 2×40 + 5×10 = 1380.
	if bal.Equity != 1380 {
		t.Errorf("equity = %v, want 1380 — tokens must be valued, not dropped", bal.Equity)
	}
}

func TestHyperliquidGetBalance_HeldTokenStillCountsInFull(t *testing.T) {
	// A token sitting in an open spot order (hold > 0) is still owned, and
	// unlike USDC it is never part of accountValue — full amount counts.
	spot := `{"balances":[{"coin":"HYPE","total":"2","hold":"2"}]}`
	srv := newHLTestServer(t, spot, nil, 0)
	defer srv.Close()
	h := newHLAgainst(t, srv)

	bal, err := h.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 1080 { // 1000 + 2×40
		t.Errorf("equity = %v, want 1080 — hold must not shrink token holdings", bal.Equity)
	}
}

func TestHyperliquidGetBalance_StableOnlySkipsPriceFetch(t *testing.T) {
	var metaCalls int64
	spot := `{"balances":[{"coin":"USDC","total":"300","hold":"250"}]}`
	srv := newHLTestServer(t, spot, &metaCalls, 0)
	defer srv.Close()
	h := newHLAgainst(t, srv)

	bal, err := h.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 1050 { // 1000 + (300−250): the 2026-05-24 hold guard, unchanged
		t.Errorf("equity = %v, want 1050", bal.Equity)
	}
	if metaCalls != 0 {
		t.Errorf("spotMetaAndAssetCtxs called %d times for a stable-only wallet, want 0", metaCalls)
	}
}

func TestHyperliquidGetBalance_PriceMapDownFailsTransient(t *testing.T) {
	spot := `{"balances":[{"coin":"HYPE","total":"2"}]}`
	srv := newHLTestServer(t, spot, nil, http.StatusServiceUnavailable)
	defer srv.Close()
	h := newHLAgainst(t, srv)

	_, err := h.GetBalance(context.Background())
	if err == nil {
		t.Fatal("want an error when tokens are held and the price map is down — not a stables-only equity")
	}
	if !errors.Is(err, ErrSpotPricingUnavailable) {
		t.Errorf("error must wrap ErrSpotPricingUnavailable, got: %v", err)
	}
	if !errors.Is(err, ErrTransient) {
		t.Errorf("error must stay transient (retry, not credential failure), got: %v", err)
	}
}

func TestHyperliquidGetBalance_UnpricedTokenContributesZero(t *testing.T) {
	// DUSTY has no USDC pair in the fixture: per-asset tolerance, 0 not error.
	spot := `{"balances":[{"coin":"USDC","total":"100"},{"coin":"DUSTY","total":"9999"}]}`
	srv := newHLTestServer(t, spot, nil, 0)
	defer srv.Close()
	h := newHLAgainst(t, srv)

	bal, err := h.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 1100 { // 1000 + 100; DUSTY contributes 0
		t.Errorf("equity = %v, want 1100", bal.Equity)
	}
}

func TestHyperliquidSpotBucket_MatchesEquitySpotLeg(t *testing.T) {
	// The divergence CONN-12 documented: the two reads of the same wallet
	// returned different spot figures. They must now be equal.
	spot := `{"balances":[` +
		`{"coin":"USDC","total":"300","hold":"50"},` +
		`{"coin":"HYPE","total":"2"},` +
		`{"coin":"PURR","total":"5"}]}`
	srv := newHLTestServer(t, spot, nil, 0)
	defer srv.Close()
	h := newHLAgainst(t, srv)

	bal, err := h.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	perp := 1000.0
	spotLegFromEquity := bal.Equity - perp

	markets, err := h.GetBalanceByMarket(context.Background())
	if err != nil {
		t.Fatalf("GetBalanceByMarket: %v", err)
	}
	var spotBucket float64
	for _, m := range markets {
		if m.MarketType == MarketSpot {
			spotBucket = m.Equity
		}
	}
	if spotBucket != spotLegFromEquity {
		t.Errorf("spot bucket %v != equity spot leg %v — the two reads disagree again", spotBucket, spotLegFromEquity)
	}
	if spotBucket != 380 { // (300−50) + 2×40 + 5×10
		t.Errorf("spot bucket = %v, want 380", spotBucket)
	}
}
