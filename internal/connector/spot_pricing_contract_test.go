package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The contract every connector that values spot holdings must keep: when the
// price source is down, GetBalance fails with ErrSpotPricingUnavailable. The
// alternative, valuing the holdings it could not price at zero, persists an
// equity that looks measured and is not (CONN-12, CONN-13).
func TestSpotPricingOutage_FailsTheBalance(t *testing.T) {
	down := func(status int) func(w http.ResponseWriter) {
		return func(w http.ResponseWriter) { http.Error(w, "price source down", status) }
	}
	body := func(s string) func(w http.ResponseWriter) {
		return func(w http.ResponseWriter) { _, _ = w.Write([]byte(s)) }
	}

	cases := []struct {
		name    string
		routes  map[string]func(w http.ResponseWriter)
		connect func(t *testing.T, srv *httptest.Server) Connector
	}{
		{
			name: "binance",
			routes: map[string]func(w http.ResponseWriter){
				"/api/v3/ticker/price": down(http.StatusUnavailableForLegalReasons),
			},
			connect: func(t *testing.T, srv *httptest.Server) Connector {
				client := &http.Client{Transport: hostRewriterFor(t, srv)}
				return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)
			},
		},
		{
			name: "bingx",
			routes: map[string]func(w http.ResponseWriter){
				"/openApi/swap/v2/user/balance":    body(`{"code":0,"data":{"balance":{"balance":"0","equity":"0","unrealizedProfit":"0","availableMargin":"0"}}}`),
				"/openApi/spot/v1/account/balance": body(`{"data":{"balances":[{"asset":"BTC","free":"1","locked":"0"}]}}`),
				"/openApi/spot/v1/ticker/24hr":     down(http.StatusForbidden),
			},
			connect: func(_ *testing.T, srv *httptest.Server) Connector {
				c := NewBingX(&Credentials{APIKey: "k", APISecret: "s"})
				c.base.BaseURL = srv.URL
				return c
			},
		},
		{
			name: "coinbase",
			routes: map[string]func(w http.ResponseWriter){
				"/v2/accounts":       body(`{"data":[{"balance":{"amount":"2","currency":"BTC"},"currency":{"code":"BTC"}}]}`),
				"/v2/exchange-rates": down(http.StatusForbidden),
			},
			connect: func(_ *testing.T, srv *httptest.Server) Connector {
				c := NewCoinbase(&Credentials{APIKey: "k", APISecret: "s"})
				c.base.BaseURL = srv.URL
				return c
			},
		},
		{
			name: "gate",
			routes: map[string]func(w http.ResponseWriter){
				"/api/v4/spot/accounts": body(`[{"currency":"BTC","available":"1","locked":"0"}]`),
				"/api/v4/spot/tickers":  down(http.StatusForbidden),
			},
			connect: func(_ *testing.T, srv *httptest.Server) Connector {
				c := NewGate(&Credentials{APIKey: "k", APISecret: "s"})
				c.base.BaseURL = srv.URL
				return c
			},
		},
		{
			name: "huobi",
			routes: map[string]func(w http.ResponseWriter){
				"/v1/account/accounts":           body(`{"data":[{"id":1,"type":"spot","state":"working"}]}`),
				"/v1/account/accounts/1/balance": body(`{"data":{"list":[{"currency":"btc","type":"trade","balance":"1"}]}}`),
				"/market/tickers":                down(http.StatusForbidden),
			},
			connect: func(_ *testing.T, srv *httptest.Server) Connector {
				c := NewHuobi(&Credentials{APIKey: "k", APISecret: "s"})
				c.base.BaseURL = srv.URL
				return c
			},
		},
		{
			name: "kucoin",
			routes: map[string]func(w http.ResponseWriter){
				"/api/v1/accounts":          body(`{"code":"200000","data":[{"currency":"BTC","type":"trade","balance":"1","available":"1","holds":"0"}]}`),
				"/api/v1/market/allTickers": down(http.StatusForbidden),
			},
			connect: func(_ *testing.T, srv *httptest.Server) Connector {
				c := NewKuCoin(&Credentials{APIKey: "k", APISecret: "s", Passphrase: "p"})
				c.base.BaseURL = srv.URL
				return c
			},
		},
		{
			name: "kraken",
			routes: map[string]func(w http.ResponseWriter){
				"/0/private/Balance": body(`{"error":[],"result":{"XXBT":"1.0"}}`),
				"/0/public/Ticker":   down(http.StatusForbidden),
			},
			connect: func(t *testing.T, srv *httptest.Server) Connector {
				c := NewKraken(&Credentials{APIKey: "k", APISecret: "c2VjcmV0"})
				c.client = &http.Client{Transport: hostRewriterFor(t, srv)}
				return c
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := serve(t, tc.routes)
			defer srv.Close()

			bal, err := tc.connect(t, srv).GetBalance(context.Background())
			if err == nil {
				t.Fatalf("price source down, yet GetBalance returned equity %v: holdings it could not price were valued at zero", bal.Equity)
			}
			if !errors.Is(err, ErrSpotPricingUnavailable) {
				t.Errorf("error must be ErrSpotPricingUnavailable so the sync fails instead of persisting, got: %v", err)
			}
		})
	}
}

// Where the outage case of each spot-valuing connector is pinned. A connector
// that starts valuing holdings without an entry here fails the scan below, so
// the next CONN-12-style omission is caught at review, not in production.
var spotPricingOutagePinnedBy = map[string]string{
	"binance":     "TestSpotPricingOutage_FailsTheBalance",
	"bingx":       "TestSpotPricingOutage_FailsTheBalance",
	"coinbase":    "TestSpotPricingOutage_FailsTheBalance",
	"gate":        "TestSpotPricingOutage_FailsTheBalance",
	"huobi":       "TestSpotPricingOutage_FailsTheBalance",
	"kucoin":      "TestSpotPricingOutage_FailsTheBalance",
	"kraken":      "TestSpotPricingOutage_FailsTheBalance",
	"bitget":      "TestBitgetGetBalance_PriceMapDownFailsTransient",
	"hyperliquid": "TestHyperliquidGetBalance_PriceMapDownFailsTransient",
	"mexc":        "TestMEXCGetBalance_PriceMapDownFailsTransient",
	"deribit":     "TestDeribitGetBalance_TickerDownFailsInsteadOfZeroing",
	// Bybit prices cashflows only; its balance arrives in USD from the venue.
	"bybit": "",
}

func TestSpotPricingOutage_EveryValuingConnectorIsPinned(t *testing.T) {
	valuation := regexp.MustCompile(`ValueSpotHoldingsUSD\(|FetchBinanceStylePriceMap\(|\) fetchPriceMap\(|\) fetchSpotPriceMap\(`)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list connector files: %v", err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || f == "base_crypto.go" {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if !valuation.Match(src) {
			continue
		}
		venue := strings.SplitN(strings.TrimSuffix(f, ".go"), "_", 2)[0]
		if _, ok := spotPricingOutagePinnedBy[venue]; !ok {
			t.Errorf("%s values spot holdings but %q has no pricing-outage case: add one to TestSpotPricingOutage_FailsTheBalance", f, venue)
		}
	}
}
