package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// CONN-12 phase B. Every connector below summed ONLY stablecoin spot balances
// and silently dropped BTC/ETH/altcoins. The shared regression is the same
// everywhere and is the whole point of this file: a wallet holding 1 BTC and
// no stablecoin must not report an equity of zero.
//
// The ticker payload shapes below were captured from the live public
// endpoints on 2026-08-27 — including the two that send bare numbers where
// everyone else quotes them.

const btcPrice = 78744.53

func serve(t *testing.T, routes map[string]func(w http.ResponseWriter)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if h, ok := routes[r.URL.Path]; ok {
			h(w)
			return
		}
		http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
	}))
}

func TestBingXGetBalance_ValuesNonStableSpot(t *testing.T) {
	srv := serve(t, map[string]func(w http.ResponseWriter){
		"/openApi/swap/v2/user/balance": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"code":0,"data":{"balance":{"balance":"0","equity":"0","unrealizedProfit":"0","availableMargin":"0"}}}`))
		},
		"/openApi/spot/v1/account/balance": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"data":{"balances":[{"asset":"BTC","free":"1","locked":"0"}]}}`))
		},
		// lastPrice arrives as a BARE NUMBER on BingX.
		"/openApi/spot/v1/ticker/24hr": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"code":0,"data":[{"symbol":"BTC-USDT","lastPrice":78744.53}]}`))
		},
	})
	defer srv.Close()

	c := NewBingX(&Credentials{APIKey: "k", APISecret: "s"})
	c.base.BaseURL = srv.URL

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != btcPrice {
		t.Errorf("equity = %v, want %v — 1 BTC must not be worth nothing", bal.Equity, btcPrice)
	}
}

func TestCoinbaseGetBalance_ValuesNonStableSpot(t *testing.T) {
	// Coinbase has no futures leg, so spot IS the account: the old code
	// reported exactly 0 for this wallet.
	srv := serve(t, map[string]func(w http.ResponseWriter){
		"/v2/accounts": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"data":[{"balance":{"amount":"2","currency":"BTC"},"currency":{"code":"BTC"}}]}`))
		},
		// Rates are INVERTED: how much BTC one USD buys.
		"/v2/exchange-rates": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"data":{"currency":"USD","rates":{"BTC":"0.0000127"}}}`))
		},
	})
	defer srv.Close()

	c := NewCoinbase(&Credentials{APIKey: "k", APISecret: "s"})
	c.base.BaseURL = srv.URL

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	want := 2 * (1 / 0.0000127)
	if diff := bal.Equity - want; diff > 0.01 || diff < -0.01 {
		t.Errorf("equity = %v, want ~%v (rate must be inverted, not used raw)", bal.Equity, want)
	}
}

func TestGateGetBalance_ValuesNonStableSpot(t *testing.T) {
	srv := serve(t, map[string]func(w http.ResponseWriter){
		"/api/v4/spot/accounts": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`[{"currency":"BTC","available":"1","locked":"0"}]`))
		},
		// Gate returns a BARE TOP-LEVEL ARRAY, not an envelope.
		"/api/v4/spot/tickers": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`[{"currency_pair":"BTC_USDT","last":"78744.53"}]`))
		},
	})
	defer srv.Close()

	c := NewGate(&Credentials{APIKey: "k", APISecret: "s"})
	c.base.BaseURL = srv.URL

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != btcPrice {
		t.Errorf("equity = %v, want %v", bal.Equity, btcPrice)
	}
}

func TestHuobiGetBalance_ValuesNonStableSpot(t *testing.T) {
	srv := serve(t, map[string]func(w http.ResponseWriter){
		"/v1/account/accounts": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"data":[{"id":1,"type":"spot","state":"working"}]}`))
		},
		"/v1/account/accounts/1/balance": func(w http.ResponseWriter) {
			// One row per (currency, type) — trade and frozen must both count.
			_, _ = w.Write([]byte(`{"data":{"list":[{"currency":"btc","type":"trade","balance":"0.5"},{"currency":"btc","type":"frozen","balance":"0.5"}]}}`))
		},
		// close arrives as a BARE NUMBER, symbols are lowercase.
		"/market/tickers": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"data":[{"symbol":"btcusdt","close":78744.53}]}`))
		},
	})
	defer srv.Close()

	c := NewHuobi(&Credentials{APIKey: "k", APISecret: "s"})
	c.base.BaseURL = srv.URL

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != btcPrice {
		t.Errorf("equity = %v, want %v (trade + frozen rows must be summed)", bal.Equity, btcPrice)
	}
}

func TestKuCoinGetBalance_ValuesNonStableSpotAndReadsFundingAccount(t *testing.T) {
	srv := serve(t, map[string]func(w http.ResponseWriter){
		"/api/v1/accounts": func(w http.ResponseWriter) {
			// The old code queried ?type=trade only, so the `main` (funding)
			// row was invisible — even its stablecoins. `margin` must stay out:
			// counting it would double-count borrowed collateral.
			_, _ = w.Write([]byte(`{"code":"200000","data":[
				{"currency":"BTC","type":"trade","balance":"1","available":"1","holds":"0"},
				{"currency":"USDT","type":"main","balance":"500","available":"500","holds":"0"},
				{"currency":"USDT","type":"margin","balance":"9999","available":"9999","holds":"0"}
			]}`))
		},
		"/api/v1/market/allTickers": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"code":"200000","data":{"ticker":[{"symbol":"BTC-USDT","last":"78744.53"}]}}`))
		},
	})
	defer srv.Close()

	c := NewKuCoin(&Credentials{APIKey: "k", APISecret: "s", Passphrase: "p"})
	c.base.BaseURL = srv.URL

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	want := btcPrice + 500
	if bal.Equity != want {
		t.Errorf("equity = %v, want %v (funding USDT in, margin out)", bal.Equity, want)
	}
}

func TestKrakenGetBalance_ValuesNonStableSpot(t *testing.T) {
	srv := serve(t, map[string]func(w http.ResponseWriter){
		"/0/private/Balance": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"error":[],"result":{"XXBT":"1.0","ZUSD":"250.0"}}`))
		},
		"/0/public/Ticker": func(w http.ResponseWriter) {
			// Legacy pair naming, price in c[0]. The :BTNL variant must be
			// skipped rather than overwrite the plain pair.
			_, _ = w.Write([]byte(`{"error":[],"result":{
				"XXBTZUSD":{"c":["78744.53","0.01"]},
				"XBTUSD:BTNL":{"c":["1.00","0.01"]}
			}}`))
		},
	})
	defer srv.Close()

	c := NewKraken(&Credentials{APIKey: "k", APISecret: "c2VjcmV0"})
	c.client = &http.Client{Transport: hostRewriterFor(t, srv)}

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	want := btcPrice + 250
	if bal.Equity != want {
		t.Errorf("equity = %v, want %v (XXBT priced, ZUSD 1:1)", bal.Equity, want)
	}
}

// The deleted fallback: when no USD-like asset was present, Kraken used to sum
// raw QUANTITIES into a USD total — 1.5 BTC contributed 1.5 USD. It is gone,
// and a pricing outage now fails loudly instead of inventing a number.
func TestKrakenGetBalance_NoQuantityAsDollarsFallback(t *testing.T) {
	srv := serve(t, map[string]func(w http.ResponseWriter){
		"/0/private/Balance": func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"error":[],"result":{"XXBT":"1.5"}}`))
		},
		"/0/public/Ticker": func(w http.ResponseWriter) {
			http.Error(w, "down", http.StatusServiceUnavailable)
		},
	})
	defer srv.Close()

	c := NewKraken(&Credentials{APIKey: "k", APISecret: "c2VjcmV0"})
	c.client = &http.Client{Transport: hostRewriterFor(t, srv)}

	bal, err := c.GetBalance(context.Background())
	if err == nil {
		t.Fatalf("want an error when pricing is unavailable, got equity %v", bal.Equity)
	}
	if !errors.Is(err, ErrSpotPricingUnavailable) || !errors.Is(err, ErrTransient) {
		t.Errorf("error must be a typed transient pricing failure, got: %v", err)
	}
}

func TestNormalizeKrakenAsset(t *testing.T) {
	cases := map[string]string{
		"XXBT":   "BTC",
		"XBT":    "BTC",
		"XETH":   "ETH",
		"ETH2.S": "ETH", // staked ether tracks ether
		"USDC.F": "USDC",
		"DOT.S":  "DOT",
		"ZUSD":   "USD",
		"ZEUR":   "EUR",
		"XXDG":   "DOGE",
		"AAVE":   "AAVE",
		"SOL":    "SOL",
		"":       "",
	}
	for in, want := range cases {
		if got := normalizeKrakenAsset(in); got != want {
			t.Errorf("normalizeKrakenAsset(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseLooseNumber(t *testing.T) {
	// BingX and Huobi send bare numbers; Gate, KuCoin and Kraken quote theirs.
	// Both shapes must decode, or a venue changing representation silently
	// zeroes every price it publishes.
	cases := map[string]float64{
		`78744.53`:   78744.53,
		`"78744.53"`: 78744.53,
		`"0"`:        0,
		`null`:       0,
		`""`:         0,
		`"abc"`:      0,
	}
	for raw, want := range cases {
		if got := ParseLooseNumber(json.RawMessage(raw)); got != want {
			t.Errorf("ParseLooseNumber(%s) = %v, want %v", raw, got, want)
		}
	}
}

// hostRewriterFor points a connector with a hardcoded API host at the test
// server (Kraken builds its URL from the krakenAPI const, not from a field).
func hostRewriterFor(t *testing.T, srv *httptest.Server) http.RoundTripper {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	return hostRewriter{base: http.DefaultTransport, target: u}
}
