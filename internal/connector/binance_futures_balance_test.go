package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// /fapi/v2/account is being retired and answers HTTP 200 with an empty
// totalMarginBalance for some accounts — no error, so the on-error fallback
// never fires and a funded futures wallet reads as zero (observed in the field
// on a master account). getFuturesBalance must cross-check /fapi/v2/balance
// when the account endpoint reports nothing and prefer it when it finds funds.
func TestBinance_FuturesBalance_FallsBackWhenAccountReportsZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/fapi/v2/account"):
			_, _ = w.Write([]byte(`{"totalMarginBalance":"0","totalUnrealizedProfit":"0","availableBalance":"0"}`))
		case strings.Contains(r.URL.Path, "/fapi/v2/balance"):
			_, _ = w.Write([]byte(`[{"asset":"USDT","balance":"20000","crossUnPnl":"164","availableBalance":"19000"}]`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	bal, err := b.getFuturesBalance(context.Background(), nil)
	if err != nil {
		t.Fatalf("getFuturesBalance: %v", err)
	}
	if bal.Equity != 20164 { // 20000 balance + 164 crossUnPnl (stable only)
		t.Fatalf("expected fallback equity 20164, got %v — the 200-but-empty account read wasn't cross-checked", bal.Equity)
	}
}

// Retry exhaustion on 429/5xx must surface as ErrTransient, and GetBalance
// must FAIL on a transiently unreadable wallet instead of silently summing
// without it (observed: the midnight herd 429'd the fapi reads and equity
// persisted short by the whole wallet; a failed sync retries, a wrong snapshot
// lies).
func TestBinance_GetBalance_FailsOnTransientFuturesError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/fapi/") {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"code":-1003,"msg":"Too many requests"}`))
			return
		}
		// Spot side healthy: ticker map + account.
		if strings.Contains(r.URL.Path, "/api/v3/ticker") {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		_, _ = w.Write([]byte(`{"balances":[{"asset":"USDT","free":"100","locked":"0"}]}`))
	}))
	defer srv.Close()

	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	_, err := b.GetBalance(context.Background())
	if err == nil {
		t.Fatal("expected GetBalance to fail when the futures wallet is transiently unreadable")
	}
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("expected ErrTransient in the chain, got %v", err)
	}
}

// A genuinely empty futures wallet (both endpoints zero) must stay 0, not loop
// or error — the fallback only wins when it actually finds funds.
func TestBinance_FuturesBalance_ZeroStaysZeroWhenBothEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/fapi/v2/balance") {
			_, _ = w.Write([]byte(`[{"asset":"USDT","balance":"0","crossUnPnl":"0","availableBalance":"0"}]`))
			return
		}
		_, _ = w.Write([]byte(`{"totalMarginBalance":"0","totalUnrealizedProfit":"0","availableBalance":"0"}`))
	}))
	defer srv.Close()

	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	bal, err := b.getFuturesBalance(context.Background(), nil)
	if err != nil {
		t.Fatalf("getFuturesBalance: %v", err)
	}
	if bal.Equity != 0 {
		t.Fatalf("expected 0 for a genuinely empty wallet, got %v", bal.Equity)
	}
}

func futuresAccountServer(t *testing.T, account string) *Binance {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(account))
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)
}

// In single-asset mode the account totals count USDT alone, so a BNFCR (or
// USDC) margin wallet must be added from the per-asset rows or it reads as its
// USDT leftover.
func TestBinance_FuturesBalance_CountsCollateralOutsideUSDTOnlyTotals(t *testing.T) {
	b := futuresAccountServer(t, `{
		"totalWalletBalance":"2.00","totalMarginBalance":"2.00","totalUnrealizedProfit":"0","availableBalance":"2.00",
		"assets":[
			{"asset":"USDT","walletBalance":"2.00","marginBalance":"2.00","unrealizedProfit":"0","availableBalance":"2.00"},
			{"asset":"BNFCR","walletBalance":"500","marginBalance":"510","unrealizedProfit":"10","availableBalance":"400"}
		]}`)
	bal, err := b.getFuturesBalance(context.Background(), nil)
	if err != nil {
		t.Fatalf("getFuturesBalance: %v", err)
	}
	if bal.Equity != 512 || bal.UnrealizedPnL != 10 || bal.Available != 402 {
		t.Fatalf("want equity 512 / uPnL 10 / available 402, got %v / %v / %v", bal.Equity, bal.UnrealizedPnL, bal.Available)
	}
}

// Multi-assets totals already fold every asset in; adding the rows again
// would double the wallet.
func TestBinance_FuturesBalance_MultiAssetTotalsAreNotDoubled(t *testing.T) {
	b := futuresAccountServer(t, `{
		"totalWalletBalance":"150","totalMarginBalance":"150","totalUnrealizedProfit":"0","availableBalance":"150",
		"assets":[
			{"asset":"USDT","walletBalance":"100","marginBalance":"100","unrealizedProfit":"0","availableBalance":"100"},
			{"asset":"USDC","walletBalance":"50","marginBalance":"50","unrealizedProfit":"0","availableBalance":"50"}
		]}`)
	bal, err := b.getFuturesBalance(context.Background(), nil)
	if err != nil {
		t.Fatalf("getFuturesBalance: %v", err)
	}
	if bal.Equity != 150 {
		t.Fatalf("want the multi-asset total 150 untouched, got %v", bal.Equity)
	}
}

// Non-stable collateral outside USDT-only totals is valued at its spot price.
func TestBinance_FuturesBalance_PricesNonStableCollateral(t *testing.T) {
	b := futuresAccountServer(t, `{
		"totalWalletBalance":"0","totalMarginBalance":"0","totalUnrealizedProfit":"0","availableBalance":"0",
		"assets":[
			{"asset":"USDT","walletBalance":"0","marginBalance":"0","unrealizedProfit":"0","availableBalance":"0"},
			{"asset":"BNB","walletBalance":"2","marginBalance":"2","unrealizedProfit":"0","availableBalance":"2"}
		]}`)
	bal, err := b.getFuturesBalance(context.Background(), map[string]float64{"BNBUSDT": 600})
	if err != nil {
		t.Fatalf("getFuturesBalance: %v", err)
	}
	if bal.Equity != 1200 {
		t.Fatalf("want 2 BNB at 600 = 1200, got %v", bal.Equity)
	}
}
