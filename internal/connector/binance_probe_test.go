package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func probeServer(t *testing.T, multiAssets, account string) *Binance {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/fapi/v1/multiAssetsMargin"):
			_, _ = w.Write([]byte(`{"multiAssetsMargin":` + multiAssets + `}`))
		case strings.Contains(r.URL.Path, "/fapi/v2/account"):
			_, _ = w.Write([]byte(account))
		case strings.Contains(r.URL.Path, "/api/v3/ticker/price"):
			_, _ = w.Write([]byte(`[{"symbol":"BNBUSDT","price":"600"}]`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)
}

// A BNFCR credit account whose totals count USDT alone: the probe states the
// collateral those totals leave out, valued at par.
func TestBinance_ProbeBalance_StatesCollateralOutsideUSDTOnlyTotals(t *testing.T) {
	b := probeServer(t, "false", `{
		"totalWalletBalance":"2.00","totalMarginBalance":"2.00","totalUnrealizedProfit":"0","availableBalance":"2.00",
		"assets":[
			{"asset":"USDT","walletBalance":"2.00","marginBalance":"2.00","unrealizedProfit":"0","availableBalance":"2.00"},
			{"asset":"BNFCR","walletBalance":"500","marginBalance":"510","unrealizedProfit":"10","availableBalance":"400"},
			{"asset":"BNB","walletBalance":"0.5","marginBalance":"0.5","unrealizedProfit":"0","availableBalance":"0.5"},
			{"asset":"USDC","walletBalance":"0","marginBalance":"0","unrealizedProfit":"0","availableBalance":"0"}
		]}`)
	probe, err := b.ProbeBalance(context.Background())
	if err != nil {
		t.Fatalf("ProbeBalance: %v", err)
	}
	if probe.AccountMode != "multiAssetsMargin=false" {
		t.Errorf("mode %q", probe.AccountMode)
	}
	if probe.Account["totalMarginBalance"] != "2.00" {
		t.Errorf("totals must arrive verbatim, got %q", probe.Account["totalMarginBalance"])
	}
	if len(probe.Currencies) != 3 {
		t.Errorf("zero rows are dropped, non-zero kept: got %d rows", len(probe.Currencies))
	}
	if probe.Account["probe.totals_cover_usdt_only"] != "true" || probe.Account["probe.collateral_outside_totals_usd"] != "810.00" {
		t.Fatalf("want USDT-only totals missing 510 BNFCR + 0.5 BNB at 600 = 810.00, got %s / %s",
			probe.Account["probe.totals_cover_usdt_only"], probe.Account["probe.collateral_outside_totals_usd"])
	}
}

// Multi-assets totals already count every asset: nothing is outside them.
func TestBinance_ProbeBalance_MultiAssetTotalsLeaveNothingOut(t *testing.T) {
	b := probeServer(t, "true", `{
		"totalWalletBalance":"150","totalMarginBalance":"150","totalUnrealizedProfit":"0","availableBalance":"150",
		"assets":[
			{"asset":"USDT","walletBalance":"100","marginBalance":"100","unrealizedProfit":"0","availableBalance":"100"},
			{"asset":"USDC","walletBalance":"50","marginBalance":"50","unrealizedProfit":"0","availableBalance":"50"}
		]}`)
	probe, err := b.ProbeBalance(context.Background())
	if err != nil {
		t.Fatalf("ProbeBalance: %v", err)
	}
	if probe.Account["probe.totals_cover_usdt_only"] != "false" || probe.Account["probe.collateral_outside_totals_usd"] != "0.00" {
		t.Fatalf("got %s / %s", probe.Account["probe.totals_cover_usdt_only"], probe.Account["probe.collateral_outside_totals_usd"])
	}
}
