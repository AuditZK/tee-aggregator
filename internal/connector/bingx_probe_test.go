package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A key whose money sits in the fund wallet syncs as 0, which reads exactly
// like an empty account. The probe has to show the wallet that holds it.
func TestBingXProbeBalance_ShowsWalletsTheSyncSkips(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openApi/swap/v2/user/balance":
			_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"balance":{"asset":"USDT","balance":"0","equity":"0","unrealizedProfit":"0","availableMargin":"0"}}}`))
		case "/openApi/spot/v1/account/balance":
			_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"balances":[]}}`))
		case "/openApi/fund/v1/account/balance":
			_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"assets":[{"asset":"USDT","free":"42.5","locked":"0"}]}}`))
		default:
			_, _ = w.Write([]byte(`{"code":100400,"msg":"this api is not exist","data":null}`))
		}
	}))
	t.Cleanup(srv.Close)
	b := &BingX{base: CryptoBase{APIKey: "k", APISecret: "s", Client: srv.Client(), BaseURL: srv.URL}}

	p, err := b.ProbeBalance(context.Background())
	if err != nil {
		t.Fatalf("ProbeBalance: %v", err)
	}
	if p.Derived == nil || p.Derived.Equity != 0 {
		t.Fatalf("derived = %+v, want the 0 a sync records", p.Derived)
	}
	var fund map[string]string
	for _, c := range p.Currencies {
		if c["wallet"] == "fund" {
			fund = c
		}
	}
	if fund == nil || !strings.Contains(fund["data"], `"free":"42.5"`) {
		t.Fatalf("fund wallet not shown verbatim: %v", p.Currencies)
	}
	if p.Account["coin_m.code"] != "100400" || p.Account["coin_m.msg"] == "" {
		t.Fatalf("venue refusal not surfaced: %v", p.Account)
	}
}
