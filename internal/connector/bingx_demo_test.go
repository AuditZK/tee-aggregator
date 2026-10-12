package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	bingxEmptyOverview = `{"code":0,"data":[{"accountType":"sopt","usdtBalance":"0.00"},{"accountType":"USDTMPerp","usdtBalance":"0.000"}]}`
	bingxEmptyFund     = `{"code":0,"data":{"assets":[]}}`
	bingxGrantOnly     = `{"code":0,"data":[{"incomeType":"TRANSFER","income":"100000","asset":"VST"}]}`
	bingxDemoTraded    = `{"code":0,"data":[{"incomeType":"TRADING_FEE","income":"-0.1","asset":"VST"},{"incomeType":"TRANSFER","income":"100000","asset":"VST"}]}`
)

func bingxDetectServer(t *testing.T, overview, fund, income string) *BingX {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openApi/account/v1/allAccountBalance":
			_, _ = w.Write([]byte(overview))
		case "/openApi/fund/v1/account/balance":
			_, _ = w.Write([]byte(fund))
		case "/openApi/swap/v2/user/income":
			_, _ = w.Write([]byte(income))
		default:
			_, _ = w.Write([]byte(`{"code":100400,"msg":"not found"}`))
		}
	}))
	t.Cleanup(srv.Close)
	return &BingX{base: CryptoBase{APIKey: "k", APISecret: "s", Client: srv.Client(), BaseURL: srv.URL}}
}

// One BingX key reaches both the live and the demo account. A connection is
// demo only when the live side holds nothing and the demo side has traded:
// the grant every user receives is not activity.
func TestBingXDetectIsPaper(t *testing.T) {
	cases := []struct {
		name                   string
		overview, fund, income string
		want                   bool
	}{
		{"empty live, demo traded", bingxEmptyOverview, bingxEmptyFund, bingxDemoTraded, true},
		{"empty live, demo grant only", bingxEmptyOverview, bingxEmptyFund, bingxGrantOnly, false},
		{"funded perpetual wins over demo", `{"code":0,"data":[{"accountType":"USDTMPerp","usdtBalance":"12.5"}]}`, bingxEmptyFund, bingxDemoTraded, false},
		{"funded fund wallet wins over demo", bingxEmptyOverview, `{"code":0,"data":{"assets":[{"asset":"USDT","free":"3","locked":"0"}]}}`, bingxDemoTraded, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := bingxDetectServer(t, tc.overview, tc.fund, tc.income).DetectIsPaper(context.Background())
			if err != nil {
				t.Fatalf("DetectIsPaper: %v", err)
			}
			if got != tc.want {
				t.Fatalf("DetectIsPaper = %v, want %v", got, tc.want)
			}
		})
	}
}

// A venue refusal must not read as "live": the flag would be stored false.
func TestBingXDetectIsPaper_VenueRefusalIsAnError(t *testing.T) {
	b := bingxDetectServer(t, `{"code":100001,"msg":"signature verification failed"}`, bingxEmptyFund, bingxDemoTraded)
	if _, err := b.DetectIsPaper(context.Background()); err == nil {
		t.Fatal("DetectIsPaper accepted a refused overview")
	}
}

// A connection stored as demo stays demo: re-detection must not flip a series
// to the live account because money arrived there later.
func TestBingXDetectIsPaper_DemoConnectionStaysDemo(t *testing.T) {
	b := NewBingX(&Credentials{APIKey: "k", APISecret: "s", Demo: true})
	got, err := b.DetectIsPaper(context.Background())
	if err != nil || !got {
		t.Fatalf("DetectIsPaper = %v, %v; want true", got, err)
	}
	if b.base.BaseURL != bingxVSTAPI {
		t.Fatalf("demo connector reads %s, want the VST host", b.base.BaseURL)
	}
}

func TestBingXDemoBalance_ReadsPerpetualOnly(t *testing.T) {
	var spotCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/openApi/swap/v2/user/balance":
			_, _ = w.Write([]byte(`{"code":0,"data":{"balance":{"asset":"VST","balance":"1000.0","equity":"990.5","unrealizedProfit":"-9.5","availableMargin":"965.5"}}}`))
		default:
			if strings.Contains(r.URL.Path, "spot") {
				spotCalled = true
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{}}`))
		}
	}))
	t.Cleanup(srv.Close)
	b := &BingX{base: CryptoBase{APIKey: "k", APISecret: "s", Client: srv.Client(), BaseURL: srv.URL}, demo: true}

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 990.5 || bal.Available != 965.5 || bal.UnrealizedPnL != -9.5 {
		t.Fatalf("balance = %+v", bal)
	}
	if spotCalled {
		t.Fatal("demo balance asked the spot account, which the demo host does not have")
	}
}
