package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func bingxTradesServer(t *testing.T, handle func(w http.ResponseWriter, r *http.Request)) *BingX {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(handle))
	t.Cleanup(srv.Close)
	return &BingX{base: CryptoBase{APIKey: "k", APISecret: "s", Client: srv.Client(), BaseURL: srv.URL}}
}

func TestBingXGetTrades_ReadsFilledOrders(t *testing.T) {
	b := bingxTradesServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/openApi/swap/v2/trade/allOrders" {
			_, _ = w.Write([]byte(`{"code":100400,"msg":"this api is not exist"}`))
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{"orders":[
			{"orderId":2109758158311264256,"symbol":"AAA-USDT","side":"BUY","avgPrice":"2.5","executedQty":"10","commission":"-0.0125","profit":"0","updateTime":1700000000000},
			{"orderId":"2109758158311264257","symbol":"AAA-USDT","side":"SELL","avgPrice":"0","executedQty":"0","commission":"0","profit":"0","updateTime":1700000000000}]}}`))
	})

	end := time.UnixMilli(1700000000000).Add(time.Hour)
	trades, err := b.GetTrades(context.Background(), end.Add(-24*time.Hour), end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("trades = %d, want the one filled order", len(trades))
	}
	if trades[0].ID != "2109758158311264256" {
		t.Fatalf("order id = %q, want the 19 digits verbatim", trades[0].ID)
	}
	if trades[0].Fee != 0.0125 || trades[0].Side != "buy" || trades[0].MarketType != MarketSwap {
		t.Fatalf("trade = %+v", trades[0])
	}
}

// A refused call used to parse as an empty list, so every BingX day recorded
// zero trades with nothing in the logs.
func TestBingXGetTrades_RefusalIsAnError(t *testing.T) {
	b := bingxTradesServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":100400,"msg":"this api is not exist"}`))
	})
	if _, err := b.GetTrades(context.Background(), time.Now().Add(-time.Hour), time.Now()); err == nil {
		t.Fatal("GetTrades accepted a refused call as an empty account")
	}
}

func TestBingXGetTrades_SplitsIntoSevenDayWindows(t *testing.T) {
	var windows [][2]int64
	b := bingxTradesServer(t, func(w http.ResponseWriter, r *http.Request) {
		from, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		to, _ := strconv.ParseInt(r.URL.Query().Get("endTime"), 10, 64)
		windows = append(windows, [2]int64{from, to})
		_, _ = w.Write([]byte(`{"code":0,"data":{"orders":[]}}`))
	})

	end := time.Date(2030, 1, 20, 0, 0, 0, 0, time.UTC)
	start := end.Add(-16 * 24 * time.Hour)
	if _, err := b.GetTrades(context.Background(), start, end); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(windows) != 3 {
		t.Fatalf("windows = %d, want 3 for 16 days", len(windows))
	}
	for i, w := range windows {
		if time.Duration(w[1]-w[0])*time.Millisecond > bingxOrderWindow {
			t.Fatalf("window %d spans %v", i, time.Duration(w[1]-w[0])*time.Millisecond)
		}
	}
	if windows[0][0] != start.UnixMilli() || windows[2][1] != end.UnixMilli() {
		t.Fatalf("windows do not cover [start, end]: %v", windows)
	}
}
