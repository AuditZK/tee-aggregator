package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeMTDealsBridge serves the given deals from /history-deals, filtered by the
// from/to query the way mt-bridge filters them, and records each "to" asked.
func fakeMTDealsBridge(t *testing.T, deals []map[string]any) (*httptest.Server, func() []time.Time) {
	t.Helper()
	var mu sync.Mutex
	var asked []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var data any
		switch {
		case r.URL.Path == "/api/v1/connect":
			data = map[string]any{"session_id": "s1"}
		case strings.HasSuffix(r.URL.Path, "/history-deals"):
			from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
			to, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
			mu.Lock()
			asked = append(asked, time.Unix(to, 0))
			mu.Unlock()
			kept := []map[string]any{}
			for _, d := range deals {
				ts, _ := time.Parse(time.RFC3339, d["close_time"].(string))
				if !ts.Before(time.Unix(from, 0)) && !ts.After(time.Unix(to, 0)) {
					kept = append(kept, d)
				}
			}
			data = kept
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data})
	}))
	return srv, func() []time.Time {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Time(nil), asked...)
	}
}

// The bridge under-reads a server's UTC offset, so a deal made just before a
// read can come back dated hours after it. A read ending now still keeps it:
// the next pass resumes at this read's start and would never see it again.
func TestMetaTraderKeepsDealsDatedAheadOfTheClock(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ahead := now.Add(2 * time.Hour).Format(time.RFC3339)
	bridge, asked := fakeMTDealsBridge(t, []map[string]any{
		{"ticket": 1, "symbol": "BALANCE", "side": "deposit", "realized_pnl": 500.0, "close_time": ahead},
		{"ticket": 2, "symbol": "EURUSD", "side": "buy", "size": 1.0, "close_price": 1.1, "realized_pnl": 3.0, "close_time": ahead},
	})
	defer bridge.Close()
	mt := newTestMetaTrader(t, bridge.URL, "Broker-Live")
	ctx := context.Background()

	flows, err := mt.GetCashflows(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("cashflows: %v", err)
	}
	if len(flows) != 1 || flows[0].Amount != 500 {
		t.Errorf("cashflows = %+v, want the 500 deposit dated ahead", flows)
	}

	trades, err := mt.GetTrades(ctx, now.Add(-time.Hour), now)
	if err != nil {
		t.Fatalf("trades: %v", err)
	}
	if !hasTradeID(trades, "2") {
		t.Errorf("trades = %+v, want the trade dated ahead", trades)
	}

	for _, to := range asked() {
		if to.Before(now.Add(mtDealClockSlack - time.Minute)) {
			t.Errorf("asked the bridge for deals up to %s, want about %s past now", to.Format(time.RFC3339), mtDealClockSlack)
		}
	}
}

// A deposit is a BALANCE deal: GetCashflows books it, GetTrades must not count
// it as a zero-volume buy carrying the deposit as realized P&L.
func TestMetaTraderBalanceDealsAreNotTrades(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	at := now.Add(-30 * time.Minute).Format(time.RFC3339)
	bridge, _ := fakeMTDealsBridge(t, []map[string]any{
		{"ticket": 1, "symbol": "BALANCE", "side": "deposit", "realized_pnl": 500.0, "close_time": at},
		{"ticket": 2, "symbol": "BALANCE", "side": "withdrawal", "realized_pnl": -200.0, "close_time": at},
		{"ticket": 3, "symbol": "EURUSD", "side": "sell", "size": 1.0, "close_price": 1.1, "realized_pnl": 3.0, "close_time": at},
	})
	defer bridge.Close()
	mt := newTestMetaTrader(t, bridge.URL, "Broker-Live")

	trades, err := mt.GetTrades(context.Background(), now.Add(-time.Hour), now)
	if err != nil {
		t.Fatalf("trades: %v", err)
	}
	if len(trades) != 1 || trades[0].ID != "3" {
		t.Errorf("trades = %+v, want only the EURUSD deal", trades)
	}
}

func hasTradeID(trades []*Trade, id string) bool {
	for _, tr := range trades {
		if tr.ID == id {
			return true
		}
	}
	return false
}

// A read over a past range keeps its own end: widening it would file a deal
// made after the range under it.
func TestMetaTraderPastTradeRangeKeepsItsEnd(t *testing.T) {
	end := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Second)
	bridge, _ := fakeMTDealsBridge(t, []map[string]any{
		{"ticket": 1, "symbol": "EURUSD", "side": "buy", "size": 1.0, "close_price": 1.1, "close_time": end.Add(-time.Hour).Format(time.RFC3339)},
		{"ticket": 2, "symbol": "EURUSD", "side": "sell", "size": 1.0, "close_price": 1.1, "close_time": end.Add(time.Hour).Format(time.RFC3339)},
	})
	defer bridge.Close()
	mt := newTestMetaTrader(t, bridge.URL, "Broker-Live")

	trades, err := mt.GetTrades(context.Background(), end.Add(-24*time.Hour), end)
	if err != nil {
		t.Fatalf("trades: %v", err)
	}
	if len(trades) != 1 || trades[0].ID != "1" {
		t.Errorf("trades = %+v, want only the trade inside the range", trades)
	}
}
