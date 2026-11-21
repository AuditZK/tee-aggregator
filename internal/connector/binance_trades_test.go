package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// binanceFillServer serves /fapi/v1/userTrades the way the venue does: fills
// in [startTime, endTime], oldest first, at most limit, no cursor.
type binanceFillServer struct {
	mu    sync.Mutex
	fills []binanceFuturesFill
	spans []time.Duration
	b     *Binance
}

func newBinanceFillServer(t *testing.T, fills []binanceFuturesFill) *binanceFillServer {
	t.Helper()
	s := &binanceFillServer{fills: fills}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.Contains(r.URL.Path, "/fapi/v1/userTrades") {
			w.Write([]byte(`{"balances":[]}`))
			return
		}
		q := r.URL.Query()
		from, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))
		s.mu.Lock()
		s.spans = append(s.spans, time.Duration(to-from)*time.Millisecond)
		s.mu.Unlock()
		var page []binanceFuturesFill
		for _, f := range s.fills {
			if f.Time >= from && f.Time <= to && len(page) < limit {
				page = append(page, f)
			}
		}
		body, _ := json.Marshal(page)
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	s.b = NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"},
		&http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}})
	return s
}

func binanceFills(n int, from time.Time, span time.Duration, side string, firstID int64) []binanceFuturesFill {
	out := make([]binanceFuturesFill, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, binanceFuturesFill{
			ID: firstID + int64(i), Symbol: "BTCUSDT", Price: "100", Qty: "1", Commission: "0.04",
			CommissionAsset: "USDT", Side: side, RealizedPnl: "0",
			Time: from.Add(time.Duration(i) * span / time.Duration(n)).UnixMilli(),
		})
	}
	return out
}

// A day past the 1000-fill page comes back whole: the call has no cursor, so
// a full page means the range is split, not that the day is done.
func TestBinanceFuturesTrades_ReadsADayPastOnePage(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Second)
	day := end.Add(-24 * time.Hour)
	fills := append(binanceFills(1500, day, 12*time.Hour, "BUY", 1), binanceFills(900, day.Add(12*time.Hour), 12*time.Hour, "SELL", 10000)...)
	s := newBinanceFillServer(t, fills)

	trades, err := s.b.getFuturesTrades(context.Background(), day, end)
	if err != nil {
		t.Fatalf("getFuturesTrades: %v", err)
	}
	if len(trades) != 2400 {
		t.Fatalf("trades = %d, want 2400", len(trades))
	}
	buys := 0
	for _, tr := range trades {
		if tr.Side != "buy" && tr.Side != "sell" {
			t.Fatalf("side = %q, want lowercase", tr.Side)
		}
		if tr.Side == "buy" {
			buys++
		}
	}
	if buys != 1500 {
		t.Fatalf("buys = %d, want 1500", buys)
	}
}

// The endpoint refuses a range wider than seven days, so a catch-up after a
// gap reads it in windows.
func TestBinanceFuturesTrades_WindowsOfSevenDays(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Second)
	start := end.Add(-20 * 24 * time.Hour)
	s := newBinanceFillServer(t, binanceFills(30, start, 20*24*time.Hour, "BUY", 1))

	trades, err := s.b.getFuturesTrades(context.Background(), start, end)
	if err != nil {
		t.Fatalf("getFuturesTrades: %v", err)
	}
	if len(trades) != 30 {
		t.Fatalf("trades = %d, want 30", len(trades))
	}
	for _, span := range s.spans {
		if span > 7*24*time.Hour {
			t.Fatalf("a request spans %v, over seven days", span)
		}
	}
	if len(s.spans) != 3 {
		t.Fatalf("requests = %d, want 3 windows", len(s.spans))
	}
}
