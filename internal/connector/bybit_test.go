package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type bybitExecRow struct {
	id, side, execType, fee string
}

func bybitExecPageJSON(cursor string, rows []bybitExecRow) string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf(
			`{"execId":%q,"symbol":"BTCUSDT","side":%q,"execType":%q,"execPrice":"100","execQty":"1","execFee":%q,"execTime":"1757000000000","closedPnl":"0"}`,
			r.id, r.side, r.execType, r.fee))
	}
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"list":[%s]}}`,
		cursor, strings.Join(out, ","))
}

func bybitExecRows(from, n int, side, execType string) []bybitExecRow {
	rows := make([]bybitExecRow, 0, n)
	for i := from; i < from+n; i++ {
		rows = append(rows, bybitExecRow{id: "e" + strconv.Itoa(i), side: side, execType: execType, fee: "0.1"})
	}
	return rows
}

// bybitExecServer answers the execution list from a scripted list of pages,
// one per call, and records every query it was asked.
type bybitExecServer struct {
	mu      sync.Mutex
	pages   []string
	queries []url.Values
	b       *Bybit
}

func newBybitExecServer(t *testing.T, pages []string) *bybitExecServer {
	t.Helper()
	s := &bybitExecServer{pages: pages}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		s.mu.Lock()
		idx := len(s.queries)
		s.queries = append(s.queries, r.URL.Query())
		s.mu.Unlock()
		if idx >= len(s.pages) {
			io.WriteString(w, bybitExecPageJSON("", nil))
			return
		}
		io.WriteString(w, s.pages[idx])
	}))
	t.Cleanup(srv.Close)

	s.b = NewBybitWithClient(
		&Credentials{Exchange: "bybit", APIKey: "key", APISecret: "secret"},
		&http.Client{Timeout: 5 * time.Second},
	)
	s.b.baseURL = srv.URL
	return s
}

func (s *bybitExecServer) recorded() []url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]url.Values(nil), s.queries...)
}

// A day busier than one execution page must come back whole, with the
// venue's capitalised side folded to the lowercase the sync aggregates on.
func TestBybitGetTrades_FollowsCursorAndLowercasesSide(t *testing.T) {
	s := newBybitExecServer(t, []string{
		bybitExecPageJSON("page2", bybitExecRows(0, 100, "Buy", "Trade")),
		bybitExecPageJSON("", bybitExecRows(100, 50, "Sell", "Trade")),
	})

	now := time.Now().UTC()
	trades, err := s.b.GetTrades(context.Background(), now.Add(-24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 150 {
		t.Fatalf("trades = %d, want 150", len(trades))
	}

	buys, sells := 0, 0
	for _, tr := range trades {
		switch tr.Side {
		case "buy":
			buys++
		case "sell":
			sells++
		default:
			t.Fatalf("side = %q, want buy or sell", tr.Side)
		}
	}
	if buys != 100 || sells != 50 {
		t.Fatalf("buys=%d sells=%d, want 100/50", buys, sells)
	}

	q := s.recorded()
	if len(q) != 2 || q[0].Get("cursor") != "" || q[1].Get("cursor") != "page2" {
		t.Fatalf("queries = %v, want a first page then cursor page2", q)
	}
}

// A funding settlement in the execution list carries the whole position as
// its quantity: counted as a fill, it adds the position's notional to the
// day's volume and a phantom buy or sell to the split.
func TestBybitGetTrades_SkipsFundingSettlements(t *testing.T) {
	rows := append(bybitExecRows(0, 3, "Buy", "Trade"), bybitExecRows(3, 4, "Buy", "Funding")...)
	rows = append(rows, bybitExecRow{id: "liq", side: "Sell", execType: "BustTrade", fee: "1"})
	s := newBybitExecServer(t, []string{bybitExecPageJSON("", rows)})

	now := time.Now().UTC()
	trades, err := s.b.GetTrades(context.Background(), now.Add(-24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 4 {
		t.Fatalf("trades = %d, want 4 (3 fills + 1 liquidation, no funding)", len(trades))
	}
	for _, tr := range trades {
		if strings.HasPrefix(tr.ID, "e") {
			if n, _ := strconv.Atoi(strings.TrimPrefix(tr.ID, "e")); n >= 3 {
				t.Fatalf("funding settlement %s returned as a trade", tr.ID)
			}
		}
	}
}

// Funding is read on its own, signed negative when charged as the
// FundingFee contract says, so it lands in funding_fees and not in volume.
func TestBybitGetFundingFees_SignedNegativeWhenCharged(t *testing.T) {
	s := newBybitExecServer(t, []string{bybitExecPageJSON("", []bybitExecRow{
		{id: "f1", side: "Buy", execType: "Funding", fee: "1.5"},
		{id: "f2", side: "Sell", execType: "Funding", fee: "-0.25"},
		{id: "f3", side: "Buy", execType: "Funding", fee: "0"},
	})})

	fees, err := s.b.GetFundingFees(context.Background(), nil, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if len(fees) != 2 {
		t.Fatalf("fees = %d, want 2 (zero row dropped)", len(fees))
	}
	if fees[0].Amount != -1.5 || fees[1].Amount != 0.25 {
		t.Fatalf("amounts = %v, %v; want -1.5 (paid), 0.25 (received)", fees[0].Amount, fees[1].Amount)
	}

	q := s.recorded()
	if len(q) != 1 || q[0].Get("execType") != "Funding" {
		t.Fatalf("queries = %v, want one execType=Funding query", q)
	}
}

// The endpoint refuses a range wider than seven days, so a sync catching up
// on a gap reads it in windows, and a fill on a shared boundary counts once.
func TestBybitExecutions_SplitsWindowsAndDedupesBoundary(t *testing.T) {
	boundary := []bybitExecRow{{id: "edge", side: "Buy", execType: "Trade", fee: "0.1"}}
	s := newBybitExecServer(t, []string{
		bybitExecPageJSON("", boundary),
		bybitExecPageJSON("", boundary),
	})

	end := time.Now().UTC()
	trades, err := s.b.GetTrades(context.Background(), end.Add(-10*24*time.Hour), end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("trades = %d, want 1 (boundary fill read twice, counted once)", len(trades))
	}

	q := s.recorded()
	if len(q) != 2 {
		t.Fatalf("queries = %d, want 2 windows", len(q))
	}
	for _, v := range q {
		st, _ := strconv.ParseInt(v.Get("startTime"), 10, 64)
		et, _ := strconv.ParseInt(v.Get("endTime"), 10, 64)
		if span := time.Duration(et-st) * time.Millisecond; span > 7*24*time.Hour {
			t.Fatalf("window spans %v, want at most 7 days", span)
		}
	}
	if q[1].Get("endTime") != strconv.FormatInt(end.UnixMilli(), 10) {
		t.Fatalf("last window ends at %s, want the requested end", q[1].Get("endTime"))
	}
}
