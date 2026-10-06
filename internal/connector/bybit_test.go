package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type bybitExecRow struct {
	id, side, execType, fee string
	at                      time.Time
}

// bybitExecServer serves the execution list the way the venue does: rows in
// [startTime, endTime] of the asked execType, newest first, limit per page,
// the cursor an offset. stallAt makes the page at that offset name itself as
// the next one, which the venue does on some accounts.
type bybitExecServer struct {
	mu      sync.Mutex
	rows    []bybitExecRow
	stallAt int
	queries []url.Values
	b       *Bybit
}

func newBybitExecServer(t *testing.T, rows []bybitExecRow, stallAt int) *bybitExecServer {
	t.Helper()
	s := &bybitExecServer{rows: rows, stallAt: stallAt}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query()
		s.mu.Lock()
		s.queries = append(s.queries, q)
		s.mu.Unlock()
		io.WriteString(w, s.serve(q))
	}))
	t.Cleanup(srv.Close)

	s.b = NewBybitWithClient(
		&Credentials{Exchange: "bybit", APIKey: "key", APISecret: "secret"},
		&http.Client{Timeout: 5 * time.Second},
	)
	s.b.baseURL = srv.URL
	return s
}

func (s *bybitExecServer) serve(q url.Values) string {
	start, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
	end, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("cursor"))

	var match []bybitExecRow
	for _, r := range s.rows {
		ms := r.at.UnixMilli()
		if ms < start || ms > end || (q.Get("execType") != "" && q.Get("execType") != r.execType) {
			continue
		}
		match = append(match, r)
	}
	sort.SliceStable(match, func(i, j int) bool { return match[i].at.After(match[j].at) })

	stop := offset + limit
	if stop > len(match) {
		stop = len(match)
	}
	next := ""
	if stop < len(match) {
		next = strconv.Itoa(stop)
	}
	if s.stallAt > 0 && offset == s.stallAt {
		next = strconv.Itoa(offset)
	}

	list := make([]map[string]string, 0, stop-offset)
	for _, r := range match[min(offset, len(match)):stop] {
		list = append(list, map[string]string{
			"execId": r.id, "symbol": "BTCUSDT", "side": r.side, "execType": r.execType,
			"execPrice": "100", "execQty": "1", "execFee": r.fee,
			"execTime": strconv.FormatInt(r.at.UnixMilli(), 10), "closedPnl": "0",
		})
	}
	body, _ := json.Marshal(map[string]any{
		"retCode": 0, "retMsg": "OK",
		"result": map[string]any{"nextPageCursor": next, "list": list},
	})
	return string(body)
}

func (s *bybitExecServer) recorded() []url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]url.Values(nil), s.queries...)
}

// spread lays n rows evenly over [from, from+span).
func spread(prefix string, n int, from time.Time, span time.Duration, side, execType string) []bybitExecRow {
	rows := make([]bybitExecRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, bybitExecRow{
			id: prefix + strconv.Itoa(i), side: side, execType: execType, fee: "0.1",
			at: from.Add(time.Duration(i) * span / time.Duration(n)),
		})
	}
	return rows
}

// A day busier than one page comes back whole, with the venue's capitalised
// side folded to the lowercase the sync aggregates on.
func TestBybitGetTrades_ReadsABusyDayWholeAndLowercasesSide(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Second)
	day := end.Add(-24 * time.Hour)
	rows := append(spread("b", 160, day, 12*time.Hour, "Buy", "Trade"),
		spread("s", 90, day.Add(12*time.Hour), 12*time.Hour, "Sell", "Trade")...)
	s := newBybitExecServer(t, rows, 0)

	trades, err := s.b.GetTrades(context.Background(), day, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 250 {
		t.Fatalf("trades = %d, want 250", len(trades))
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
	if buys != 160 || sells != 90 {
		t.Fatalf("buys=%d sells=%d, want 160/90", buys, sells)
	}
}

// Bybit's cursor can name the page it came with. Followed, it returned the
// same rows forever and the rest of the window went unread, a different few
// funding rows on every read; the window is split instead.
func TestBybitGetTrades_ASelfPointingCursorLosesNothing(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Second)
	day := end.Add(-24 * time.Hour)
	s := newBybitExecServer(t, spread("e", 250, day, 24*time.Hour, "Buy", "Trade"), 100)

	trades, err := s.b.GetTrades(context.Background(), day, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 250 {
		t.Fatalf("trades = %d, want 250 despite the stalled cursor", len(trades))
	}
	for _, q := range s.recorded() {
		if q.Get("cursor") != "" {
			t.Fatalf("followed a cursor (%s) on a range that could be split", q.Get("cursor"))
		}
	}
}

// Every position's funding settles on the same millisecond, so a settlement
// wider than one page cannot be split: the cursor is followed there, and a
// stall fails the read instead of returning part of it.
func TestBybitGetFundingFees_OneSettlementWiderThanAPage(t *testing.T) {
	settle := time.Now().UTC().Truncate(time.Hour).Add(-8 * time.Hour)
	var rows []bybitExecRow
	for i := 0; i < 150; i++ {
		rows = append(rows, bybitExecRow{id: "f" + strconv.Itoa(i), side: "Buy", execType: "Funding", fee: "1", at: settle})
	}

	fees, err := newBybitExecServer(t, rows, 0).b.GetFundingFees(context.Background(), nil, settle.Add(-time.Hour))
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if len(fees) != 150 {
		t.Fatalf("fees = %d, want 150", len(fees))
	}

	if _, err := newBybitExecServer(t, rows, 100).b.GetFundingFees(context.Background(), nil, settle.Add(-time.Hour)); err == nil {
		t.Fatal("a stalled cursor on an unsplittable settlement returned no error")
	}
}

// A funding settlement in the execution list carries the whole position as
// its quantity: counted as a fill, it adds the position's notional to the
// day's volume and a phantom buy or sell to the split.
func TestBybitGetTrades_SkipsFundingSettlements(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Second)
	day := end.Add(-24 * time.Hour)
	rows := append(spread("t", 3, day, time.Hour, "Buy", "Trade"), spread("f", 4, day.Add(2*time.Hour), 4*time.Hour, "Buy", "Funding")...)
	rows = append(rows, bybitExecRow{id: "liq", side: "Sell", execType: "BustTrade", fee: "1", at: day.Add(20 * time.Hour)})
	s := newBybitExecServer(t, rows, 0)

	trades, err := s.b.GetTrades(context.Background(), day, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 4 {
		t.Fatalf("trades = %d, want 4 (3 fills + 1 liquidation, no funding)", len(trades))
	}
	for _, tr := range trades {
		if strings.HasPrefix(tr.ID, "f") {
			t.Fatalf("funding settlement %s returned as a trade", tr.ID)
		}
	}
}

// Funding is read on its own, signed negative when charged as the
// FundingFee contract says, so it lands in funding_fees and not in volume.
func TestBybitGetFundingFees_SignedNegativeWhenCharged(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour)
	s := newBybitExecServer(t, []bybitExecRow{
		{id: "f1", side: "Buy", execType: "Funding", fee: "1.5", at: at},
		{id: "f2", side: "Sell", execType: "Funding", fee: "-0.25", at: at.Add(-time.Minute)},
		{id: "f3", side: "Buy", execType: "Funding", fee: "0", at: at.Add(-2 * time.Minute)},
		{id: "t1", side: "Buy", execType: "Trade", fee: "9", at: at},
	}, 0)

	fees, err := s.b.GetFundingFees(context.Background(), nil, at.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if len(fees) != 2 {
		t.Fatalf("fees = %d, want 2 (zero row and trade dropped)", len(fees))
	}
	if fees[0].Amount != -1.5 || fees[1].Amount != 0.25 {
		t.Fatalf("amounts = %v, %v; want -1.5 (paid), 0.25 (received)", fees[0].Amount, fees[1].Amount)
	}
	for _, q := range s.recorded() {
		if q.Get("execType") != "Funding" {
			t.Fatalf("query %v, want execType=Funding", q)
		}
	}
}

// The endpoint refuses a range wider than seven days, so a sync catching up
// on a gap reads it in windows, and a fill on a shared boundary counts once.
func TestBybitExecutions_SplitsWindowsAndDedupesBoundary(t *testing.T) {
	end := time.Now().UTC().Truncate(time.Second)
	start := end.Add(-10 * 24 * time.Hour)
	edge := start.Add(bybitExecWindow)
	s := newBybitExecServer(t, []bybitExecRow{{id: "edge", side: "Buy", execType: "Trade", fee: "0.1", at: edge}}, 0)

	trades, err := s.b.GetTrades(context.Background(), start, end)
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
