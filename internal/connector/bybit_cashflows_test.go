package connector

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// bybitLogServer answers the transaction-log endpoint from a scripted list of
// pages, one per call, and records every query it was asked.
type bybitLogServer struct {
	srv          *httptest.Server
	mu           sync.Mutex
	queries      []url.Values
	pages        []string
	tickers      string
	tickerHits   int
	tickerStatus int
}

func newBybitLogServer(t *testing.T, pages []string, tickers string) *bybitLogServer {
	t.Helper()
	s := &bybitLogServer{pages: pages, tickers: tickers}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v5/account/transaction-log":
			s.mu.Lock()
			idx := len(s.queries)
			s.queries = append(s.queries, r.URL.Query())
			page := ""
			if idx < len(s.pages) {
				page = s.pages[idx]
			}
			s.mu.Unlock()
			if page == "" {
				page = bybitPageJSON("")
			}
			io.WriteString(w, page)
		case "/v5/market/tickers":
			s.mu.Lock()
			s.tickerHits++
			status := s.tickerStatus
			s.mu.Unlock()
			if status != 0 {
				w.WriteHeader(status)
				io.WriteString(w, `{"retCode":10001,"retMsg":"bad request"}`)
				return
			}
			io.WriteString(w, s.tickers)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *bybitLogServer) connector() *Bybit {
	b := NewBybitWithClient(
		&Credentials{Exchange: "bybit", APIKey: "key", APISecret: "secret"},
		&http.Client{Timeout: 5 * time.Second},
	)
	b.baseURL = s.srv.URL
	return b
}

func (s *bybitLogServer) recorded() []url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]url.Values(nil), s.queries...)
}

func (s *bybitLogServer) tickerCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tickerHits
}

func bybitRowJSON(id string, at time.Time, coin, typ, cashFlow string) string {
	return `{"id":"` + id +
		`","transactionTime":"` + strconv.FormatInt(at.UnixMilli(), 10) +
		`","currency":"` + coin +
		`","type":"` + typ +
		`","cashFlow":"` + cashFlow +
		`","cashBalance":"1000"}`
}

func bybitPageJSON(nextCursor string, rows ...string) string {
	return `{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":"` + nextCursor +
		`","list":[` + strings.Join(rows, ",") + `]}}`
}

func bybitTickersJSON(pairs map[string]string) string {
	var items []string
	for symbol, price := range pairs {
		items = append(items, `{"symbol":"`+symbol+`","lastPrice":"`+price+`"}`)
	}
	return `{"retCode":0,"retMsg":"OK","result":{"list":[` + strings.Join(items, ",") + `]}}`
}

func bySum(flows []*Cashflow) (dep, wd float64) {
	for _, f := range flows {
		if f.Amount > 0 {
			dep += f.Amount
		} else {
			wd += -f.Amount
		}
	}
	return
}

func byApprox(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// The shape of a real sync after a gap: the range is split into windows the
// endpoint accepts, one of them pages through a cursor, a row repeats where two
// windows touch, and the TRADE rows between them are performance, not capital.
func TestBybitGetCashflows_WindowsCursorAndDedupe(t *testing.T) {
	now := time.Now().UTC()
	s := newBybitLogServer(t, []string{
		bybitPageJSON("c1", bybitRowJSON("t1", now.Add(-12*24*time.Hour), "USDT", "TRANSFER_IN", "500")),
		bybitPageJSON("",
			bybitRowJSON("t2", now.Add(-11*24*time.Hour), "USDT", "TRADE", "3.2"),
			bybitRowJSON("t3", now.Add(-8*24*time.Hour), "USDT", "TRANSFER_OUT", "-120.5"),
		),
		bybitPageJSON("",
			bybitRowJSON("t3", now.Add(-8*24*time.Hour), "USDT", "TRANSFER_OUT", "-120.5"),
			bybitRowJSON("t4", now.Add(-3*24*time.Hour), "USDT", "TRANSFER_IN", "40"),
		),
	}, "")

	b := s.connector()
	flows, err := b.GetCashflows(context.Background(), now.Add(-13*24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 3 {
		t.Fatalf("got %d flows, want 3 — TRADE must not book and t3 must count once: %+v", len(flows), flows)
	}
	dep, wd := bySum(flows)
	if !byApprox(dep, 540) || !byApprox(wd, 120.5) {
		t.Fatalf("got dep=%v wd=%v, want dep=540 wd=120.5", dep, wd)
	}

	queries := s.recorded()
	if len(queries) != 4 {
		t.Fatalf("got %d requests, want 4 (3 windows, one of them paged): %v", len(queries), queries)
	}
	if queries[0].Get("cursor") != "" {
		t.Fatalf("first page carried a cursor: %q", queries[0].Get("cursor"))
	}
	if queries[1].Get("cursor") != "c1" {
		t.Fatalf("second page cursor = %q, want c1", queries[1].Get("cursor"))
	}
	for i, q := range queries {
		if q.Get("accountType") != "UNIFIED" {
			t.Fatalf("request %d accountType = %q, want UNIFIED", i, q.Get("accountType"))
		}
		if q.Get("limit") != "50" {
			t.Fatalf("request %d limit = %q, want 50 (endpoint maximum)", i, q.Get("limit"))
		}
		start, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
		end, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
		if span := time.Duration(end-start) * time.Millisecond; span > 7*24*time.Hour {
			t.Fatalf("request %d spans %v, over the 7-day limit", i, span)
		}
	}

	if s.tickerCalls() != 0 {
		t.Fatalf("priced a stables-only window through %d ticker calls", s.tickerCalls())
	}
	if warns := b.CapabilityWarnings(); len(warns) != 0 {
		t.Fatalf("a clean window raised warnings: %v", warns)
	}
}

// Bybit refuses a transaction-log range wider than 7 days, so a catch-up sync
// reaching a month back has to arrive as a tiled sequence of legal windows.
func TestBybitGetCashflows_SplitsIntoWindowsTheEndpointAccepts(t *testing.T) {
	now := time.Now().UTC()
	since := now.Add(-20 * 24 * time.Hour)
	s := newBybitLogServer(t, nil, "")

	if _, err := s.connector().GetCashflows(context.Background(), since); err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}

	queries := s.recorded()
	if len(queries) == 0 {
		t.Fatal("no transaction-log request was issued")
	}

	prevEnd := since.UnixMilli()
	for i, q := range queries {
		start, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
		end, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
		if span := time.Duration(end-start) * time.Millisecond; span > 7*24*time.Hour {
			t.Fatalf("window %d spans %v, over the endpoint's 7-day limit", i, span)
		}
		if start != prevEnd {
			t.Fatalf("window %d starts at %v, leaving a hole after %v",
				i, time.UnixMilli(start).UTC(), time.UnixMilli(prevEnd).UTC())
		}
		prevEnd = end
	}
	if prevEnd < now.UnixMilli() {
		t.Fatalf("the windows stopped at %v, short of the requested end",
			time.UnixMilli(prevEnd).UTC())
	}
}

// A non-stable transfer is valued through the public spot tickers, and the sign
// of cashFlow decides the direction.
func TestBybitGetCashflows_NonStableValuedThroughTickers(t *testing.T) {
	now := time.Now().UTC()
	s := newBybitLogServer(t, []string{
		bybitPageJSON("", bybitRowJSON("t1", now.Add(-30*time.Minute), "BTC", "TRANSFER_OUT", "-0.5")),
	}, bybitTickersJSON(map[string]string{"BTCUSDT": "50000"}))

	flows, err := s.connector().GetCashflows(context.Background(), now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 1 || !byApprox(flows[0].Amount, -25000) {
		t.Fatalf("flows = %+v, want one -25000 outflow", flows)
	}
	if s.tickerCalls() != 1 {
		t.Fatalf("ticker calls = %d, want exactly 1", s.tickerCalls())
	}
}

// A coin with no tradable pair is dropped WITH a marker. Valuing it at zero
// would turn a real deposit into performance, which is the whole defect.
func TestBybitGetCashflows_UnpriceableCoinWarnsInsteadOfBookingZero(t *testing.T) {
	now := time.Now().UTC()
	s := newBybitLogServer(t, []string{
		bybitPageJSON("", bybitRowJSON("t1", now.Add(-30*time.Minute), "RWUSD", "TRANSFER_IN", "999")),
	}, bybitTickersJSON(map[string]string{"BTCUSDT": "50000"}))

	b := s.connector()
	flows, err := b.GetCashflows(context.Background(), now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 0 {
		t.Fatalf("flows = %+v, want none — an unpriceable coin must not book", flows)
	}
	if got := b.CapabilityWarnings(); len(got) != 1 || got[0] != "bybit_transfer_unpriced:RWUSD" {
		t.Fatalf("warnings = %v, want [bybit_transfer_unpriced:RWUSD]", got)
	}
}

// The ticker call failing is reported, not swallowed into a stables-only window.
func TestBybitGetCashflows_PricingFailureIsReported(t *testing.T) {
	now := time.Now().UTC()
	s := newBybitLogServer(t, []string{
		bybitPageJSON("", bybitRowJSON("t1", now.Add(-30*time.Minute), "BTC", "TRANSFER_IN", "0.25")),
	}, "")
	s.tickerStatus = http.StatusBadRequest

	b := s.connector()
	flows, err := b.GetCashflows(context.Background(), now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 0 {
		t.Fatalf("flows = %+v, want none while pricing is unavailable", flows)
	}
	warns := b.CapabilityWarnings()
	if !hasWarning(warns, "bybit_spot_pricing_unavailable") || !hasWarning(warns, "bybit_transfer_unpriced:BTC") {
		t.Fatalf("warnings = %v, want both the pricing outage and the unpriced coin", warns)
	}
}

// A transaction-log window that fails aborts the call: a partial ledger
// under-reports a deposit, and the snapshot would book it as profit.
func TestBybitGetCashflows_WindowFailureAbortsTheCall(t *testing.T) {
	now := time.Now().UTC()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"retCode":10001,"retMsg":"invalid request"}`)
	}))
	t.Cleanup(srv.Close)

	b := NewBybitWithClient(
		&Credentials{Exchange: "bybit", APIKey: "key", APISecret: "secret"},
		&http.Client{Timeout: 5 * time.Second},
	)
	b.baseURL = srv.URL

	if _, err := b.GetCashflows(context.Background(), now.Add(-time.Hour)); err == nil {
		t.Fatal("a refused ledger returned no error")
	}
}

// The perimeter rule, without IO: only the two transfer types cross it.
func TestBybitClassifyCashflows_OnlyTransfersCrossThePerimeter(t *testing.T) {
	at := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)
	rows := []bybitLogRow{
		{ID: "1", T: at, Coin: "USDT", Type: "TRANSFER_IN", CashFlow: 1000},
		{ID: "2", T: at, Coin: "USDT", Type: "TRANSFER_OUT", CashFlow: -250},
		{ID: "3", T: at, Coin: "USDT", Type: "TRADE", CashFlow: 42},
		{ID: "4", T: at, Coin: "USDT", Type: "SETTLEMENT", CashFlow: -7},
		{ID: "5", T: at, Coin: "USDT", Type: "TRANSFER_IN", CashFlow: 0},
	}

	flows, warnings := bybitClassifyCashflows(rows, nil)
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v, want none", warnings)
	}
	if len(flows) != 2 {
		t.Fatalf("flows = %+v, want 2", flows)
	}
	dep, wd := bySum(flows)
	if !byApprox(dep, 1000) || !byApprox(wd, 250) {
		t.Fatalf("got dep=%v wd=%v, want dep=1000 wd=250", dep, wd)
	}
}

// The Bybit connector must satisfy CashflowFetcher — the live daily sync only
// records deposits through this interface, and without it every snapshot
// carries deposits=withdrawals=0 and reads a deposit as performance.
var _ CashflowFetcher = (*Bybit)(nil)
