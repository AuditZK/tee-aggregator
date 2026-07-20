package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// igFakeServer stands in for IG's REST API: it holds a ledger, paginates it the
// way IG does, and can be told to expire the session or rate-limit. Driving the
// real connector against it exercises session handling, pagination, parsing and
// classification together, which is where the failures that unit tests miss
// actually live.
type igFakeServer struct {
	t       *testing.T
	ledger  []map[string]any
	balance map[string]any

	// omitTotalPages reproduces an upstream that paginates without reporting a
	// page count.
	omitTotalPages bool
	// expireAfter rejects session tokens once this many authed calls have been
	// served, forcing a re-login mid-sequence.
	expireAfter int32
	// rejectGenBelow rejects every token issued by a login older than this
	// generation, so all concurrent holders of the old pair are refused at once.
	rejectGenBelow int

	logins     atomic.Int32
	authedReqs atomic.Int32
	pageHits   atomic.Int32
	sessionGen atomic.Int32
}

func (f *igFakeServer) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", igContentType)

		if r.URL.Path == "/session" {
			gen := f.sessionGen.Add(1)
			f.logins.Add(1)
			f.authedReqs.Store(0)
			w.Header().Set("CST", fmt.Sprintf("cst-%d", gen))
			w.Header().Set("X-SECURITY-TOKEN", fmt.Sprintf("xst-%d", gen))
			fmt.Fprint(w, `{"currentAccountId":"ACC1","currencyIsoCode":"EUR"}`)
			return
		}

		if r.Header.Get("CST") == "" || r.Header.Get("X-SECURITY-TOKEN") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"errorCode":"error.security.client-token-missing"}`)
			return
		}

		if n := f.authedReqs.Add(1); f.expireAfter > 0 && n > f.expireAfter {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"errorCode":"error.security.client-token-invalid"}`)
			return
		}

		if f.rejectGenBelow > 0 {
			var gen int
			_, _ = fmt.Sscanf(r.Header.Get("CST"), "cst-%d", &gen)
			if gen < f.rejectGenBelow {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, `{"errorCode":"error.security.client-token-invalid"}`)
				return
			}
		}

		switch r.URL.Path {
		case "/accounts":
			resp := map[string]any{"accounts": []any{
				map[string]any{"accountId": "OTHER", "currency": "GBP",
					"balance": map[string]any{"balance": 1, "profitLoss": 0, "available": 1}},
				f.balance,
			}}
			_ = json.NewEncoder(w).Encode(resp)

		case "/positions":
			fmt.Fprint(w, `{"positions":[
				{"market":{"instrumentName":"EUR/USD","instrumentType":"CURRENCIES","bid":1.1050,"offer":1.1052},
				 "position":{"dealId":"D1","direction":"BUY","size":2,"level":1.1000,"contractSize":1}}
			]}`)

		case "/history/transactions":
			f.pageHits.Add(1)
			f.writeTransactionPage(w, r)

		default:
			f.t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func (f *igFakeServer) writeTransactionPage(w http.ResponseWriter, r *http.Request) {
	size, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if size <= 0 {
		size = 20
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("pageNumber"))
	if page <= 0 {
		page = 1
	}

	start := (page - 1) * size
	if start > len(f.ledger) {
		start = len(f.ledger)
	}
	end := start + size
	if end > len(f.ledger) {
		end = len(f.ledger)
	}

	totalPages := (len(f.ledger) + size - 1) / size
	body := map[string]any{"transactions": f.ledger[start:end]}
	if !f.omitTotalPages {
		body["metaData"] = map[string]any{
			"pageData": map[string]any{
				"pageNumber": page, "pageSize": size, "totalPages": totalPages,
			},
		}
	}
	_ = json.NewEncoder(w).Encode(body)
}

func igDeal(ref string, day int, size string, pnl string) map[string]any {
	return map[string]any{
		"transactionType": "DEAL",
		"cashTransaction": false,
		"reference":       ref,
		"instrumentName":  "EUR/USD",
		"size":            size,
		"openLevel":       "1.1000",
		"closeLevel":      "1.1050",
		"profitAndLoss":   pnl,
		"currency":        "EUR",
		"dateUtc":         fmt.Sprintf("2026-07-%02dT10:00:00", day),
	}
}

func igCash(kind string, day int, pnl string) map[string]any {
	return map[string]any{
		"transactionType": kind,
		"cashTransaction": true,
		"reference":       kind + strconv.Itoa(day),
		"size":            "0",
		"openLevel":       "0",
		"closeLevel":      "0",
		"profitAndLoss":   pnl,
		"currency":        "EUR",
		"dateUtc":         fmt.Sprintf("2026-07-%02dT09:00:00", day),
	}
}

func newIGFake(t *testing.T, f *igFakeServer) *IG {
	t.Helper()
	f.t = t
	if f.balance == nil {
		f.balance = map[string]any{"accountId": "ACC1", "currency": "EUR",
			"balance": map[string]any{"balance": 10000.0, "profitLoss": 100.0, "available": 9000.0}}
	}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	ig := NewIG(&Credentials{APIKey: "key", APISecret: "pw", Passphrase: "user"}, true)
	ig.baseURL = srv.URL
	return ig
}

// A full daily-sync sequence against a realistic ledger: every call the
// scheduler makes, in order, on one connector instance.
func TestIGEndToEndDailySyncSequence(t *testing.T) {
	fake := &igFakeServer{ledger: []map[string]any{
		igCash("DEPO", 1, "E5000.00"),
		igDeal("D1", 2, "+2", "E150.00"),
		igDeal("D2", 3, "-1", "E-75.50"),
		igCash("DIVIDEND", 4, "E12.00"),
		igCash("WITH", 5, "E-1000.00"),
		igDeal("D3", 6, "+3", "E220.25"),
		igCash("CASHIN", 7, "E250.00"),
	}}
	ig := newIGFake(t, fake)
	ctx := context.Background()

	if err := ig.TestConnection(ctx); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}

	isPaper, err := ig.DetectIsPaper(ctx)
	if err != nil {
		t.Fatalf("DetectIsPaper: %v", err)
	}
	if !isPaper {
		t.Fatal("demo connector must report paper")
	}

	bal, err := ig.GetBalance(ctx)
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 10100 {
		t.Fatalf("Equity = %v, want 10100 (balance + profitLoss)", bal.Equity)
	}
	if bal.Currency != "EUR" {
		t.Fatalf("Currency = %q, want EUR", bal.Currency)
	}

	positions, err := ig.GetPositions(ctx)
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(positions) != 1 || positions[0].Side != "long" {
		t.Fatalf("positions = %+v, want one long", positions)
	}

	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)

	trades, err := ig.GetTrades(ctx, start, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 3 {
		t.Fatalf("got %d trades, want 3 (cash lines excluded): %+v", len(trades), trades)
	}
	var realized float64
	for _, tr := range trades {
		realized += tr.RealizedPnL
	}
	if diff := realized - 294.75; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("realized P&L = %v, want 294.75", realized)
	}

	flows, err := ig.GetCashflows(ctx, start)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 3 {
		t.Fatalf("got %d cashflows, want 3 (DEPO/WITH/CASHIN; dividend excluded): %+v", len(flows), flows)
	}
	var net float64
	for _, cf := range flows {
		net += cf.Amount
	}
	if diff := net - 4250; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("net capital = %v, want 4250 (5000 - 1000 + 250)", net)
	}

	// The dividend is P&L, not capital: it must not have leaked into the flows.
	for _, cf := range flows {
		if cf.Amount == 12 {
			t.Fatal("dividend booked as a deposit — phantom capital inflow")
		}
	}

	if got := fake.logins.Load(); got != 1 {
		t.Fatalf("logins = %d, want 1 across the whole sequence", got)
	}
}

// A ledger larger than one page must come back whole. Silent truncation here
// drops deposits, and a missing deposit reads as return: the exact failure that
// craters TWR.
func TestIGEndToEndPaginatesWholeLedger(t *testing.T) {
	var ledger []map[string]any
	for n := range igTransactionPageSize + 37 {
		ledger = append(ledger, igDeal(fmt.Sprintf("D%d", n), 1+(n%28), "+1", "E1.00"))
	}
	ledger = append(ledger, igCash("DEPO", 28, "E9999.00"))

	fake := &igFakeServer{ledger: ledger}
	ig := newIGFake(t, fake)

	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	trades, err := ig.GetTrades(context.Background(), start, start.AddDate(0, 1, 0))
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != igTransactionPageSize+37 {
		t.Fatalf("got %d trades, want %d — ledger truncated", len(trades), igTransactionPageSize+37)
	}

	flows, err := ig.GetCashflows(context.Background(), start)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 1 || flows[0].Amount != 9999 {
		t.Fatalf("deposit on the last page was lost: %+v", flows)
	}
}

// Same ledger, but the upstream reports no page count. The walk must not stop
// at the first page just because the count is absent.
func TestIGEndToEndPaginatesWithoutTotalPages(t *testing.T) {
	var ledger []map[string]any
	for n := range igTransactionPageSize + 15 {
		ledger = append(ledger, igDeal(fmt.Sprintf("D%d", n), 1+(n%28), "+1", "E1.00"))
	}
	ledger = append(ledger, igCash("DEPO", 28, "E4242.00"))

	fake := &igFakeServer{ledger: ledger, omitTotalPages: true}
	ig := newIGFake(t, fake)

	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	flows, err := ig.GetCashflows(context.Background(), start)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 1 || flows[0].Amount != 4242 {
		t.Fatalf("deposit beyond page 1 lost when totalPages is absent: %+v", flows)
	}
}

// The from/to params carry no zone designator. An upstream reading them as
// account-local hands back lines from outside the window, and a deposit that
// leaks in was already booked by an earlier sync — counting it twice is the
// phantom capital inflow that craters TWR.
func TestIGEndToEndDropsLinesOutsideWindow(t *testing.T) {
	fake := &igFakeServer{ledger: []map[string]any{
		igCash("DEPO", 1, "E5000.00"),   // before the window
		igDeal("D0", 2, "+1", "E10.00"), // before the window
		igCash("DEPO", 10, "E750.00"),
		igDeal("D1", 11, "+2", "E99.00"),
		igCash("DEPO", 25, "E123.00"), // after the window
		igDeal("D2", 26, "+1", "E5.00"),
	}}
	ig := newIGFake(t, fake)
	ctx := context.Background()

	start := time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)

	trades, err := ig.GetTrades(ctx, start, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 1 || trades[0].ID != "D1" {
		t.Fatalf("trades = %+v, want only D1 (out-of-window lines dropped)", trades)
	}

	flows, err := ig.GetCashflows(ctx, start)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	// GetCashflows has no upper bound, so the later deposit legitimately counts;
	// only the one predating `since` must be dropped.
	if len(flows) != 2 {
		t.Fatalf("got %d cashflows, want 2 (pre-window deposit dropped): %+v", len(flows), flows)
	}
	for _, cf := range flows {
		if cf.Amount == 5000 {
			t.Fatal("deposit from before the window was re-booked — double-counted capital")
		}
	}
}

// IG's 6h token life is a floor: a login elsewhere kills the pair early. A
// mid-sequence expiry must re-authenticate and complete, not fail the sync.
func TestIGEndToEndSurvivesSessionExpiryMidSequence(t *testing.T) {
	fake := &igFakeServer{
		expireAfter: 2,
		ledger: []map[string]any{
			igCash("DEPO", 1, "E1000.00"),
			igDeal("D1", 2, "+1", "E10.00"),
		},
	}
	ig := newIGFake(t, fake)
	ctx := context.Background()

	if _, err := ig.GetBalance(ctx); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if _, err := ig.GetPositions(ctx); err != nil {
		t.Fatalf("GetPositions after expiry: %v", err)
	}
	trades, err := ig.GetTrades(ctx, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetTrades after expiry: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("got %d trades after re-auth, want 1", len(trades))
	}
	if got := fake.logins.Load(); got < 2 {
		t.Fatalf("logins = %d, want at least 2 (re-auth happened)", got)
	}
}

// Concurrency and rejection together — the case each existing test covers only
// half of. Every caller holds the pair IG just killed; retiring the session
// blindly would let one caller wipe the pair another had just logged in for,
// sending that caller's single retry out with a dead token. It comes back as a
// bare 401, which connection create reads as bad credentials.
func TestIGEndToEndConcurrentReAuthDoesNotStampede(t *testing.T) {
	fake := &igFakeServer{
		rejectGenBelow: 2,
		ledger:         []map[string]any{igCash("DEPO", 1, "E1000.00")},
	}
	ig := newIGFake(t, fake)

	errs := make(chan error, 8)
	done := make(chan struct{})
	for range 6 {
		go func() {
			defer func() { done <- struct{}{} }()
			if _, err := ig.GetBalance(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	for range 6 {
		<-done
	}
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent caller saw a rejected session as a hard failure: %v", err)
	}

	// One login to open, one to replace the pair IG rejected. More means callers
	// are wiping each other's fresh sessions and burning IG's login allowance.
	if got := fake.logins.Load(); got != 2 {
		t.Fatalf("logins = %d, want 2 (initial + one shared re-auth)", got)
	}
}

// The scheduler drives balance, positions and history off one cached connector.
// A cold connection opens concurrently, and IG meters logins per account.
func TestIGEndToEndConcurrentColdStart(t *testing.T) {
	fake := &igFakeServer{ledger: []map[string]any{igCash("DEPO", 1, "E1000.00")}}
	ig := newIGFake(t, fake)

	errs := make(chan error, 12)
	done := make(chan struct{})
	for range 4 {
		go func() {
			defer func() { done <- struct{}{} }()
			ctx := context.Background()
			if _, err := ig.GetBalance(ctx); err != nil {
				errs <- err
			}
			if _, err := ig.GetPositions(ctx); err != nil {
				errs <- err
			}
			if _, err := ig.GetCashflows(ctx, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)); err != nil {
				errs <- err
			}
		}()
	}
	for range 4 {
		<-done
	}
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent sync call failed: %v", err)
	}

	if got := fake.logins.Load(); got != 1 {
		t.Fatalf("logins = %d, want 1 — concurrent cold start must not stampede IG's login allowance", got)
	}
}
