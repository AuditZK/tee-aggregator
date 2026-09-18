package connector

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// okxBillsServer answers the bills, bills-archive and tickers endpoints from
// canned bodies, recording which paths were hit.
type okxBillsServer struct {
	srv    *httptest.Server
	mu     sync.Mutex
	hits   []string
	bodies map[string]string
	status map[string]int
}

func newOKXBillsServer(t *testing.T, bodies map[string]string, status map[string]int) *okxBillsServer {
	t.Helper()
	s := &okxBillsServer{bodies: bodies, status: status}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.hits = append(s.hits, r.URL.Path)
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if code, ok := s.status[r.URL.Path]; ok {
			w.WriteHeader(code)
			io.WriteString(w, `{"code":"50013","msg":"system busy"}`)
			return
		}
		body, ok := s.bodies[r.URL.Path]
		if !ok {
			io.WriteString(w, `{"code":"0","data":[]}`)
			return
		}
		io.WriteString(w, body)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *okxBillsServer) connector() *OKX {
	creds := &Credentials{Exchange: "okx", APIKey: "key", APISecret: "secret", Passphrase: "pass"}
	return newOKXWithHosts(creds, &http.Client{Timeout: 5 * time.Second}, []string{s.srv.URL})
}

func (s *okxBillsServer) pathsHit() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.hits...)
}

// msAgo dates a fixture relative to the run. Absolute epochs used to work
// here because nothing checked them against the window the test asked for;
// GetCashflows now enforces that window itself, so a fixture pinned to a fixed
// calendar day falls out of "the last 24 hours" as soon as the day passes.
func msAgo(d time.Duration) string {
	return strconv.FormatInt(time.Now().Add(-d).UnixMilli(), 10)
}

func billRow(id, ts, ccy, balChg, typ, subType string) string {
	return `{"billId":"` + id + `","ts":"` + ts + `","ccy":"` + ccy + `","balChg":"` + balChg + `","type":"` + typ + `","subType":"` + subType + `"}`
}

const (
	okxBillsPath   = "/api/v5/account/bills"
	okxArchivePath = "/api/v5/account/bills-archive"
	okxTickersPath = "/api/v5/market/tickers"
)

// A transfer in books positive, a transfer out negative, and the trade and
// funding rows around them book nothing: the type-1 rule.
func TestOKXGetCashflows_TransfersInAndOut(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("b1", msAgo(5*time.Hour), "USDT", "500", "1", "11") + `,` +
			billRow("b2", msAgo(4*time.Hour), "USDT", "-120.5", "1", "12") + `,` +
			billRow("b3", msAgo(3*time.Hour), "USDT", "3.2", "2", "1") + `,` +
			billRow("b4", msAgo(2*time.Hour), "USDT", "-0.8", "8", "173") +
			`]}`,
	}, nil)

	flows, err := s.connector().GetCashflows(context.Background(), time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 2 {
		t.Fatalf("got %d flows, want 2 (trade and funding rows must not book)", len(flows))
	}
	if flows[0].Amount != 500 {
		t.Fatalf("transfer in booked %v, want 500", flows[0].Amount)
	}
	if flows[1].Amount != -120.5 {
		t.Fatalf("transfer out booked %v, want -120.5", flows[1].Amount)
	}
}

// A non-stable transfer is valued through the public tickers call, and the
// call happens only when needed.
func TestOKXGetCashflows_NonStableValuedThroughTickers(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("b1", msAgo(2*time.Hour), "BTC", "-0.5", "1", "12") +
			`]}`,
		okxTickersPath: `{"code":"0","data":[{"instId":"BTC-USDT","last":"50000"}]}`,
	}, nil)

	flows, err := s.connector().GetCashflows(context.Background(), time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 1 || flows[0].Amount != -25000 {
		t.Fatalf("flows = %+v, want one -25000 outflow", flows)
	}
}

// Found on the first live run: an OKX Europe account moves EUR, and OKX quotes
// fiat on the quote side (USDT-EUR), so the direct lookup priced nothing.
func TestOKXPriceInUSD_FiatThroughTheInversePair(t *testing.T) {
	prices := okxPriceInUSD([]string{"EUR"}, []okxTicker{
		{InstID: "BTC-USDT", Last: 50000},
		{InstID: "USDT-EUR", Last: 0.8},
	})
	if got := prices["EUR"]; got != 1.25 {
		t.Fatalf("EUR priced %v, want 1.25 from 1/USDT-EUR", got)
	}
}

// A currency with no stable pair at all is still priced through BTC before
// being given up on.
func TestOKXPriceInUSD_TriangleThroughBTC(t *testing.T) {
	prices := okxPriceInUSD([]string{"GBP", "RWUSD"}, []okxTicker{
		{InstID: "BTC-USDT", Last: 50000},
		{InstID: "BTC-GBP", Last: 40000},
	})
	if got := prices["GBP"]; got != 1.25 {
		t.Fatalf("GBP priced %v, want 1.25 from BTC-USDT/BTC-GBP", got)
	}
	if _, priced := prices["RWUSD"]; priced {
		t.Fatal("a currency with no route to USD must stay unpriced, never invented")
	}
}

// The direct pair beats the inverse and the triangle; USDT beats USDC.
func TestOKXPriceInUSD_Precedence(t *testing.T) {
	prices := okxPriceInUSD([]string{"ETH"}, []okxTicker{
		{InstID: "BTC-USDT", Last: 50000},
		{InstID: "BTC-ETH", Last: 20},
		{InstID: "ETH-USDC", Last: 2490},
		{InstID: "ETH-USDT", Last: 2500},
		{InstID: "USDT-ETH", Last: 0.0004},
	})
	if got := prices["ETH"]; got != 2500 {
		t.Fatalf("ETH priced %v, want 2500 from the direct USDT pair", got)
	}
}

func TestOKXGetCashflows_StablesOnlySkipTickers(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("b1", msAgo(2*time.Hour), "USDC", "1000", "1", "11") +
			`]}`,
	}, nil)

	if _, err := s.connector().GetCashflows(context.Background(), time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	for _, p := range s.pathsHit() {
		if p == okxTickersPath {
			t.Fatal("tickers fetched for a stables-only window")
		}
	}
}

// The RWUSD lesson: a transfer in a currency with no tradable pair must not
// be valued at zero silently. It is skipped and the gap is stated.
func TestOKXGetCashflows_UnpricedTransferWarnsInsteadOfZero(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("b1", msAgo(2*time.Hour), "RWUSD", "2000", "1", "11") +
			`]}`,
		okxTickersPath: `{"code":"0","data":[{"instId":"BTC-USDT","last":"50000"}]}`,
	}, nil)

	conn := s.connector()
	flows, err := conn.GetCashflows(context.Background(), time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 0 {
		t.Fatalf("flows = %+v, want none for an unpriceable currency", flows)
	}
	warns := conn.CapabilityWarnings()
	if len(warns) != 1 || warns[0] != "okx_transfer_unpriced:RWUSD" {
		t.Fatalf("warnings = %v, want [okx_transfer_unpriced:RWUSD]", warns)
	}
}

// An unknown bill type that moves balance is a warning, never a silent
// absorption — the rebuilder's ledger-histogram doctrine, applied live.
func TestOKXGetCashflows_UnknownTypeWarns(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("b1", msAgo(2*time.Hour), "USDT", "77", "33", "9") +
			`]}`,
	}, nil)

	conn := s.connector()
	flows, err := conn.GetCashflows(context.Background(), time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 0 {
		t.Fatalf("flows = %+v, want none for an unclassified type", flows)
	}
	warns := conn.CapabilityWarnings()
	if len(warns) != 1 || warns[0] != "okx_bill_unclassified:33/9" {
		t.Fatalf("warnings = %v, want [okx_bill_unclassified:33/9]", warns)
	}
}

// A window inside the live retention must not touch the archive; a window
// past it must, and a bill served by both endpoints books once.
func TestOKXGetCashflows_ArchiveOnlyWhenWindowNeedsIt(t *testing.T) {
	within := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[]}`,
	}, nil)
	if _, err := within.connector().GetCashflows(context.Background(), time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	for _, p := range within.pathsHit() {
		if p == okxArchivePath {
			t.Fatal("archive fetched for a 24h window")
		}
	}

	past := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("dup", msAgo(10*24*time.Hour), "USDT", "500", "1", "11") +
			`]}`,
		okxArchivePath: `{"code":"0","data":[` +
			billRow("dup", msAgo(10*24*time.Hour), "USDT", "500", "1", "11") + `,` +
			billRow("old", msAgo(15*24*time.Hour), "USDT", "900", "1", "11") +
			`]}`,
	}, nil)
	flows, err := past.connector().GetCashflows(context.Background(), time.Now().Add(-20*24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	archiveHit := false
	for _, p := range past.pathsHit() {
		if p == okxArchivePath {
			archiveHit = true
		}
	}
	if !archiveHit {
		t.Fatal("archive not fetched for a 20-day window")
	}
	if len(flows) != 2 {
		t.Fatalf("got %d flows, want 2 — the shared billId must book once", len(flows))
	}
	total := flows[0].Amount + flows[1].Amount
	if total != 1400 {
		t.Fatalf("total flow %v, want 1400", total)
	}
}

// OKX rejects some bad requests with HTTP 200 and the error code in the body.
// doRequestAt already turns that envelope into an error; this pins it for the
// ledger, where reading it as "no bills" would book every missed transfer as
// performance.
func TestOKXGetCashflows_InBodyErrorIsAnErrorNotAnEmptyLedger(t *testing.T) {
	dead := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"51000","msg":"parameter error","data":[]}`,
	}, nil)
	if _, err := dead.connector().GetCashflows(context.Background(), time.Now().Add(-24*time.Hour)); err == nil {
		t.Fatal("an in-body error on the live endpoint read as an empty ledger")
	}

	degraded := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("b1", msAgo(10*24*time.Hour), "USDT", "500", "1", "11") +
			`]}`,
		okxArchivePath: `{"code":"51000","msg":"parameter error","data":[]}`,
	}, nil)
	conn := degraded.connector()
	flows, err := conn.GetCashflows(context.Background(), time.Now().Add(-20*24*time.Hour))
	if err != nil {
		t.Fatalf("an in-body archive error must degrade, got: %v", err)
	}
	if len(flows) != 1 {
		t.Fatalf("got %d flows, want the live window's 1", len(flows))
	}
	found := false
	for _, w := range conn.CapabilityWarnings() {
		if strings.Contains(w, "archive_unavailable") {
			found = true
		}
	}
	if !found {
		t.Fatalf("warnings = %v, want the shortened window stated", conn.CapabilityWarnings())
	}
}

// The live endpoint failing is fatal; the archive failing shortens the window
// and says so.
func TestOKXGetCashflows_FailurePolicy(t *testing.T) {
	dead := newOKXBillsServer(t, nil, map[string]int{okxBillsPath: http.StatusInternalServerError})
	if _, err := dead.connector().GetCashflows(context.Background(), time.Now().Add(-24*time.Hour)); err == nil {
		t.Fatal("live bills endpoint down must fail the call")
	}

	degraded := newOKXBillsServer(t, map[string]string{
		okxBillsPath: `{"code":"0","data":[` +
			billRow("b1", msAgo(10*24*time.Hour), "USDT", "500", "1", "11") +
			`]}`,
	}, map[string]int{okxArchivePath: http.StatusInternalServerError})
	conn := degraded.connector()
	flows, err := conn.GetCashflows(context.Background(), time.Now().Add(-20*24*time.Hour))
	if err != nil {
		t.Fatalf("archive failure must degrade, got: %v", err)
	}
	if len(flows) != 1 {
		t.Fatalf("got %d flows, want the live window's 1", len(flows))
	}
	found := false
	for _, w := range conn.CapabilityWarnings() {
		if strings.Contains(w, "archive_unavailable") {
			found = true
		}
	}
	if !found {
		t.Fatalf("warnings = %v, want the shortened window stated", conn.CapabilityWarnings())
	}
}
