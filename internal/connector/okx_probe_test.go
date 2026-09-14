package connector

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// okxFakeRouter answers per path, which the probe needs: it reads the balance
// and the account config in one call.
type okxFakeRouter struct {
	srv    *httptest.Server
	hits   int32
	byPath map[string]string
}

func newOKXFakeRouter(t *testing.T, byPath map[string]string) *okxFakeRouter {
	t.Helper()
	f := &okxFakeRouter{byPath: byPath}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.hits, 1)
		if r.Header.Get("OK-ACCESS-KEY") == "" || r.Header.Get("OK-ACCESS-SIGN") == "" {
			t.Errorf("request to %s is not signed", r.URL.Path)
		}
		body, ok := f.byPath[r.URL.Path]
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"code":"50011","msg":"unknown path"}`)
			return
		}
		io.WriteString(w, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *okxFakeRouter) connector() *OKX {
	creds := &Credentials{Exchange: "okx", APIKey: "key", APISecret: "secret", Passphrase: "pass"}
	return newOKXWithHosts(creds, &http.Client{Timeout: 5 * time.Second}, []string{f.srv.URL})
}

const okxProbePortfolioBalance = `{"code":"0","data":[{` +
	`"totalEq":"22077.42","isoEq":"0","adjEq":"21880.10","availEq":"20950.33","ordFroz":"0",` +
	`"imr":"1214.88","mmr":"402.51","mgnRatio":"54.36","upl":"-97.34","uTime":"1757548800000","details":[` +
	`{"ccy":"BTC","eq":"0.1801","eqUsd":"19056.32","cashBal":"0.1801","availBal":"0.1801","availEq":"0.1801","frozenBal":"0","isoEq":"0","upl":"0","imr":"","mmr":""},` +
	`{"ccy":"USDC","eq":"3021.10","eqUsd":"3021.10","cashBal":"3021.10","availBal":"3021.10","availEq":"3021.10","frozenBal":"0","isoEq":"0","upl":"0","imr":"","mmr":""}` +
	`]}]}`

// The probe's whole job is to let an operator see the venue's own figures next
// to the ones the enclave computed, so a disagreement can be settled without
// anybody handling the account's API key.
func TestOKXProbeBalance_ReportsRawFieldsBesideTheDerivedBalance(t *testing.T) {
	router := newOKXFakeRouter(t, map[string]string{
		"/api/v5/account/balance": okxProbePortfolioBalance,
		"/api/v5/account/config":  `{"code":"0","data":[{"acctLv":"4","uid":"1234"}]}`,
	})
	okx := router.connector()

	probe, err := okx.ProbeBalance(context.Background())
	if err != nil {
		t.Fatalf("ProbeBalance: %v", err)
	}

	if probe.Exchange != "okx" {
		t.Fatalf("exchange = %q, want okx", probe.Exchange)
	}
	if !strings.Contains(probe.AccountMode, "acctLv 4") {
		t.Fatalf("account mode = %q, want the venue's acctLv 4", probe.AccountMode)
	}
	if probe.MarginBasis != okxBasisAdjEqMinusIMR {
		t.Fatalf("margin basis = %q, want %q", probe.MarginBasis, okxBasisAdjEqMinusIMR)
	}

	// Raw, verbatim, strings — an inapplicable field must stay empty rather
	// than arrive as a zero somebody reads as measured.
	for field, want := range map[string]string{
		"totalEq": "22077.42",
		"adjEq":   "21880.10",
		"availEq": "20950.33",
		"imr":     "1214.88",
		"upl":     "-97.34",
	} {
		if got := probe.Account[field]; got != want {
			t.Errorf("account[%s] = %q, want %q", field, got, want)
		}
	}
	if len(probe.Currencies) != 2 {
		t.Fatalf("got %d currency lines, want 2", len(probe.Currencies))
	}
	if probe.Currencies[0]["ccy"] != "BTC" || probe.Currencies[0]["imr"] != "" {
		t.Errorf("first currency line = %v, want BTC with an empty imr", probe.Currencies[0])
	}

	if probe.Derived == nil {
		t.Fatal("probe carries no derived balance — the comparison is the point")
	}
	assertNear(t, "derived equity", probe.Derived.Equity, 22077.42)
	assertNear(t, "derived available", probe.Derived.Available, 21880.10-1214.88)
	assertNear(t, "derived unrealized", probe.Derived.UnrealizedPnL, -97.34)
}

// The account mode is a nicety; the balance is the answer. A key that cannot
// read /account/config still gets a probe, with the reason attached.
func TestOKXProbeBalance_SurvivesAnUnreadableAccountConfig(t *testing.T) {
	router := newOKXFakeRouter(t, map[string]string{
		"/api/v5/account/balance": okxProbePortfolioBalance,
	})
	okx := router.connector()

	probe, err := okx.ProbeBalance(context.Background())
	if err != nil {
		t.Fatalf("ProbeBalance: %v", err)
	}
	if probe.Derived == nil || probe.Derived.Equity == 0 {
		t.Fatal("no balance returned when only the mode lookup failed")
	}
	if !strings.Contains(probe.AccountMode, "inferred") {
		t.Fatalf("account mode = %q, want the inferred mode", probe.AccountMode)
	}
	if len(probe.Notes) == 0 {
		t.Fatal("the mode lookup failed silently — a probe that hides a gap is worse than none")
	}
}

// acctLv is fetched once and kept: a probe run twice against the same
// connector must not spend a second config request.
func TestOKXProbeBalance_CachesTheAccountMode(t *testing.T) {
	configHits := int32(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v5/account/config" {
			atomic.AddInt32(&configHits, 1)
			io.WriteString(w, `{"code":"0","data":[{"acctLv":"3"}]}`)
			return
		}
		io.WriteString(w, okxProbePortfolioBalance)
	}))
	t.Cleanup(srv.Close)

	creds := &Credentials{Exchange: "okx", APIKey: "key", APISecret: "secret", Passphrase: "pass"}
	okx := newOKXWithHosts(creds, &http.Client{Timeout: 5 * time.Second}, []string{srv.URL})

	for i := 0; i < 3; i++ {
		if _, err := okx.ProbeBalance(context.Background()); err != nil {
			t.Fatalf("ProbeBalance #%d: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&configHits); got != 1 {
		t.Fatalf("account/config requested %d times, want 1", got)
	}
}

// OKX implements the optional prober; the compiler is the one that should say
// so, not a runtime 501 discovered during an incident.
var _ BalanceProber = (*OKX)(nil)
