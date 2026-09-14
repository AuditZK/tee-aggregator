package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/service"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// fakeProber stands in for the sync service, which in production needs a
// DEK-unwrapped enclave behind it.
type fakeProber struct {
	probe *connector.BalanceProbe
	err   error

	gotUser, gotExchange, gotLabel string
}

func (f *fakeProber) ProbeBalance(_ context.Context, userUID, exchange, label string) (*connector.BalanceProbe, error) {
	f.gotUser, f.gotExchange, f.gotLabel = userUID, exchange, label
	return f.probe, f.err
}

// okxProbeFromVenue builds the probe the OKX connector would return, by
// actually fetching the venue payload over HTTP — the shape of the customer's
// portfolio-margin account, served by an httptest stand-in for OKX.
func okxProbeFromVenue(t *testing.T) *connector.BalanceProbe {
	t.Helper()
	const body = `{"code":"0","data":[{"totalEq":"22077.42","adjEq":"21880.10","availEq":"20950.33",` +
		`"imr":"1214.88","mmr":"402.51","upl":"-97.34","details":[` +
		`{"ccy":"USDC","eq":"3021.10","eqUsd":"3021.10","availBal":"3021.10","availEq":"3021.10","upl":"0"}]}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/api/v5/account/balance")
	if err != nil {
		t.Fatalf("fetch venue payload: %v", err)
	}
	defer resp.Body.Close()
	var venue struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&venue); err != nil {
		t.Fatalf("decode venue payload: %v", err)
	}

	account := map[string]string{}
	for _, k := range []string{"totalEq", "adjEq", "availEq", "imr", "mmr", "upl"} {
		account[k] = fmt.Sprint(venue.Data[0][k])
	}
	return &connector.BalanceProbe{
		Exchange:    "okx",
		AccountMode: "acctLv 4 (portfolio margin mode)",
		MarginBasis: "account.adjEq - account.imr",
		Account:     account,
		Currencies: []map[string]string{
			{"ccy": "USDC", "availBal": "3021.10", "availEq": "3021.10", "upl": "0"},
		},
		Derived: &connector.Balance{
			Available:     21880.10 - 1214.88,
			Equity:        22077.42,
			UnrealizedPnL: -97.34,
			Currency:      "USDT",
		},
	}
}

func observedServer(prober balanceProbeRunner) (*Server, *observer.ObservedLogs) {
	core, logs := observer.New(zap.DebugLevel)
	return &Server{logger: zap.New(core), probeSvc: prober}, logs
}

func postProbe(s *Server, qs string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/balance-probe?"+qs, nil)
	s.handleAdminBalanceProbe(rec, req)
	return rec
}

// The figures are the answer and they go to the loopback caller. They must not
// also go to a log: a log is the one artefact of this endpoint that outlives
// the call and travels.
func TestHandleAdminBalanceProbe_ServesTheFiguresAndLogsNone(t *testing.T) {
	prober := &fakeProber{probe: okxProbeFromVenue(t)}
	s, logs := observedServer(prober)

	rec := postProbe(s, "user_uid=user_abc1234567890&exchange=okx&label=RAVCA_UW")
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if prober.gotUser != "user_abc1234567890" || prober.gotExchange != "okx" || prober.gotLabel != "RAVCA_UW" {
		t.Fatalf("probe called with (%q, %q, %q)", prober.gotUser, prober.gotExchange, prober.gotLabel)
	}

	var got struct {
		Success bool                    `json:"success"`
		Probe   *connector.BalanceProbe `json:"probe"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if !got.Success || got.Probe == nil {
		t.Fatalf("response carries no probe: %s", rec.Body.String())
	}
	if got.Probe.Account["adjEq"] != "21880.10" || got.Probe.Account["imr"] != "1214.88" {
		t.Errorf("raw account fields missing from the response: %v", got.Probe.Account)
	}
	if got.Probe.MarginBasis != "account.adjEq - account.imr" {
		t.Errorf("margin basis = %q", got.Probe.MarginBasis)
	}
	if got.Probe.Derived == nil || got.Probe.Derived.Available >= got.Probe.Derived.Equity {
		t.Errorf("derived balance did not come through: %+v", got.Probe.Derived)
	}

	entries := logs.All()
	if len(entries) != 1 || entries[0].Message != "balance probe served" {
		t.Fatalf("log entries = %v, want exactly one \"balance probe served\"", entries)
	}
	for _, amount := range []string{"22077", "21880", "1214", "97.34", "3021", "RAVCA_UW", "user_abc1234567890"} {
		if strings.Contains(fmt.Sprint(entries[0].ContextMap()), amount) ||
			strings.Contains(entries[0].Message, amount) {
			t.Errorf("log line leaks %q: %v %v", amount, entries[0].Message, entries[0].ContextMap())
		}
	}
}

// A venue nobody wired for probing must say so. An empty 200 would read like
// an account holding nothing.
func TestHandleAdminBalanceProbe_UnsupportedConnectorIs501(t *testing.T) {
	s, _ := observedServer(&fakeProber{
		err: fmt.Errorf("bitget: %w", service.ErrBalanceProbeUnsupported),
	})

	rec := postProbe(s, "user_uid=user_abc1234567890&exchange=bitget")
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("got %d, want 501: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleAdminBalanceProbe_FailureIsA500WithNoFiguresLogged(t *testing.T) {
	s, logs := observedServer(&fakeProber{err: errors.New("decrypt credentials: no such connection")})

	rec := postProbe(s, "user_uid=user_abc1234567890&exchange=okx")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("got %d, want 500: %s", rec.Code, rec.Body.String())
	}
	if logs.FilterMessage("balance probe served").Len() != 0 {
		t.Error("a failed probe logged itself as served")
	}
}

// SEC-10: trust-boundary inputs are validated like every other REST entrypoint.
func TestHandleAdminBalanceProbe_Validation(t *testing.T) {
	s, _ := observedServer(&fakeProber{probe: okxProbeFromVenue(t)})

	bad := []struct{ name, qs string }{
		{"missing user_uid", "exchange=okx"},
		{"bad user_uid", "user_uid=bad%20uid%21&exchange=okx"},
		{"missing exchange", "user_uid=user_abc1234567890"},
		{"bad exchange", "user_uid=user_abc1234567890&exchange=BAD%2FEX"},
		{"label with delimiter", "user_uid=user_abc1234567890&exchange=okx&label=a%2Fb"},
	}
	for _, tc := range bad {
		if got := postProbe(s, tc.qs).Code; got != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", tc.name, got)
		}
	}

	rec := httptest.NewRecorder()
	s.handleAdminBalanceProbe(rec, httptest.NewRequest(http.MethodGet, "/api/v1/admin/balance-probe?user_uid=user_abc1234567890&exchange=okx", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET got %d, want 405", rec.Code)
	}
}

// Without a sync service there is nothing to decrypt credentials with; say so
// rather than answering as if the account were empty.
func TestHandleAdminBalanceProbe_NoServiceIs503(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	s := &Server{logger: zap.New(core)}

	if got := postProbe(s, "user_uid=user_abc1234567890&exchange=okx").Code; got != http.StatusServiceUnavailable {
		t.Errorf("got %d, want 503", got)
	}
}

// SEC-001: the probe returns a customer's balance, so it is loopback-only like
// every other admin endpoint.
func TestHandleAdminBalanceProbe_IsLoopbackOnly(t *testing.T) {
	s, _ := observedServer(&fakeProber{probe: okxProbeFromVenue(t)})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/balance-probe?user_uid=user_abc1234567890&exchange=okx", nil)
	req.RemoteAddr = "203.0.113.7:51234"
	s.localhostOnly(s.handleAdminBalanceProbe)(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("a remote peer got %d, want 403", rec.Code)
	}
}
