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

// OKX answers HTTP 401 with a JSON envelope for every auth failure; these are
// the bodies observed live on 2026-08-29 against www / eea / us.okx.com.
const (
	okxBodyBalanceOK  = `{"code":"0","msg":"","data":[{"totalEq":"1234.5","details":[{"ccy":"USDT","eq":"1234.5","availBal":"1000","upl":"0"}]}]}`
	okxBodyKeyUnknown = `{"msg":"API key doesn't exist","code":"50119"}`
	okxBodyBadSign    = `{"msg":"Invalid Sign","code":"50113"}`
	okxBodyBadIP      = `{"msg":"Invalid IP","code":"50110"}`
)

type okxFakeRegion struct {
	srv  *httptest.Server
	hits int32
}

func (f *okxFakeRegion) requests() int { return int(atomic.LoadInt32(&f.hits)) }

func newOKXFakeRegion(t *testing.T, status int, body string) *okxFakeRegion {
	t.Helper()
	f := &okxFakeRegion{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.hits, 1)
		if r.Header.Get("OK-ACCESS-KEY") == "" || r.Header.Get("OK-ACCESS-SIGN") == "" {
			t.Errorf("request to %s is not signed", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func newOKXAcross(regions ...*okxFakeRegion) *OKX {
	hosts := make([]string, 0, len(regions))
	for _, r := range regions {
		hosts = append(hosts, r.srv.URL)
	}
	creds := &Credentials{Exchange: "okx", APIKey: "key", APISecret: "secret", Passphrase: "pass"}
	return newOKXWithHosts(creds, &http.Client{Timeout: 5 * time.Second}, hosts)
}

// The bug this guards: an OKX Europe key (issued on my.okx.com) is unknown to
// the global domain, and the connector used to stop there and report bad
// credentials.
func TestOKXRegionProbePinsTheDomainThatKnowsTheKey(t *testing.T) {
	global := newOKXFakeRegion(t, http.StatusUnauthorized, okxBodyKeyUnknown)
	eea := newOKXFakeRegion(t, http.StatusOK, okxBodyBalanceOK)
	us := newOKXFakeRegion(t, http.StatusUnauthorized, okxBodyKeyUnknown)
	o := newOKXAcross(global, eea, us)

	if err := o.TestConnection(context.Background()); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if global.requests() != 1 || eea.requests() != 1 || us.requests() != 0 {
		t.Fatalf("probe order broken: global=%d eea=%d us=%d, want 1/1/0",
			global.requests(), eea.requests(), us.requests())
	}

	// Once recognised, the region is pinned: no more probing on later calls.
	bal, err := o.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 1234.5 || bal.Available != 1000 {
		t.Fatalf("balance = %+v, want equity 1234.5 / available 1000", bal)
	}
	// Two more on the pinned region, one per wallet, and none anywhere else.
	if global.requests() != 1 || eea.requests() != 3 || us.requests() != 0 {
		t.Fatalf("pin broken: global=%d eea=%d us=%d, want 1/3/0",
			global.requests(), eea.requests(), us.requests())
	}
}

func TestOKXRegionProbeGlobalKeyNeverLeavesGlobal(t *testing.T) {
	global := newOKXFakeRegion(t, http.StatusOK, okxBodyBalanceOK)
	eea := newOKXFakeRegion(t, http.StatusUnauthorized, okxBodyKeyUnknown)
	o := newOKXAcross(global, eea)

	if err := o.TestConnection(context.Background()); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if eea.requests() != 0 {
		t.Fatalf("eea probed %d times for a global key, want 0", eea.requests())
	}
}

func TestOKXRegionProbeReportsWhenNoRegionKnowsTheKey(t *testing.T) {
	global := newOKXFakeRegion(t, http.StatusUnauthorized, okxBodyKeyUnknown)
	eea := newOKXFakeRegion(t, http.StatusUnauthorized, okxBodyKeyUnknown)
	us := newOKXFakeRegion(t, http.StatusUnauthorized, okxBodyKeyUnknown)
	o := newOKXAcross(global, eea, us)

	err := o.TestConnection(context.Background())
	if err == nil {
		t.Fatal("expected an error when every region refuses the key")
	}
	if global.requests() != 1 || eea.requests() != 1 || us.requests() != 1 {
		t.Fatalf("each region must be asked exactly once: global=%d eea=%d us=%d",
			global.requests(), eea.requests(), us.requests())
	}
	// The venue text survives so the service layer still files it under
	// "invalid credentials", and the probe leaves its trace for the logs.
	for _, want := range []string{"API key doesn't exist", "every OKX region"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err.Error(), want)
		}
	}
}

// A signature or passphrase error proves the key exists on the domain that
// answered: probing on would replace a precise error with "doesn't exist".
func TestOKXRegionProbeStopsOnSignatureError(t *testing.T) {
	global := newOKXFakeRegion(t, http.StatusUnauthorized, okxBodyBadSign)
	eea := newOKXFakeRegion(t, http.StatusOK, okxBodyBalanceOK)
	o := newOKXAcross(global, eea)

	err := o.TestConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Invalid Sign") {
		t.Fatalf("err = %v, want the venue's Invalid Sign", err)
	}
	if eea.requests() != 0 {
		t.Fatalf("eea probed %d times after a signature error, want 0", eea.requests())
	}
}

// An IP rejection must keep its shape: the service layer routes it to the
// whitelist remediation, not to "regenerate your key".
func TestOKXRegionProbeStopsOnIPRestriction(t *testing.T) {
	global := newOKXFakeRegion(t, http.StatusUnauthorized, okxBodyBadIP)
	eea := newOKXFakeRegion(t, http.StatusOK, okxBodyBalanceOK)
	o := newOKXAcross(global, eea)

	err := o.TestConnection(context.Background())
	if err == nil {
		t.Fatal("expected the IP rejection to surface")
	}
	if !IsIPRestriction(err) {
		t.Fatalf("IsIPRestriction(%q) = false, want true", err.Error())
	}
	if eea.requests() != 0 {
		t.Fatalf("eea probed %d times after an IP rejection, want 0", eea.requests())
	}
}

// A non-zero code inside an HTTP 200 is the other envelope OKX uses; the
// probe must read it the same way.
func TestOKXRegionProbeReadsCodeFromA200Envelope(t *testing.T) {
	global := newOKXFakeRegion(t, http.StatusOK, okxBodyKeyUnknown)
	eea := newOKXFakeRegion(t, http.StatusOK, okxBodyBalanceOK)
	o := newOKXAcross(global, eea)

	if err := o.TestConnection(context.Background()); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if global.requests() != 1 || eea.requests() != 1 {
		t.Fatalf("global=%d eea=%d, want 1/1", global.requests(), eea.requests())
	}
}

func TestOKXKeyUnknownHere(t *testing.T) {
	cases := []struct {
		name string
		body string
		err  string
		want bool
	}{
		{"no error", okxBodyBalanceOK, "", false},
		{"50119 in body", okxBodyKeyUnknown, "HTTP 401: " + okxBodyKeyUnknown, true},
		{"50111 in body", `{"msg":"Invalid OK-ACCESS-KEY","code":"50111"}`, "HTTP 401", true},
		{"50113 is not a region miss", okxBodyBadSign, "HTTP 401: " + okxBodyBadSign, false},
		{"body lost, code in error text", "", `HTTP 401: {"msg":"API key doesn't exist","code":"50119"}`, true},
		{"network error", "", "dial tcp: connection refused", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.err != "" {
				err = errorString(tc.err)
			}
			if got := okxKeyUnknownHere([]byte(tc.body), err); got != tc.want {
				t.Fatalf("okxKeyUnknownHere = %v, want %v", got, tc.want)
			}
		})
	}
}
