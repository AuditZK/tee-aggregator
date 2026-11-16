package connector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	kucoinBodyAccountsOK = `{"code":"200000","data":[{"currency":"USDT","type":"trade","balance":"250","available":"250","holds":"0"}]}`
	kucoinBodyKeyUnknown = `{"code":"400003","msg":"The API key does not exist or site mismatch."}`
	kucoinBodyPassphrase = `{"code":"400004","msg":"The API passphrase is invalid."}`
	kucoinBodyLegacyIP   = `{"code":"400006","msg":"Invalid request ip, the current clientIp is:203.0.113.7"}`
	kucoinBodyNoPerm     = `{"code":"400007","msg":"The API key does not have permission to access this resource."}`
)

type kucoinFakeSite struct {
	srv  *httptest.Server
	hits int32
}

func (f *kucoinFakeSite) requests() int { return int(atomic.LoadInt32(&f.hits)) }

func newKuCoinFakeSite(t *testing.T, status int, body string) *kucoinFakeSite {
	t.Helper()
	f := &kucoinFakeSite{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.hits, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func newKuCoinAcross(primary *kucoinFakeSite, fallbacks ...*kucoinFakeSite) *KuCoin {
	k := NewKuCoin(&Credentials{Exchange: "kucoin", APIKey: "key", APISecret: "secret", Passphrase: "pass"})
	k.base.BaseURL = primary.srv.URL
	k.fallbacks = nil
	for _, f := range fallbacks {
		k.fallbacks = append(k.fallbacks, f.srv.URL)
	}
	return k
}

func TestKuCoinSignsTheDocumentedHeaders(t *testing.T) {
	var got http.Header
	var gotURI string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		gotURI = r.URL.RequestURI()
		io.WriteString(w, kucoinBodyAccountsOK)
	}))
	defer srv.Close()

	k := NewKuCoin(&Credentials{APIKey: "key", APISecret: "secret", Passphrase: "pass"})
	k.base.BaseURL = srv.URL
	if err := k.TestConnection(context.Background()); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}

	ts := got.Get("KC-API-TIMESTAMP")
	if want := signHMACBase64("secret", ts+"GET"+gotURI); got.Get("KC-API-SIGN") != want {
		t.Errorf("KC-API-SIGN does not cover timestamp+method+path+query (%s)", gotURI)
	}
	if want := signHMACBase64("secret", "pass"); got.Get("KC-API-PASSPHRASE") != want {
		t.Error("KC-API-PASSPHRASE must be the passphrase HMAC-signed with the secret")
	}
	if got.Get("KC-API-KEY") != "key" || got.Get("KC-API-KEY-VERSION") != "2" {
		t.Errorf("key headers = %q / version %q", got.Get("KC-API-KEY"), got.Get("KC-API-KEY-VERSION"))
	}
}

func TestKuCoinEUKeyIsFoundOnTheEUSite(t *testing.T) {
	global := newKuCoinFakeSite(t, http.StatusUnauthorized, kucoinBodyKeyUnknown)
	eu := newKuCoinFakeSite(t, http.StatusOK, kucoinBodyAccountsOK)
	k := newKuCoinAcross(global, eu)

	if err := k.TestConnection(context.Background()); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if global.requests() != 1 || eu.requests() != 1 {
		t.Fatalf("probe: global=%d eu=%d, want 1/1", global.requests(), eu.requests())
	}

	bal, err := k.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 250 {
		t.Fatalf("equity = %v, want 250", bal.Equity)
	}
	if global.requests() != 1 || eu.requests() != 2 {
		t.Fatalf("pin: global=%d eu=%d, want 1/2", global.requests(), eu.requests())
	}
}

func TestKuCoinGlobalKeyNeverLeavesGlobal(t *testing.T) {
	global := newKuCoinFakeSite(t, http.StatusOK, kucoinBodyAccountsOK)
	eu := newKuCoinFakeSite(t, http.StatusUnauthorized, kucoinBodyKeyUnknown)
	k := newKuCoinAcross(global, eu)

	if err := k.TestConnection(context.Background()); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if eu.requests() != 0 {
		t.Fatalf("eu probed %d times for a global key, want 0", eu.requests())
	}
}

func TestKuCoinKeyUnknownOnEverySite(t *testing.T) {
	global := newKuCoinFakeSite(t, http.StatusUnauthorized, kucoinBodyKeyUnknown)
	eu := newKuCoinFakeSite(t, http.StatusUnauthorized, kucoinBodyKeyUnknown)
	k := newKuCoinAcross(global, eu)

	err := k.TestConnection(context.Background())
	if err == nil {
		t.Fatal("expected an error when no site knows the key")
	}
	for _, want := range []string{"every KuCoin site", "site mismatch", "code 400003"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err.Error(), want)
		}
	}
	if ClassifyConnectFailure(err) != ConnectFailureCredentials || IsIPRestriction(err) {
		t.Errorf("an unknown key must stay a credential failure: %q", err.Error())
	}
}

func TestKuCoinRefusalsThatProveTheKeyExistStopTheProbe(t *testing.T) {
	cases := []struct {
		name, body, want string
		ip               bool
	}{
		{"passphrase", kucoinBodyPassphrase, "not the login password", false},
		{"permission", kucoinBodyNoPerm, "General permission", false},
		{"legacy ip text", kucoinBodyLegacyIP, "code 400006", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			global := newKuCoinFakeSite(t, http.StatusUnauthorized, tc.body)
			eu := newKuCoinFakeSite(t, http.StatusOK, kucoinBodyAccountsOK)
			k := newKuCoinAcross(global, eu)

			err := k.TestConnection(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
			if IsIPRestriction(err) != tc.ip {
				t.Errorf("IsIPRestriction = %v, want %v", !tc.ip, tc.ip)
			}
			if !tc.ip && ClassifyConnectFailure(err) != ConnectFailureCredentials {
				t.Errorf("%q must stay a credential failure", err.Error())
			}
			if eu.requests() != 0 {
				t.Fatalf("eu probed %d times, want 0", eu.requests())
			}
		})
	}
}

func TestKuCoinThrottleStaysTransient(t *testing.T) {
	site := newKuCoinFakeSite(t, http.StatusTooManyRequests, `{"code":"429000","msg":"Too many requests."}`)
	k := newKuCoinAcross(site)
	err := k.TestConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "code 429000") {
		t.Fatalf("err = %v, want the 429000 refusal", err)
	}
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("a 429 must stay transient: %q", err.Error())
	}
}
