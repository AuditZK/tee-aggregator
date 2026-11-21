package connector

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/errsanitize"
)

// saxoTokenServer is a Saxo stand-in that enforces what makes refreshing
// dangerous: every refresh token works once. Access tokens it issued open the
// gateway, the expired one a test starts from does not. It counts refreshes in
// flight at the same time.
type saxoTokenServer struct {
	t *testing.T

	mu          sync.Mutex
	validRT     map[string]bool
	validAccess map[string]bool
	issued      int

	refreshes   atomic.Int32
	refused     atomic.Int32
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	hold        time.Duration
}

func newSaxoTokenServer(t *testing.T, access, refresh string) *saxoTokenServer {
	return &saxoTokenServer{t: t, validRT: map[string]bool{refresh: true}, validAccess: map[string]bool{access: true}}
}

func (ts *saxoTokenServer) handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/token" {
		ts.token(w, r)
		return
	}
	ts.mu.Lock()
	ok := ts.validAccess[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
	ts.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch r.URL.Path {
	case "/port/v1/clients/me":
		saxoWriteClient(w)
	case "/port/v1/balances/me":
		fmt.Fprint(w, `{"TotalValue":1000,"Currency":"EUR"}`)
	default:
		ts.t.Errorf("unexpected path %s", r.URL.Path)
	}
}

func (ts *saxoTokenServer) token(w http.ResponseWriter, r *http.Request) {
	n := ts.inFlight.Add(1)
	defer ts.inFlight.Add(-1)
	for {
		m := ts.maxInFlight.Load()
		if n <= m || ts.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	ts.refreshes.Add(1)
	time.Sleep(ts.hold)

	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(saxoTestAppKey+":"+saxoTestAppSecret))
	if r.Header.Get("Authorization") != wantAuth {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"invalid_client"}`)
		return
	}
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "refresh_token" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"invalid_request"}`)
		return
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	rt := r.PostForm.Get("refresh_token")
	if !ts.validRT[rt] {
		ts.refused.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"invalid_grant"}`)
		return
	}
	delete(ts.validRT, rt)
	ts.issued++
	access := fmt.Sprintf("synthetic-access-r%d", ts.issued)
	refresh := fmt.Sprintf("synthetic-refresh-r%d", ts.issued)
	ts.validRT[refresh] = true
	ts.validAccess[access] = true
	fmt.Fprintf(w, `{"access_token":%q,"token_type":"Bearer","expires_in":1200,"refresh_token":%q,"refresh_token_expires_in":2400}`, access, refresh)
}

func newSaxoAgainst(t *testing.T, srv *httptest.Server, access, refresh string) *Saxo {
	t.Helper()
	s := NewSaxo(&Credentials{
		APIKey:       access,
		APISecret:    refresh,
		ClientID:     saxoTestAppKey,
		ClientSecret: saxoTestAppSecret,
	}, true)
	s.baseURL = srv.URL
	s.authURL = srv.URL
	s.redirectURI = ""
	s.retryBackoff = time.Millisecond
	return s
}

// The gateway refusing the access token rotates the pair once, retries the
// request with the new access token, and exposes the rotated pair so the
// connect path stores what the connector holds (E-H4).
func TestSaxoRefreshesOnUnauthorizedAndRetries(t *testing.T) {
	ts := newSaxoTokenServer(t, "synthetic-access-current", saxoTestRefresh)
	srv := httptest.NewServer(http.HandlerFunc(ts.handler))
	t.Cleanup(srv.Close)
	s := newSaxoAgainst(t, srv, "synthetic-access-expired", saxoTestRefresh)

	bal, err := s.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 1000 {
		t.Fatalf("equity = %v", bal.Equity)
	}
	access, refresh := s.CurrentCredentials()
	if access != "synthetic-access-r1" || refresh != "synthetic-refresh-r1" {
		t.Fatalf("holding %q/%q, want the rotated pair", access, refresh)
	}
	if a, r := s.TokenLifetimes(); a != 20*time.Minute || r != 40*time.Minute {
		t.Fatalf("lifetimes = %v/%v", a, r)
	}

	// The next rotation must spend the rotated refresh token, not the first one.
	if err := s.KeepAlive(context.Background(), 0); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	if _, refresh := s.CurrentCredentials(); refresh != "synthetic-refresh-r2" {
		t.Fatalf("refresh = %q after a second rotation", refresh)
	}
	if ts.refused.Load() != 0 {
		t.Fatal("a spent refresh token was presented")
	}
}

func TestSaxoSendsTheRedirectURIWhenConfigured(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		got = r.PostForm.Get("redirect_uri")
		fmt.Fprint(w, `{"access_token":"synthetic-access-2","refresh_token":"synthetic-refresh-2"}`)
	}))
	t.Cleanup(srv.Close)
	s := newSaxoAgainst(t, srv, saxoTestAccess, saxoTestRefresh)
	s.redirectURI = "https://app.example.test/saxo/callback"

	if err := s.KeepAlive(context.Background(), 0); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	if got != "https://app.example.test/saxo/callback" {
		t.Fatalf("redirect_uri = %q", got)
	}
}

// Every request of a sync that started on an expired access token runs into
// the same 401 at once. Exactly one of them may refresh: the others must wait
// and take the pair it got, because the refresh token they hold is spent the
// moment the first refresh lands.
func TestSaxoConcurrentRejectionsRefreshOnce(t *testing.T) {
	ts := newSaxoTokenServer(t, "synthetic-access-current", saxoTestRefresh)
	ts.hold = 20 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(ts.handler))
	t.Cleanup(srv.Close)
	s := newSaxoAgainst(t, srv, "synthetic-access-expired", saxoTestRefresh)

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.GetBalance(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("GetBalance: %v", err)
	}
	if n := ts.refreshes.Load(); n != 1 {
		t.Fatalf("%d refreshes for one expired token, want 1", n)
	}
	if ts.refused.Load() != 0 {
		t.Fatal("a spent refresh token was presented")
	}
}

// memTokenGuard stands in for the service guard: one stored pair shared by
// every connector instance of a connection, one refresh at a time, and the
// rotated pair stored before anyone uses it.
type memTokenGuard struct {
	mu        sync.Mutex
	stored    OAuthTokens
	rotatedAt time.Time
}

func (g *memTokenGuard) Rotate(_ context.Context, fn func(OAuthTokens, time.Time) (*OAuthTokens, error)) (OAuthTokens, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	next, err := fn(g.stored, g.rotatedAt)
	if err != nil || next == nil {
		return g.stored, err
	}
	g.stored = *next
	g.rotatedAt = time.Now()
	return g.stored, nil
}

// Two instances built from the same stored row (a sync and a reconstruction)
// and a keep-alive, all at once: with the guard shared, Saxo never sees two
// refreshes in flight and never sees a spent refresh token.
func TestSaxoSharedGuardSerialisesInstancesAndKeepAlive(t *testing.T) {
	ts := newSaxoTokenServer(t, "synthetic-access-current", saxoTestRefresh)
	ts.hold = 5 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(ts.handler))
	t.Cleanup(srv.Close)
	guard := &memTokenGuard{stored: OAuthTokens{AccessToken: "synthetic-access-expired", RefreshToken: saxoTestRefresh}}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := range 4 {
		inst := newSaxoAgainst(t, srv, "synthetic-access-expired", saxoTestRefresh)
		inst.SetRefreshGuard(guard)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				if i == 0 {
					if err := inst.KeepAlive(context.Background(), 0); err != nil {
						errs <- err
					}
					continue
				}
				if _, err := inst.GetBalance(context.Background()); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("call failed: %v", err)
	}
	if m := ts.maxInFlight.Load(); m > 1 {
		t.Fatalf("%d refreshes in flight at once, want at most 1", m)
	}
	if ts.refused.Load() != 0 {
		t.Fatal("a spent refresh token was presented")
	}
}

// A holder that finds the refused access token already replaced takes the
// stored pair instead of refreshing again.
func TestSaxoAdoptsAPairRotatedElsewhere(t *testing.T) {
	ts := newSaxoTokenServer(t, "synthetic-access-elsewhere", "synthetic-refresh-elsewhere")
	srv := httptest.NewServer(http.HandlerFunc(ts.handler))
	t.Cleanup(srv.Close)

	s := newSaxoAgainst(t, srv, "synthetic-access-stale", "synthetic-refresh-spent")
	s.SetRefreshGuard(&memTokenGuard{stored: OAuthTokens{AccessToken: "synthetic-access-elsewhere", RefreshToken: "synthetic-refresh-elsewhere"}})

	if _, err := s.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if ts.refreshes.Load() != 0 {
		t.Fatal("refreshed although the stored pair was already new")
	}
	if access, _ := s.CurrentCredentials(); access != "synthetic-access-elsewhere" {
		t.Fatalf("holding %q, want the stored pair", access)
	}
}

func TestSaxoKeepAliveSkipsARecentRotation(t *testing.T) {
	ts := newSaxoTokenServer(t, saxoTestAccess, saxoTestRefresh)
	srv := httptest.NewServer(http.HandlerFunc(ts.handler))
	t.Cleanup(srv.Close)
	s := newSaxoAgainst(t, srv, saxoTestAccess, saxoTestRefresh)

	ctx := context.Background()
	if err := s.KeepAlive(ctx, time.Hour); err != nil {
		t.Fatalf("first KeepAlive: %v", err)
	}
	if err := s.KeepAlive(ctx, time.Hour); err != nil {
		t.Fatalf("second KeepAlive: %v", err)
	}
	if n := ts.refreshes.Load(); n != 1 {
		t.Fatalf("%d refreshes, want 1 (the second was inside minAge)", n)
	}
}

func saxoTokenStub(t *testing.T, status int, body string) *Saxo {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return newSaxoAgainst(t, srv, saxoTestAccess, saxoTestRefresh)
}

// Each answer of the token endpoint has to land in the category that tells the
// right person what to do: a dead grant asks the user to reconnect, a refused
// app asks the operator to fix the enclave, a throttle or an outage asks
// nobody anything.
func TestSaxoTokenEndpointClassification(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		body        string
		refused     bool
		transient   bool
		wantKind    ConnectFailure
		wantMessage string
	}{
		{"expired grant", 400, `{"error":"invalid_grant"}`, true, false, ConnectFailureCredentials, errsanitize.MsgBrokerReauthRequired},
		{"refused without a code", 401, ``, true, false, ConnectFailureCredentials, errsanitize.MsgBrokerReauthRequired},
		{"app refused", 401, `{"error":"invalid_client"}`, false, false, ConnectFailureNotConfigured, errsanitize.MsgExchangeNotConfigured},
		{"malformed request", 400, `{"error":"invalid_request"}`, false, false, ConnectFailureNotConfigured, errsanitize.MsgExchangeNotConfigured},
		{"throttled", 429, ``, false, true, ConnectFailureUpstream, errsanitize.MsgRateLimited},
		{"outage", 503, ``, false, true, ConnectFailureUpstream, ""},
		{"no access token", 200, `{"token_type":"Bearer"}`, false, true, ConnectFailureCredentials, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := saxoTokenStub(t, tc.status, tc.body)
			_, err := s.GetBalance(context.Background())
			if err == nil {
				t.Fatal("GetBalance succeeded")
			}
			if got := errors.Is(err, ErrRefreshRefused); got != tc.refused {
				t.Fatalf("ErrRefreshRefused = %v, want %v (%v)", got, tc.refused, err)
			}
			if got := errors.Is(err, ErrTransient); got != tc.transient {
				t.Fatalf("ErrTransient = %v, want %v (%v)", got, tc.transient, err)
			}
			if got := ClassifyConnectFailure(err); got != tc.wantKind {
				t.Fatalf("ClassifyConnectFailure = %v, want %v (%v)", got, tc.wantKind, err)
			}
			if tc.wantMessage != "" {
				if got := errsanitize.Category(err.Error()); got != tc.wantMessage {
					t.Fatalf("category = %q, want %q (%v)", got, tc.wantMessage, err)
				}
			}
			// The pair the connector holds is left alone: a refusal or an
			// outage rotated nothing.
			if access, refresh := s.CurrentCredentials(); access != saxoTestAccess || refresh != saxoTestRefresh {
				t.Fatalf("holding %q/%q after a failed refresh", access, refresh)
			}
		})
	}
}

// Without the app credentials no refresh can ever work, so the connect is
// refused as an enclave problem before a single call reaches Saxo.
func TestSaxoNotConfiguredIsTheOperatorsProblem(t *testing.T) {
	t.Setenv("SAXO_APP_KEY", "")
	t.Setenv("SAXO_APP_SECRET", "")
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	t.Cleanup(srv.Close)
	s := NewSaxo(&Credentials{APIKey: saxoTestAccess, APISecret: saxoTestRefresh}, true)
	s.baseURL, s.authURL = srv.URL, srv.URL

	err := s.TestConnection(context.Background())
	if !errors.Is(err, ErrSaxoNotConfigured) {
		t.Fatalf("err = %v, want ErrSaxoNotConfigured", err)
	}
	if got := ClassifyConnectFailure(err); got != ConnectFailureNotConfigured {
		t.Fatalf("ClassifyConnectFailure = %v", got)
	}
	if got := errsanitize.Category(err.Error()); got != errsanitize.MsgExchangeNotConfigured {
		t.Fatalf("category = %q", got)
	}
	if err := s.KeepAlive(context.Background(), 0); !errors.Is(err, ErrSaxoNotConfigured) {
		t.Fatalf("KeepAlive err = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("an unconfigured connector called Saxo")
	}
}

func TestSaxoAppCredentialsComeFromTheEnvironment(t *testing.T) {
	t.Setenv("SAXO_APP_KEY", "env-app-key")
	t.Setenv("SAXO_APP_SECRET", "env-app-secret")
	t.Setenv("SAXO_REDIRECT_URI", "https://app.example.test/cb")
	s := NewSaxo(&Credentials{APIKey: "a", APISecret: "r"}, false)
	if s.appKey != "env-app-key" || s.appSecret != "env-app-secret" || s.redirectURI != "https://app.example.test/cb" {
		t.Fatalf("app config = %q/%q/%q", s.appKey, s.appSecret, s.redirectURI)
	}
}

// No refresh failure, whatever Saxo echoes back, may carry the refresh token,
// the access token or the app secret into an error, which ends in a log.
func TestSaxoRefreshErrorsCarryNoSecrets(t *testing.T) {
	echo := func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error":"invalid_grant","error_description":"refresh_token=%s auth=%s"}`,
			r.PostForm.Get("refresh_token"), r.Header.Get("Authorization"))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			echo(w, r)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `{"Message":"bad bearer %s"}`, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	}))
	t.Cleanup(srv.Close)
	s := newSaxoAgainst(t, srv, saxoTestAccess, saxoTestRefresh)

	_, err := s.GetBalance(context.Background())
	if err == nil {
		t.Fatal("GetBalance succeeded")
	}
	msg := err.Error()
	basic := base64.StdEncoding.EncodeToString([]byte(saxoTestAppKey + ":" + saxoTestAppSecret))
	for _, secret := range []string{saxoTestRefresh, saxoTestAccess, saxoTestAppSecret, basic} {
		if strings.Contains(msg, secret) {
			t.Fatalf("error %q carries a secret", msg)
		}
	}

	// The unreachable path too: a transport error names the URL, and the URL
	// carries the client key.
	dead := newSaxoAgainst(t, srv, saxoTestAccess, saxoTestRefresh)
	dead.baseURL = "http://127.0.0.1:1"
	dead.authURL = "http://127.0.0.1:1"
	_, err = dead.GetBalance(context.Background())
	if err == nil || strings.Contains(err.Error(), "127.0.0.1:1/port") {
		t.Fatalf("transport error %v names the request URL", err)
	}
}
