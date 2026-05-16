package connector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTruncatedBody_BelowCap(t *testing.T) {
	body := []byte("short body")
	if got := TruncatedBody(body); got != "short body" {
		t.Fatalf("got %q", got)
	}
}

func TestTruncatedBody_AboveCap(t *testing.T) {
	body := bytes.Repeat([]byte("A"), errorBodyMaxLen+50)
	got := TruncatedBody(body)
	if !strings.HasSuffix(got, "...[truncated]") {
		t.Fatalf("missing truncation marker: %q", got[len(got)-30:])
	}
	if len(got) != errorBodyMaxLen+len("...[truncated]") {
		t.Fatalf("len=%d, want %d", len(got), errorBodyMaxLen+len("...[truncated]"))
	}
}

func TestTruncatedBody_AtBoundary(t *testing.T) {
	// Body of exactly errorBodyMaxLen must NOT be marked truncated.
	body := bytes.Repeat([]byte("X"), errorBodyMaxLen)
	got := TruncatedBody(body)
	if strings.Contains(got, "[truncated]") {
		t.Fatal("body of exactly cap length should not be marked truncated")
	}
	if len(got) != errorBodyMaxLen {
		t.Fatalf("len=%d", len(got))
	}
}

func TestIsRetryableStatus(t *testing.T) {
	yes := []int{429, 500, 502, 503, 504}
	no := []int{200, 201, 301, 400, 401, 403, 404, 409, 418, 422}
	for _, c := range yes {
		if !isRetryableStatus(c) {
			t.Errorf("status %d should be retryable", c)
		}
	}
	for _, c := range no {
		if isRetryableStatus(c) {
			t.Errorf("status %d should NOT be retryable", c)
		}
	}
}

func TestParseRetryAfter_Seconds(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "2")
	got := parseRetryAfter(h, 99*time.Second)
	if got != 2*time.Second {
		t.Fatalf("got %v, want 2s", got)
	}
}

// parseRetryAfter must cap pathological values at maxBackoff so an
// adversarial server cannot pin our retry loop indefinitely.
func TestParseRetryAfter_CapsAtMaxBackoff(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "9999")
	got := parseRetryAfter(h, time.Second)
	if got != maxBackoff {
		t.Fatalf("got %v, want %v (capped)", got, maxBackoff)
	}
}

func TestParseRetryAfter_HTTPDate(t *testing.T) {
	h := http.Header{}
	when := time.Now().Add(2 * time.Second)
	h.Set("Retry-After", when.UTC().Format(http.TimeFormat))
	got := parseRetryAfter(h, time.Hour)
	// Tolerate clock skew inside the 0-3s band.
	if got <= 0 || got > maxBackoff {
		t.Fatalf("got %v (out of plausible 0..maxBackoff range)", got)
	}
}

// A past HTTP-date should fall back to the supplied default, not a
// negative duration that would make time.After panic.
func TestParseRetryAfter_PastDateFallsBack(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
	got := parseRetryAfter(h, 250*time.Millisecond)
	if got != 250*time.Millisecond {
		t.Fatalf("got %v, want fallback 250ms", got)
	}
}

func TestParseRetryAfter_GarbageFallsBack(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "not-a-thing")
	got := parseRetryAfter(h, 42*time.Millisecond)
	if got != 42*time.Millisecond {
		t.Fatalf("got %v, want fallback", got)
	}
}

func TestParseRetryAfter_EmptyFallsBack(t *testing.T) {
	got := parseRetryAfter(http.Header{}, 17*time.Millisecond)
	if got != 17*time.Millisecond {
		t.Fatalf("got %v", got)
	}
}

func TestReadCappedBody_BelowCap(t *testing.T) {
	src := io.NopCloser(strings.NewReader("hello"))
	body, err := ReadCappedBody(src, 1024)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if string(body) != "hello" {
		t.Fatalf("got %q", body)
	}
}

func TestReadCappedBody_ExactCap(t *testing.T) {
	src := io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("X"), 1024)))
	body, err := ReadCappedBody(src, 1024)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(body) != 1024 {
		t.Fatalf("len=%d", len(body))
	}
}

func TestReadCappedBody_OverCap_ReturnsErrAndPrefix(t *testing.T) {
	src := io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("X"), 4096)))
	body, err := ReadCappedBody(src, 1024)
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("got %v, want ErrResponseTooLarge", err)
	}
	if int64(len(body)) != 1024 {
		t.Fatalf("len=%d, want 1024 (prefix)", len(body))
	}
}

// closeTracker records whether Close was called on the body.
type closeTracker struct {
	io.Reader
	closed atomic.Bool
}

func (c *closeTracker) Close() error {
	c.closed.Store(true)
	return nil
}

func TestReadCappedBody_AlwaysClosesBody(t *testing.T) {
	cases := []struct {
		name  string
		body  []byte
		max   int64
		errOk bool // err is acceptable
	}{
		{"under cap", []byte("ok"), 1024, false},
		{"over cap", bytes.Repeat([]byte("X"), 2048), 1024, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct := &closeTracker{Reader: bytes.NewReader(tc.body)}
			_, err := ReadCappedBody(ct, tc.max)
			if !tc.errOk && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if !ct.closed.Load() {
				t.Fatal("body must be closed in all paths")
			}
		})
	}
}

// errReader simulates a network read failure mid-stream.
type errReader struct{}

func (errReader) Read(p []byte) (int, error) { return 0, errors.New("net broken") }
func (errReader) Close() error               { return nil }

func TestReadCappedBody_PropagatesReadError(t *testing.T) {
	body, err := ReadCappedBody(errReader{}, 1024)
	if err == nil || errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected non-cap error, got %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("body=%v", body)
	}
}

func TestNewCryptoBase_DefaultTimeout(t *testing.T) {
	b := NewCryptoBase("k", "s", "https://api.example.com")
	if b.Client.Timeout != 30*time.Second {
		t.Fatalf("timeout=%v", b.Client.Timeout)
	}
	if b.APIKey != "k" || b.APISecret != "s" {
		t.Fatal("creds not stored verbatim")
	}
	if b.BaseURL != "https://api.example.com" {
		t.Fatalf("baseurl=%q", b.BaseURL)
	}
}

func TestCryptoBase_DoRequest_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	b := NewCryptoBase("k", "s", srv.URL)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/whatever", nil)
	body, err := b.DoRequest(req)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if string(body) != `{"ok":true}` {
		t.Fatalf("body=%s", body)
	}
}

func TestCryptoBase_DoRequest_4xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"bad creds"}`))
	}))
	defer srv.Close()

	b := NewCryptoBase("k", "s", srv.URL)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/x", nil)
	body, err := b.DoRequest(req)
	if err == nil {
		t.Fatal("expected error for 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("err=%q must mention status code", err)
	}
	if len(body) == 0 {
		t.Error("body should be returned alongside the error for diagnostics")
	}
}

func TestCryptoBase_DoJSON_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"value":42}`))
	}))
	defer srv.Close()

	b := NewCryptoBase("k", "s", srv.URL)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)

	var out struct {
		Value int `json:"value"`
	}
	if err := b.DoJSON(req, &out); err != nil {
		t.Fatalf("err=%v", err)
	}
	if out.Value != 42 {
		t.Fatalf("got %d", out.Value)
	}
}

// CONN-004 retry: 429 followed by 200 must succeed within maxRetryAttempts,
// and buildReq must be invoked once per attempt — that is what re-signs the
// request and is the whole reason retryHTTP takes a builder.
func TestRetryHTTP_RecoversAfter429(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0") // immediate retry
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	var builds int32
	body, err := retryHTTP(srv.Client(), func() (*http.Request, error) {
		atomic.AddInt32(&builds, 1)
		return http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	})
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("body=%s", body)
	}
	if atomic.LoadInt32(&hits) != 2 {
		t.Fatalf("hits=%d, want 2", hits)
	}
	if atomic.LoadInt32(&builds) != 2 {
		t.Fatalf("builds=%d, want 2 — request must be rebuilt (re-signed) per attempt", builds)
	}
}

func TestRetryHTTP_PermanentFailureNoRetry(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`unauth`))
	}))
	defer srv.Close()

	_, err := retryHTTP(srv.Client(), func() (*http.Request, error) {
		return http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("hits=%d, want 1 (no retry on 401)", hits)
	}
}

func TestRetryHTTP_ContextCancelledStopsRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	_, err := retryHTTP(srv.Client(), func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/", nil)
	})
	if err == nil {
		t.Fatal("expected ctx error")
	}
}

// End-to-end: signedQueryGET (the BingX / MEXC-spot request path) must route
// through retryHTTP and re-sign on retry — the retried request must carry a
// fresh `timestamp`, otherwise the exchange would reject the replayed HMAC.
func TestSignedQueryGET_RetriesWithFreshSignature(t *testing.T) {
	var attempts int32
	tsCh := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tsCh <- r.URL.Query().Get("timestamp")
		if atomic.AddInt32(&attempts, 1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	b := NewCryptoBase("key", "secret", srv.URL)
	body, err := b.signedQueryGET(context.Background(), "X-API-KEY", "/account", "")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("body=%s", body)
	}
	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("attempts=%d, want 2", got)
	}
	close(tsCh)
	var seen []string
	for ts := range tsCh {
		seen = append(seen, ts)
	}
	if len(seen) != 2 || seen[0] == seen[1] {
		t.Fatalf("timestamps=%v — retry must rebuild with a fresh signed timestamp", seen)
	}
}

func TestCryptoBase_GET_BuildsRequest(t *testing.T) {
	b := NewCryptoBase("k", "s", "http://x/")
	req, err := b.GET("http://x/y?q=1")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if req.Method != http.MethodGet {
		t.Fatalf("method=%q", req.Method)
	}
	if req.URL.RawQuery != "q=1" {
		t.Fatalf("rawquery=%q", req.URL.RawQuery)
	}
}

// Sanity: ErrResponseTooLarge has a stable string for log readability.
func TestErrResponseTooLarge_Stringer(t *testing.T) {
	if !strings.Contains(ErrResponseTooLarge.Error(), "exceeds cap") {
		t.Fatalf("err=%q", ErrResponseTooLarge.Error())
	}
}

// strconv import sanity (avoids "imported and not used"); used by
// Retry-After numeric parsing in the helper. The compiler will catch
// any drift between this test file and the helper's signature.
var _ = strconv.Itoa
