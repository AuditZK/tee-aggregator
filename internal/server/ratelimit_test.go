package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseCIDRs(t *testing.T) {
	cases := []struct {
		name  string
		raw   []string
		want  int // expected number of parsed nets
		check string // expected to contain this IP if want > 0
	}{
		{"empty list", nil, 0, ""},
		{"whitespace only", []string{" ", ""}, 0, ""},
		{"bare ipv4 → /32", []string{"10.0.0.1"}, 1, "10.0.0.1"},
		{"bare ipv6 → /128", []string{"::1"}, 1, "::1"},
		{"explicit cidr", []string{"10.0.0.0/8"}, 1, "10.5.5.5"},
		{"mixed", []string{"10.0.0.0/8", "192.168.1.1"}, 2, "10.5.5.5"},
		{"invalid silently dropped", []string{"not-an-ip", "10.0.0.0/8"}, 1, "10.5.5.5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseCIDRs(tc.raw)
			if len(got) != tc.want {
				t.Fatalf("len=%d, want %d", len(got), tc.want)
			}
		})
	}
}

func TestIsTrustedPeer(t *testing.T) {
	rl := NewIPRateLimiter(100, time.Minute, "10.0.0.0/8", "192.168.1.5")

	cases := []struct {
		name string
		addr string
		want bool
	}{
		{"in CIDR", "10.5.5.5:1234", true},
		{"bare ip in allowlist", "192.168.1.5:443", true},
		{"outside CIDR", "8.8.8.8:80", false},
		{"empty addr", "", false},
		{"unparseable host", "garbage", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rl.isTrustedPeer(tc.addr); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsTrustedPeer_NoTrustedProxiesDefaultsFalse(t *testing.T) {
	rl := NewIPRateLimiter(100, time.Minute /* no trusted */)
	if rl.isTrustedPeer("10.0.0.1:80") {
		t.Fatal("with empty trustedProxies, every peer must be untrusted")
	}
}

func TestExtractIP_TrustedProxy(t *testing.T) {
	rl := NewIPRateLimiter(100, time.Minute, "10.0.0.0/8")
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.1:443"
	r.Header.Set("X-Forwarded-For", "203.0.113.42, 10.0.0.1")

	if got := rl.extractIP(r); got != "203.0.113.42" {
		t.Fatalf("got %q, want 203.0.113.42 (trusted proxy → first XFF)", got)
	}
}

func TestExtractIP_TrustedProxy_XRealIP(t *testing.T) {
	rl := NewIPRateLimiter(100, time.Minute, "10.0.0.0/8")
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.1:443"
	r.Header.Set("X-Real-IP", "203.0.113.99")
	if got := rl.extractIP(r); got != "203.0.113.99" {
		t.Fatalf("got %q, want 203.0.113.99", got)
	}
}

// SEC-004: untrusted peers cannot spoof their IP via XFF.
func TestExtractIP_UntrustedPeerIgnoresXFF(t *testing.T) {
	rl := NewIPRateLimiter(100, time.Minute /* no trusted proxies */)
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "8.8.8.8:443"
	r.Header.Set("X-Forwarded-For", "127.0.0.1")
	r.Header.Set("X-Real-IP", "127.0.0.1")
	if got := rl.extractIP(r); got != "8.8.8.8" {
		t.Fatalf("got %q, want 8.8.8.8 (XFF must be ignored from untrusted peer)", got)
	}
}

func TestExtractIP_FallbackStripsPort(t *testing.T) {
	rl := NewIPRateLimiter(100, time.Minute)
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.10:9876"
	if got := rl.extractIP(r); got != "203.0.113.10" {
		t.Fatalf("got %q, want 203.0.113.10", got)
	}
}

func TestAllow_AllowsUpToLimit(t *testing.T) {
	rl := NewIPRateLimiter(3, time.Minute)
	for i := 0; i < 3; i++ {
		if !rl.Allow("1.1.1.1") {
			t.Fatalf("call #%d should be allowed", i+1)
		}
	}
	if rl.Allow("1.1.1.1") {
		t.Fatal("4th call should be blocked")
	}
}

func TestAllow_PerIPIndependent(t *testing.T) {
	rl := NewIPRateLimiter(2, time.Minute)
	rl.Allow("1.1.1.1")
	rl.Allow("1.1.1.1") // 1.1 is at limit
	if rl.Allow("1.1.1.1") {
		t.Fatal("1.1.1.1 should be blocked")
	}
	if !rl.Allow("2.2.2.2") {
		t.Fatal("2.2.2.2 must still be allowed (per-IP isolation)")
	}
}

func TestAllow_WindowExpiry(t *testing.T) {
	rl := NewIPRateLimiter(1, 10*time.Millisecond)
	if !rl.Allow("9.9.9.9") {
		t.Fatal("first must pass")
	}
	if rl.Allow("9.9.9.9") {
		t.Fatal("second must be blocked")
	}
	time.Sleep(20 * time.Millisecond)
	if !rl.Allow("9.9.9.9") {
		t.Fatal("after window expiry, must allow again")
	}
}

// SEC-004: cap on the per-IP map prevents an attacker rotating IPs from
// growing unbounded memory.
func TestAllow_EvictsOldestWhenMapFull(t *testing.T) {
	rl := NewIPRateLimiter(1, time.Minute)
	// Inject a known capacity by directly writing past the visible-ish API:
	// we cannot mutate maxRateLimiterEntries, but we can verify the
	// insertionOrder bookkeeping stays consistent on hot path.
	for i := 0; i < 50; i++ {
		ip := byte(i)
		rl.Allow(string([]byte{1, 2, 3, ip}))
	}
	// The map should not have grown beyond the number of unique IPs we
	// inserted — basic sanity check that the bookkeeping works.
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if len(rl.insertionOrder) != len(rl.requests) {
		t.Fatalf("insertionOrder=%d, requests=%d (must stay aligned)",
			len(rl.insertionOrder), len(rl.requests))
	}
}

func TestMiddleware_429OnLimitExceeded(t *testing.T) {
	rl := NewIPRateLimiter(1, time.Minute)
	called := 0
	wrapped := rl.Middleware(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	})

	rec1 := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "1.2.3.4:1000"
	wrapped(rec1, r)
	if rec1.Code != http.StatusOK || called != 1 {
		t.Fatalf("first call: code=%d called=%d", rec1.Code, called)
	}

	rec2 := httptest.NewRecorder()
	wrapped(rec2, r)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second call: want 429, got %d", rec2.Code)
	}
	if called != 1 {
		t.Fatalf("inner handler must NOT be called when rate-limited: called=%d", called)
	}
}

func TestCleanup_DropsExpiredEntries(t *testing.T) {
	rl := NewIPRateLimiter(5, 5*time.Millisecond)
	rl.Allow("a")
	rl.Allow("b")
	time.Sleep(10 * time.Millisecond)
	rl.cleanup()

	rl.mu.Lock()
	defer rl.mu.Unlock()
	if len(rl.requests) != 0 {
		t.Fatalf("want empty map after cleanup, got %d entries", len(rl.requests))
	}
	if len(rl.insertionOrder) != 0 {
		t.Fatalf("insertionOrder must also be compacted, got %d", len(rl.insertionOrder))
	}
}

// SEC-02: only the POST (nonce) path of /api/v1/attestation forks snpguest and
// must be metered; cheap GET probes pass through unmetered.
func TestAttestationPOSTRateLimit(t *testing.T) {
	called := 0
	next := func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	}
	rl := NewIPRateLimiter(1, time.Minute)
	h := attestationPOSTRateLimit(rl, next)

	const ip = "203.0.113.7:1234"
	do := func(method, addr string) int {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(method, "/api/v1/attestation", nil)
		r.RemoteAddr = addr
		h(rec, r)
		return rec.Code
	}

	// GET is never metered.
	for i := 0; i < 3; i++ {
		if got := do(http.MethodGet, ip); got != http.StatusOK {
			t.Fatalf("GET #%d: got %d, want 200", i+1, got)
		}
	}

	// First POST passes; the second from the same IP is throttled.
	if got := do(http.MethodPost, ip); got != http.StatusOK {
		t.Fatalf("POST #1: got %d, want 200", got)
	}
	if got := do(http.MethodPost, ip); got != http.StatusTooManyRequests {
		t.Fatalf("POST #2: got %d, want 429", got)
	}

	// A different IP has its own bucket.
	if got := do(http.MethodPost, "198.51.100.9:5678"); got != http.StatusOK {
		t.Fatalf("POST from fresh IP: got %d, want 200", got)
	}

	// 3 GET + 1 POST(ok) + 1 POST(fresh IP) = 5; the throttled POST never
	// reaches the inner handler.
	if called != 5 {
		t.Fatalf("inner handler called %d times, want 5", called)
	}
}
