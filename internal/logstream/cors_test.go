package logstream

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORSMiddleware_ReflectsAllowedOrigin(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	h := corsMiddleware("https://a.example,https://b.example", next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/logs/stream", nil)
	req.Header.Set("Origin", "https://b.example")
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatal("inner handler should have been called for non-OPTIONS request")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://b.example" {
		t.Errorf("ACAO=%q, want https://b.example", got)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary=%q, want Origin (cache poisoning hardening)", got)
	}
}

func TestCORSMiddleware_RejectsUnknownOrigin(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := corsMiddleware("https://a.example", next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/logs/stream", nil)
	req.Header.Set("Origin", "https://evil.example")
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO=%q, want empty (origin not allowed)", got)
	}
}

func TestCORSMiddleware_PreflightShortCircuit(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	h := corsMiddleware("https://a.example", next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/errors/stream", nil)
	req.Header.Set("Origin", "https://a.example")
	h.ServeHTTP(rec, req)

	if called {
		t.Error("OPTIONS preflight should not invoke inner handler")
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("preflight status=%d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://a.example" {
		t.Errorf("preflight ACAO=%q", got)
	}
}

// SSE clients (the dashboard) send Cache-Control on the actual request and
// preflight it via Access-Control-Request-Headers. Allow-Headers must list
// it or the browser refuses the response, breaking the log-stream UI.
func TestCORSMiddleware_AllowsSSEHeaders(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	h := corsMiddleware("https://a.example", next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/logs/stream", nil)
	req.Header.Set("Origin", "https://a.example")
	h.ServeHTTP(rec, req)

	allowHeaders := rec.Header().Get("Access-Control-Allow-Headers")
	for _, want := range []string{"Cache-Control", "Accept", "Authorization", "X-Api-Key"} {
		if !contains(allowHeaders, want) {
			t.Errorf("Allow-Headers missing %q (got %q)", want, allowHeaders)
		}
	}
}

func TestParseOrigins(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"empty → wildcard", "", []string{"*"}},
		{"bare wildcard → wildcard", "*", []string{"*"}},
		{"single origin", "https://a.example", []string{"https://a.example"}},
		{"multiple origins", "https://a.example,https://b.example", []string{"https://a.example", "https://b.example"}},
		{"strips whitespace", "  https://a.example , https://b.example  ", []string{"https://a.example", "https://b.example"}},
		{"drops empty entries", "https://a.example,,https://b.example", []string{"https://a.example", "https://b.example"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseOrigins(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("len=%d, want %d (%v vs %v)", len(got), len(tc.want), got, tc.want)
			}
			for i, g := range got {
				if g != tc.want[i] {
					t.Fatalf("got[%d]=%q want %q", i, g, tc.want[i])
				}
			}
		})
	}
}

func TestIsOriginAllowed(t *testing.T) {
	cases := []struct {
		name    string
		origin  string
		allowed []string
		want    bool
	}{
		{"wildcard accepts anything", "https://evil.example", []string{"*"}, true},
		{"exact match accepted", "https://a.example", []string{"https://a.example"}, true},
		{"non-listed rejected", "https://evil.example", []string{"https://a.example"}, false},
		{"case-sensitive", "https://A.Example", []string{"https://a.example"}, false},
		{"empty allowlist rejects", "https://a.example", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isOriginAllowed(tc.origin, tc.allowed); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
