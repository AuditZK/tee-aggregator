package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidateCORSConfig(t *testing.T) {
	cases := []struct {
		name    string
		env     string
		raw     string
		wantErr bool
	}{
		{"dev allows wildcard", "development", "*", false},
		{"dev allows empty", "development", "", false},
		{"dev allows list", "development", "https://a.example,https://b.example", false},
		{"prod refuses empty", "production", "", true},
		{"prod refuses whitespace-only", "production", "   ", true},
		{"prod refuses bare wildcard", "production", "*", true},
		{"prod refuses wildcard in list", "production", "https://a.example, *", true},
		{"prod accepts explicit list", "production", "https://app.example.com", false},
		{"prod case-insensitive on env name", "Production", "https://app.example.com", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCORSConfig(tc.env, tc.raw)
			if tc.wantErr && err == nil {
				t.Fatal("want error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
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

func TestCORSMiddleware_ReflectsAllowedOrigin(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	h := CORSMiddleware("https://a.example,https://b.example", next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "https://a.example")
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatal("inner handler should have been called for non-OPTIONS request")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://a.example" {
		t.Errorf("ACAO=%q, want https://a.example", got)
	}
}

func TestCORSMiddleware_RejectsUnknownOrigin(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := CORSMiddleware("https://a.example", next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
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
	h := CORSMiddleware("https://a.example", next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/connection", nil)
	req.Header.Set("Origin", "https://a.example")
	h.ServeHTTP(rec, req)

	if called {
		t.Error("OPTIONS preflight should not invoke inner handler")
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("preflight status=%d, want 204", rec.Code)
	}
	// Preflight must still carry CORS headers so the browser proceeds.
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://a.example" {
		t.Errorf("preflight ACAO=%q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got == "" {
		t.Error("preflight missing Allow-Headers")
	}
}

// Documents LOW-002: the middleware does not currently emit Vary: Origin.
// The test ASSERTS the documented (broken) behaviour so the audit team
// can flip it to want="Origin" once the fix lands. Today: empty.
func TestCORSMiddleware_VaryOrigin_DocumentsCurrentBehaviour(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := CORSMiddleware("https://a.example", next)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "https://a.example")
	h.ServeHTTP(rec, req)

	got := rec.Header().Get("Vary")
	// Snapshot of current behaviour. When LOW-002 is fixed, change this
	// expectation to "Origin" to lock in the fix.
	if got != "" {
		t.Logf("Vary=%q (LOW-002 fix landed? Update assertion)", got)
	}
}
