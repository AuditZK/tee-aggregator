package logstream

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/trackrecord/enclave/internal/errtrack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const validAPIKey = "test-api-key-12345"

func newServerWithStore(t *testing.T) (*Server, *errtrack.Store) {
	t.Helper()
	s := NewServer(0, validAPIKey, zap.NewNop())
	store := errtrack.NewStore(100, 1000)
	s.SetErrorStore(store)
	return s, store
}

func authedRequest(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("X-Api-Key", validAPIKey)
	return req
}

// recordSampleEvent inserts one event into the store with a known
// fingerprint id so endpoint tests can target it directly.
func recordSampleEvent(store *errtrack.Store, id string) {
	store.Record(id, zapcore.ErrorLevel, errtrack.SanitizedEvent{
		Level:   "error",
		Message: "boom",
		Fields:  map[string]string{"path": "/x"},
	})
}

func TestErrorsEndpoints_RequireAuth(t *testing.T) {
	s, _ := newServerWithStore(t)
	cases := []string{
		"/errors/groups",
		"/errors/groups/abcdef0123456789",
		"/errors/stats",
		"/errors/stream",
	}
	for _, path := range cases {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil) // no header
			mux := http.NewServeMux()
			s.registerErrorRoutes(mux)
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s: expected 401, got %d", path, rec.Code)
			}
		})
	}
}

func TestErrorsEndpoints_DisabledWhenNoStore(t *testing.T) {
	s := NewServer(0, validAPIKey, zap.NewNop())
	// No SetErrorStore.

	mux := http.NewServeMux()
	s.registerErrorRoutes(mux)

	req := authedRequest(http.MethodGet, "/errors/groups")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestErrorsGroups_ListReturnsRecorded(t *testing.T) {
	s, store := newServerWithStore(t)
	recordSampleEvent(store, "abcdef0123456789")
	recordSampleEvent(store, "abcdef0123456789") // bump count
	recordSampleEvent(store, "1234567890abcdef")

	mux := http.NewServeMux()
	s.registerErrorRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, authedRequest(http.MethodGet, "/errors/groups"))

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Groups []map[string]any `json:"groups"`
		Count  int              `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Count != 2 {
		t.Fatalf("expected 2 groups, got %d", body.Count)
	}
}

func TestErrorsGroups_LimitClamped(t *testing.T) {
	s, store := newServerWithStore(t)
	for i := 0; i < 10; i++ {
		// distinct 16-hex-char ids
		id := "00000000000000" + string(rune('a'+i)) + "f"
		recordSampleEvent(store, id)
	}
	mux := http.NewServeMux()
	s.registerErrorRoutes(mux)

	rec := httptest.NewRecorder()
	// limit=999 must be clamped to maxListLimit (200), not crash.
	mux.ServeHTTP(rec, authedRequest(http.MethodGet, "/errors/groups?limit=999"))

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestErrorsGroupByID_RejectsBadID(t *testing.T) {
	s, _ := newServerWithStore(t)
	mux := http.NewServeMux()
	s.registerErrorRoutes(mux)

	for _, id := range []string{
		"%2E%2E%2Fetc%2Fpasswd",
		"AAAAAAAAAAAAAAAA",  // uppercase
		"xxxxxxxxxxxxxxxx",  // non-hex
		"abc",               // too short
		"abcdef0123456789a", // too long
		"%27%20OR%201%3D1", // URL-encoded SQLi attempt
	} {
		t.Run(id, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, authedRequest(http.MethodGet, "/errors/groups/"+id))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("id=%q expected 400, got %d", id, rec.Code)
			}
		})
	}
}

func TestErrorsGroupByID_NotFound(t *testing.T) {
	s, _ := newServerWithStore(t)
	mux := http.NewServeMux()
	s.registerErrorRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, authedRequest(http.MethodGet, "/errors/groups/0123456789abcdef"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestErrorsGroupByID_OutputReSanitizes(t *testing.T) {
	// Inject a poisoned sample directly into the store, bypassing
	// the capture path. The endpoint must scrub it on the way out.
	s, store := newServerWithStore(t)
	recordSampleEvent(store, "deadbeefcafebabe")
	// Poison: this would only happen if the capture-path guarantee
	// was broken. The endpoint output layer must catch it.
	store.PoisonForTest("deadbeefcafebabe", errtrack.SanitizedEvent{
		Level:   "error",
		Message: "raw api_key=AKIAIOSFODNN7EXAMPLE leaked here",
		Fields:  map[string]string{"x": "password=topsecret123"},
	})

	mux := http.NewServeMux()
	s.registerErrorRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, authedRequest(http.MethodGet, "/errors/groups/deadbeefcafebabe"))

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "AKIAIOSFODNN7EXAMPLE") {
		t.Fatalf("api_key leaked in output: %s", body)
	}
	if strings.Contains(body, "topsecret123") {
		t.Fatalf("password leaked in output: %s", body)
	}
}

func TestErrorsStats_ReturnsCounters(t *testing.T) {
	s, store := newServerWithStore(t)
	recordSampleEvent(store, "1111111111111111")
	recordSampleEvent(store, "2222222222222222")

	mux := http.NewServeMux()
	s.registerErrorRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, authedRequest(http.MethodGet, "/errors/stats"))

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
	var stats errtrack.Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats.TotalEvents != 2 {
		t.Fatalf("TotalEvents = %d, want 2", stats.TotalEvents)
	}
	if stats.ActiveGroups != 2 {
		t.Fatalf("ActiveGroups = %d, want 2", stats.ActiveGroups)
	}
}

func TestErrorsEndpoints_RejectNonGET(t *testing.T) {
	s, _ := newServerWithStore(t)
	mux := http.NewServeMux()
	s.registerErrorRoutes(mux)

	for _, path := range []string{
		"/errors/groups",
		"/errors/groups/abcdef0123456789",
		"/errors/stats",
		"/errors/stream",
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, authedRequest(http.MethodPost, path))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s POST: expected 405, got %d", path, rec.Code)
		}
	}
}
