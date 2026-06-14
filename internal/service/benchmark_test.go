package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// BENCH-01: the benchmark fetch carries X-Internal-Token when a token is
// configured, and omits the header entirely when it isn't (dev/test stacks).
func TestBenchmarkService_InternalTokenHeader(t *testing.T) {
	const body = `{"success":true,"data":{"symbol":"SPY","data":[` +
		`{"date":"2026-01-01","close":100,"adjustedClose":100},` +
		`{"date":"2026-01-02","close":101,"adjustedClose":101}]}}`

	run := func(t *testing.T, token string) (present bool, value string) {
		var saw bool
		var val string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, saw = r.Header["X-Internal-Token"]
			val = r.Header.Get("X-Internal-Token")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		defer srv.Close()

		s := NewBenchmarkService(srv.URL, token)
		if _, err := s.DailyReturnsByDate(context.Background(), "SPY", time.Now().AddDate(0, -1, 0), time.Now()); err != nil {
			t.Fatalf("DailyReturnsByDate: %v", err)
		}
		return saw, val
	}

	t.Run("token sent when configured", func(t *testing.T) {
		const tok = "bench-secret-token-abcdef123456"
		present, value := run(t, tok)
		if !present || value != tok {
			t.Fatalf("X-Internal-Token: present=%v value=%q, want the configured token", present, value)
		}
	})

	t.Run("header omitted when no token", func(t *testing.T) {
		present, _ := run(t, "")
		if present {
			t.Fatalf("X-Internal-Token must be absent when no token is configured")
		}
	})
}
