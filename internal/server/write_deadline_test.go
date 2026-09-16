package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// deadlineRecorder is an httptest.ResponseRecorder that can carry a write
// deadline, which is what http.NewResponseController looks for.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	set   time.Time
	calls int
}

func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	d.calls++
	d.set = t
	return nil
}

// The server caps every response at sixty seconds. A reconstruction outlives
// that — one OKX account with 13,952 bills takes three minutes — so the
// connection closed before the answer existed. The caller read a dropped
// connection as a failure and retried, which started the three minutes again:
// a loop that hammered the venue and could not end on its own.
func TestExtendWriteDeadline_PushesThePerRequestCap(t *testing.T) {
	rec := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	before := time.Now()

	extendWriteDeadline(rec, 45*time.Minute)

	if rec.calls != 1 {
		t.Fatalf("SetWriteDeadline called %d times, want 1", rec.calls)
	}
	if got := rec.set.Sub(before); got < 44*time.Minute {
		t.Fatalf("deadline moved by %v, want at least 44 minutes", got)
	}
}

// A ResponseWriter that cannot carry a deadline must not panic or abort the
// handler: the request simply keeps the server-wide cap, which is where it was
// before this existed.
func TestExtendWriteDeadline_UnsupportedWriterIsHarmless(t *testing.T) {
	rec := httptest.NewRecorder()
	extendWriteDeadline(rec, time.Minute)

	if err := http.NewResponseController(rec).SetWriteDeadline(time.Now()); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("recorder unexpectedly supports deadlines (%v); the test no longer covers the fallback", err)
	}
}
