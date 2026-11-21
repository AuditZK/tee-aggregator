package service

import (
	"testing"
	"time"
)

// A failed attempt must not advance lastSyncTime. Two things depend on it.
// The deferred rate-limit retry and the next daily pass both decide what to
// re-run from that timestamp, and a connection whose first sync failed would
// otherwise report a sync it never completed.
func TestSyncStatusStamp(t *testing.T) {
	attempt := time.Date(2026, 8, 25, 0, 0, 30, 0, time.UTC)

	if got := syncStatusStamp("error", attempt); got != nil {
		t.Fatalf("a failure must not stamp a sync time, got %v", *got)
	}

	for _, status := range []string{"completed", "pending"} {
		got := syncStatusStamp(status, attempt)
		if got == nil {
			t.Fatalf("status %q must stamp the attempt", status)
		}
		if !got.Equal(attempt) {
			t.Fatalf("status %q stamped %v, want %v", status, *got, attempt)
		}
	}
}

// The due-set behaviour the nil stamp protects. A connection that has never
// synced carries a NULL lastSyncTime, and every pass must consider it due.
// Before this change a first-sync failure wrote no row at all, which produced
// the same due-ness by accident; now the row exists and carries the error, so
// the NULL has to do that work on purpose.
func TestFailedFirstSyncStaysDue(t *testing.T) {
	now := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)

	if !isDueByInterval(syncStatusStamp("error", now), 1440, now) {
		t.Fatal("a connection whose sync failed must stay due")
	}

	// And a success on the same day takes it out of the due set, which is what
	// stops the daily pass from re-running a connection it just synced.
	if isDueByInterval(syncStatusStamp("completed", now), 1440, now) {
		t.Fatal("a connection synced today must not be due again today")
	}
}
