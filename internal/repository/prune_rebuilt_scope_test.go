package repository

import (
	"strings"
	"testing"
	"time"
)

// This predicate deletes production rows. Three terms keep it a tidy-up rather
// than data loss, and each one guards a different way of losing a user's
// history: the origin flag protects everything the live branch measured, the
// range confines the delete to the span actually reconstructed, and the keep
// list spares the series just written.
func TestPrunedRebuiltScope_GuardsAreAllPresent(t *testing.T) {
	keep := []time.Time{
		time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC),
	}

	where, args := prunedRebuiltScope(true, true, "user-1", "alpaca", "alpaca account", time.Time{}, keep)

	for _, want := range []string{
		`"userUid" = $1`,
		`exchange = $2`,
		`label = $3`,
		`from_external_rebuilder = TRUE`,
		`timestamp >= $4`,
		`timestamp <= $5`,
		`timestamp <> ALL($6)`,
	} {
		if !strings.Contains(where, want) {
			t.Errorf("predicate missing %q — a guard against deleting live or unreconstructed history:\n%s", want, where)
		}
	}

	if len(args) != 6 {
		t.Fatalf("got %d args, want 6: %v", len(args), args)
	}
	// The floor is the caller's — here a full run, which owns everything it
	// ever rebuilt, so nothing older than the new series may survive it.
	if from := args[3].(time.Time); !from.IsZero() {
		t.Errorf("range starts %v, want the zero time for a full run", from)
	}
	if to := args[4].(time.Time); !to.Equal(keep[0]) {
		t.Errorf("range ends %v, want the latest kept day %v", to, keep[0])
	}
	if days := args[5].([]time.Time); len(days) != 3 {
		t.Errorf("keep list carries %d days, want 3", len(days))
	}
}

// Without the label column every connection of the same exchange shares one
// scope, so the placeholders must close up rather than leave a hole.
func TestPrunedRebuiltScope_WithoutLabelColumn(t *testing.T) {
	keep := []time.Time{time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)}
	where, args := prunedRebuiltScope(false, false, "user-1", "alpaca", "ignored", keep[0], keep)

	if strings.Contains(where, "label") {
		t.Errorf("predicate references a label column that does not exist:\n%s", where)
	}
	if !strings.Contains(where, "user_uid = $1") {
		t.Errorf("Go schema must use snake_case:\n%s", where)
	}
	if !strings.Contains(where, "timestamp >= $3") || !strings.Contains(where, "timestamp <> ALL($5)") {
		t.Errorf("placeholders did not close up after dropping label:\n%s", where)
	}
	if len(args) != 5 {
		t.Fatalf("got %d args, want 5: %v", len(args), args)
	}
}

// A single-day reconstruction — the bounded repair window — must not widen into
// a delete of the days around it.
func TestPrunedRebuiltScope_SingleDayStaysSingleDay(t *testing.T) {
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	_, args := prunedRebuiltScope(true, true, "user-1", "alpaca", "main", day, []time.Time{day})

	from := args[3].(time.Time)
	to := args[4].(time.Time)
	if !from.Equal(day) || !to.Equal(day) {
		t.Fatalf("range is %v..%v, want exactly %v", from, to, day)
	}
}

// A full reconstruction must reach BELOW its own output. The Alpaca correction
// moved every row a day, and the run before it had written a day earlier than
// anything the new one emits; deriving the floor from the new series left that
// row alive, and its funding was then counted twice in the contributed capital.
func TestPrunedRebuiltScope_FullRunReachesBelowItsOwnOutput(t *testing.T) {
	keep := []time.Time{
		time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
	}
	_, args := prunedRebuiltScope(true, true, "user-1", "alpaca", "main", time.Time{}, keep)

	from := args[3].(time.Time)
	stray := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	if from.After(stray) {
		t.Fatalf("floor %v spares a rebuilt row at %v that the run no longer produces", from, stray)
	}
	if to := args[4].(time.Time); !to.Equal(keep[1]) {
		t.Errorf("range ends %v, want the latest kept day %v", to, keep[1])
	}
}

// A venue that rations its ledger hands back only what it still serves. OKX
// keeps ninety days, so a rebuild run today says nothing about a day from four
// months ago — and a floor at the beginning of time would read that silence as
// a correction and delete it. Truncating a customer's record to the venue's
// retention, on every reconstruction, with nothing able to bring it back.
func TestPrunedRebuiltScope_FloorStopsAtTheRebuildsHorizon(t *testing.T) {
	horizon := time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC)
	keep := []time.Time{
		time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC),
	}

	_, args := prunedRebuiltScope(true, true, "user-1", "okx", "RAVCA_UW", horizon, keep)

	from := args[3].(time.Time)
	if !from.Equal(horizon) {
		t.Fatalf("floor is %v, want the rebuild's horizon %v", from, horizon)
	}
	// The days the rebuild could not reach sit below the floor and survive.
	older := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	if !older.Before(from) {
		t.Fatalf("a day at %v is inside a scope floored at %v", older, from)
	}
	// Everything the rebuild DID examine and no longer produces still goes:
	// 06-19 sits above the floor and outside the keep list.
	stale := time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC)
	if stale.Before(from) || stale.After(args[4].(time.Time)) {
		t.Fatalf("a stale day at %v escaped the scope %v..%v", stale, from, args[4])
	}
}
