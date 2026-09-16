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

	where, args := prunedRebuiltScope(true, true, "user-1", "alpaca", "alpaca account", keep)

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
	// Bounds come from the set itself, whatever order it arrived in.
	if from := args[3].(time.Time); !from.Equal(keep[1]) {
		t.Errorf("range starts %v, want the earliest kept day %v", from, keep[1])
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
	where, args := prunedRebuiltScope(false, false, "user-1", "alpaca", "ignored", keep)

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
	_, args := prunedRebuiltScope(true, true, "user-1", "alpaca", "main", []time.Time{day})

	from := args[3].(time.Time)
	to := args[4].(time.Time)
	if !from.Equal(day) || !to.Equal(day) {
		t.Fatalf("range is %v..%v, want exactly %v", from, to, day)
	}
}
