package repository

import (
	"strings"
	"testing"
	"time"
)

// The delete behind UpsertBatchVacating must name one connection and the
// listed days, nothing wider: dropping a term turns a tidy-up into data loss.
func TestVacatedDaysScope_OneConnectionListedDaysOnly(t *testing.T) {
	d := time.Date(2026, 5, 4, 0, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	where, args := vacatedDaysScope(true, true, VacatedDays{UserUID: "user-1", Exchange: "ctrader", Label: "main", Days: []time.Time{d}})

	for _, want := range []string{`"userUid" = $1`, `exchange = $2`, `label = $3`, `timestamp = ANY($4)`} {
		if !strings.Contains(where, want) {
			t.Errorf("predicate missing %q:\n%s", want, where)
		}
	}
	if len(args) != 4 {
		t.Fatalf("got %d args, want 4: %v", len(args), args)
	}
	days := args[3].([]time.Time)
	if len(days) != 1 || days[0].Location() != time.UTC || !days[0].Equal(d) {
		t.Fatalf("days %v, want the one day in UTC", days)
	}
}

func TestVacatedDaysScope_WithoutLabelColumn(t *testing.T) {
	where, args := vacatedDaysScope(false, false, VacatedDays{UserUID: "user-1", Exchange: "ctrader", Label: "ignored", Days: []time.Time{time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)}})

	if strings.Contains(where, "label") {
		t.Errorf("predicate references a label column that does not exist:\n%s", where)
	}
	if !strings.Contains(where, "user_uid = $1") || !strings.Contains(where, "timestamp = ANY($3)") {
		t.Errorf("Go schema columns or placeholders wrong:\n%s", where)
	}
	if len(args) != 3 {
		t.Fatalf("got %d args, want 3: %v", len(args), args)
	}
}
