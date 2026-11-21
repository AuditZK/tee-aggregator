package repository

import (
	"strings"
	"testing"
	"time"
)

// The scope built here is spliced straight into a DELETE. Losing either guard
// — the origin term or the cutoff — or widening past one connection takes the
// user's live snapshots with it, the one outcome the feature promises can
// never happen.
func TestRebuiltHistoryScope(t *testing.T) {
	cutoff := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)

	t.Run("both guards are always present", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			isTS     bool
			hasLabel bool
		}{
			{"go schema with label", false, true},
			{"go schema without label", false, false},
			{"ts schema with label", true, true},
			{"ts schema without label", true, false},
		} {
			where, _ := rebuiltHistoryScope(tc.isTS, tc.hasLabel, "uid-1", "bybit", "acct", cutoff)
			if !strings.Contains(where, "from_external_rebuilder = TRUE") {
				t.Errorf("%s: origin guard missing: %q", tc.name, where)
			}
			if !strings.Contains(where, "timestamp < $") {
				t.Errorf("%s: cutoff guard missing: %q", tc.name, where)
			}
		}
	})

	t.Run("go schema binds user, exchange, label then cutoff", func(t *testing.T) {
		where, args := rebuiltHistoryScope(false, true, "uid-1", "bybit", "acct", cutoff)
		want := "user_uid = $1 AND exchange = $2 AND label = $3 AND from_external_rebuilder = TRUE AND timestamp < $4"
		if where != want {
			t.Fatalf("where = %q", where)
		}
		if len(args) != 4 || args[0] != "uid-1" || args[1] != "bybit" || args[2] != "acct" || args[3] != cutoff {
			t.Fatalf("args = %v", args)
		}
	})

	t.Run("ts schema quotes the camelCase user column", func(t *testing.T) {
		where, _ := rebuiltHistoryScope(true, true, "uid-1", "bybit", "acct", cutoff)
		if !strings.HasPrefix(where, `"userUid" = $1`) {
			t.Fatalf("where = %q", where)
		}
	})

	t.Run("cutoff placeholder follows the label term", func(t *testing.T) {
		// Binding a placeholder the SQL does not mention, or mentioning one that
		// was never bound, is a runtime error rather than a wrong result — so the
		// numbering has to move with the optional label term.
		withLabel, argsWith := rebuiltHistoryScope(false, true, "uid-1", "bybit", "acct", cutoff)
		if !strings.HasSuffix(withLabel, "timestamp < $4") || len(argsWith) != 4 {
			t.Errorf("with label: where = %q args = %d", withLabel, len(argsWith))
		}

		noLabel, argsNo := rebuiltHistoryScope(false, false, "uid-1", "bybit", "acct", cutoff)
		if !strings.HasSuffix(noLabel, "timestamp < $3") || len(argsNo) != 3 {
			t.Errorf("without label: where = %q args = %d", noLabel, len(argsNo))
		}
		if strings.Contains(noLabel, "label") {
			t.Errorf("label term survived its column: %q", noLabel)
		}
	})

	t.Run("scope never widens to the whole user", func(t *testing.T) {
		where, _ := rebuiltHistoryScope(false, true, "uid-1", "bybit", "acct", cutoff)
		if !strings.Contains(where, "exchange = $2") || !strings.Contains(where, "label = $3") {
			t.Errorf("predicate is not scoped to one connection: %q", where)
		}
	})
}
