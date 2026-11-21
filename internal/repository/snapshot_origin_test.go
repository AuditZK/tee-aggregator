package repository

import (
	"strings"
	"testing"
)

// The TS write paths splice from_external_rebuilder in only when the column
// exists. Getting either state wrong is quietly destructive: with the column
// absent the SQL must stay byte-identical to the pre-015 shape (or every prod
// write breaks), and with it present the flag must actually be bound (or every
// externally rebuilt day keeps signing as attested live data).
func TestSnapshotTSOptionalOrigin(t *testing.T) {
	s := &Snapshot{FromExternalRebuilder: true}

	t.Run("column absent yields empty fragments", func(t *testing.T) {
		cols, ph, excluded, extra := snapshotTSOptionalOrigin(s, false, 13)
		if cols != "" || ph != "" || excluded != "" || len(extra) != 0 {
			t.Fatalf("expected all-empty fragments, got cols=%q ph=%q excluded=%q extra=%v", cols, ph, excluded, extra)
		}
	})

	t.Run("column present binds the flag after the base args", func(t *testing.T) {
		cols, ph, excluded, extra := snapshotTSOptionalOrigin(s, true, 13)
		if cols != ", from_external_rebuilder" {
			t.Errorf("cols = %q", cols)
		}
		if ph != ", $14" {
			t.Errorf("placeholder = %q, want \", $14\" (13 base args)", ph)
		}
		if !strings.Contains(excluded, "from_external_rebuilder = EXCLUDED.from_external_rebuilder") {
			t.Errorf("excluded fragment missing the ON CONFLICT assignment: %q", excluded)
		}
		if len(extra) != 1 || extra[0] != true {
			t.Errorf("extra args = %v, want [true]", extra)
		}
	})

	t.Run("false flag still binds explicitly", func(t *testing.T) {
		// An upsert over a previously-flagged row must be able to write FALSE
		// back; relying on the column default would silently keep stale taint.
		_, _, _, extra := snapshotTSOptionalOrigin(&Snapshot{}, true, 13)
		if len(extra) != 1 || extra[0] != false {
			t.Errorf("extra args = %v, want [false]", extra)
		}
	})
}
