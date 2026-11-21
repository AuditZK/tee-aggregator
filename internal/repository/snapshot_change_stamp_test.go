package repository

import "testing"

func TestSnapshotChangeStampExpr(t *testing.T) {
	cases := []struct {
		name       string
		isTS       bool
		hasUpdated bool
		hasCreated bool
		want       string
	}{
		{"ts schema as production runs it", true, true, true, `COALESCE("updatedAt", "createdAt")`},
		{"go schema has created_at only", false, false, true, "created_at"},
		{"go schema after an updated_at migration", false, true, true, "COALESCE(updated_at, created_at)"},
		{"updated stamp alone", false, true, false, "updated_at"},
		{"no stamp at all", false, false, false, ""},
		{"ts schema with no stamp at all", true, false, false, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := snapshotChangeStampExpr(tc.isTS, tc.hasUpdated, tc.hasCreated); got != tc.want {
				t.Fatalf("snapshotChangeStampExpr(%v, %v, %v) = %q, want %q",
					tc.isTS, tc.hasUpdated, tc.hasCreated, got, tc.want)
			}
		})
	}
}
