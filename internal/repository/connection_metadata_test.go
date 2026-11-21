package repository

import (
	"strings"
	"testing"
)

// The paper/live flag decides whether a track record is a demo account, and the
// report badge reads it from this column. Prod runs the TS Prisma schema, where
// the column is "isPaper": a hardcoded snake_case name updates nothing, Postgres
// answers 42703, and every account reads as live. Both writes must go through
// the schema-aware column name.
func TestUpdateConnectionMetadataQuery_UsesSchemaAwareColumns(t *testing.T) {
	ts := &ConnectionRepo{isTSSchema: true, capabilitiesLoaded: true}
	pg := &ConnectionRepo{isTSSchema: false, capabilitiesLoaded: true}

	cases := []struct {
		name  string
		repo  *ConnectionRepo
		col   string
		want  string
		unfit string
	}{
		{"ts is_paper", ts, "is_paper", `"isPaper" = $1`, `is_paper =`},
		{"ts kyc_level", ts, "kyc_level", `"kycLevel" = $1`, `kyc_level =`},
		{"go is_paper", pg, "is_paper", `"is_paper" = $1`, `"isPaper"`},
		{"go kyc_level", pg, "kyc_level", `"kyc_level" = $1`, `"kycLevel"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := updateConnectionMetadataQuery(tc.repo.qcol(tc.col), tc.repo.qcol("updated_at"))
			if !strings.Contains(q, tc.want) {
				t.Errorf("query missing %q — the write targets a column that does not exist:\n%s", tc.want, q)
			}
			if strings.Contains(q, tc.unfit) {
				t.Errorf("query contains %q, which belongs to the other schema:\n%s", tc.unfit, q)
			}
			if !strings.Contains(q, `WHERE id = $3`) {
				t.Errorf("query lost its row predicate:\n%s", q)
			}
		})
	}
}

func TestUpdateConnectionMetadataQuery_StampsUpdatedAt(t *testing.T) {
	ts := &ConnectionRepo{isTSSchema: true, capabilitiesLoaded: true}
	q := updateConnectionMetadataQuery(ts.qcol("is_paper"), ts.qcol("updated_at"))
	if !strings.Contains(q, `"updatedAt" = $2`) {
		t.Errorf("query does not stamp updatedAt:\n%s", q)
	}
}
