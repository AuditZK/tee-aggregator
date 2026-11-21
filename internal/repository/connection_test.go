package repository

import (
	"strings"
	"testing"
)

// SEC-08: the nightly recalibration pass (RecalibrateRebuiltHistories) decrypts
// and POSTs exchange credentials to the external rebuilder. Its connection set
// comes solely from this query, so the query MUST scope egress to connections
// that recorded an explicit opt-in (rebuild_requested_at IS NOT NULL) and MUST
// bound retries with the created-at window. This guards the query shape against
// a regression that silently drops either predicate and re-opens unscoped
// credential egress.
func TestUnfinalizedExternalRebuildsQuery_ConsentGateAndWindow(t *testing.T) {
	q := unfinalizedExternalRebuildsQuery(
		`"user_uid"`, `"created_at"`, `"rebuild_finalized_at"`, `"rebuild_requested_at"`, `"is_active"`,
	)

	mustContain := []string{
		`"rebuild_requested_at" IS NOT NULL`, // consent gate (SEC-08)
		`"rebuild_finalized_at" IS NULL`,     // not yet recalibrated
		`"created_at" < $1`,                  // had time for a midnight snapshot
		`"created_at" > $3`,                  // retry window (SEC-08)
		`"is_active" = true`,
		`lower(exchange) = ANY($2)`,
	}
	for _, want := range mustContain {
		if !strings.Contains(q, want) {
			t.Errorf("recalibration query missing %q — credential egress may be unscoped:\n%s", want, q)
		}
	}
}
