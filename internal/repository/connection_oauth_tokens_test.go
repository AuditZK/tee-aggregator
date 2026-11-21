package repository

import (
	"strings"
	"testing"
)

// E-H3: cTrader rotates single-use refresh tokens. GetByUserExchangeLabel
// matches the label with TRIM(label) = TRIM($3); the OAuth-token UPDATE used a
// bare `label = $6`. On a legacy row whose label carries whitespace the read
// therefore succeeded and the write matched nothing — the connector believed
// the rotated token was saved, the row kept the consumed one, and the
// connection died at the next refresh with ACCESS_DENIED.
func TestUpdateOAuthTokensQuery_LabelPredicateIsTrimAligned(t *testing.T) {
	for _, tc := range []struct {
		name string
		isTS bool
		cols []string
	}{
		{"prisma/camelCase", true, []string{`"userUid" = $4`, `"isActive" = true`}},
		{"native/snake_case", false, []string{`user_uid = $4`, `is_active = true`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := updateOAuthTokensQuery(tc.isTS)
			if !strings.Contains(q, "TRIM(label) = TRIM($6)") {
				t.Errorf("label predicate is not TRIM-aligned with the reads:\n%s", q)
			}
			if strings.Contains(q, "AND label = $6") {
				t.Errorf("bare label comparison is back:\n%s", q)
			}
			for _, want := range tc.cols {
				if !strings.Contains(q, want) {
					t.Errorf("query missing %q:\n%s", want, q)
				}
			}
		})
	}
}
