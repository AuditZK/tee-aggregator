package connector

import "testing"

// The classification decides what a user is told and whether the sync retries.
// An earlier table paraphrased IBKR's codes from memory: 1014 ("query is
// invalid") and 1011 ("service account is inactive") were retried forever, and
// 1008 ("MTM and FIFO P/L data is not ready") was reported as a bad token.
// Codes here are IBKR's Flex Web Service v3 table.
func TestFlexErrorCodeClassification(t *testing.T) {
	transient := []struct{ code, meaning string }{
		{"1001", "statement could not be generated at this time"},
		{"1004", "statement is incomplete at this time"},
		{"1005", "settlement data is not ready"},
		{"1006", "FIFO P/L data is not ready"},
		{"1007", "MTM P/L data is not ready"},
		{"1008", "MTM and FIFO P/L data is not ready"},
		{"1009", "server under heavy load"},
		{"1018", "too many requests"},
		{"1019", "statement generation in progress"},
		{"1021", "statement could not be retrieved at this time"},
	}
	for _, tc := range transient {
		if !isTransientFlexErrorCode(tc.code) {
			t.Errorf("%s (%s) must be retryable — IBKR says try again shortly", tc.code, tc.meaning)
		}
	}

	permanent := []struct{ code, meaning string }{
		{"1003", "statement is not available"},
		{"1010", "legacy flex queries no longer supported"},
		{"1011", "service account is inactive"},
		{"1012", "token has expired"},
		{"1014", "query is invalid"},
		{"1015", "token is invalid"},
		{"1016", "account is invalid"},
		{"1017", "reference code is invalid"},
		{"1020", "invalid request"},
	}
	for _, tc := range permanent {
		if isTransientFlexErrorCode(tc.code) {
			t.Errorf("%s (%s) must NOT be retried — the account or its configuration has to change first",
				tc.code, tc.meaning)
		}
	}

	// 1013 is neither: the token works and the source address does not.
	if isTransientFlexErrorCode(flexIPRestrictionCode) {
		t.Error("1013 is an IP restriction, not a transient condition")
	}
	if flexIPRestrictionCode != "1013" {
		t.Errorf("flexIPRestrictionCode = %q, want 1013", flexIPRestrictionCode)
	}
}
