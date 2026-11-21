package repository

import (
	"strings"
	"testing"
)

// The keep-alive lists every Saxo connection of every user on a short cycle;
// the listing must not pull a single credential column into memory.
func TestActiveByExchangesQueryReadsNoCredential(t *testing.T) {
	q := strings.ToLower(activeByExchangesQuery(`"userUid"`, `"isActive"`, `"createdAt"`))
	for _, col := range []string{"encrypted", "apikey", "api_key", "secret", "passphrase"} {
		if strings.Contains(q, col) {
			t.Fatalf("query reads %q:\n%s", col, q)
		}
	}
	if !strings.Contains(q, `"isactive" = true`) || !strings.Contains(q, "limit $2") {
		t.Fatalf("query lost its active filter or its cap:\n%s", q)
	}
}
