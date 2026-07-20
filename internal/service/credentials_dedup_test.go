package service

import "testing"

// Create refuses credentials whose hash already backs an active connection, so
// the hash IS the "same exchange account" proxy. Two properties carry it:
// identical credentials collide, and an IG login pinned to two different
// accounts does not — the pin rides in the passphrase, so it must reach the
// hash. Lose either and the gate silently blocks a legitimate second account
// or waves the same account through twice, double-counting its equity in the
// aggregate report.
func TestCredentialsHashSeparatesPinnedAccounts(t *testing.T) {
	base := hashCredentials("api-key", "password", "user1")

	if hashCredentials("api-key", "password", "user1") != base {
		t.Fatal("identical credentials must collide — the dedup gate relies on it")
	}

	pinnedA := hashCredentials("api-key", "password", "user1:Z41WTX")
	pinnedB := hashCredentials("api-key", "password", "user1:Z41WTU")
	if pinnedA == pinnedB {
		t.Fatal("different pinned accounts must hash apart, or a second IG account is blocked as a duplicate")
	}
	if pinnedA == base {
		t.Fatal("pinned and unpinned credentials must hash apart")
	}
}
