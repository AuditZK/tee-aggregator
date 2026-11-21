package service

import (
	"testing"
	"time"
)

func TestCacheIsFresh(t *testing.T) {
	signedAt := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)

	cases := []struct {
		name                 string
		cachedAt             time.Time
		latestSnapshotChange time.Time
		want                 bool
	}{
		{"no write stamp available", signedAt, time.Time{}, true},
		{"snapshots untouched since signing", signedAt, signedAt.Add(-72 * time.Hour), true},
		{"last write at the signing instant", signedAt, signedAt, true},
		{"history rebuilt one second later", signedAt, signedAt.Add(time.Second), false},
		{"history rebuilt a day later", signedAt, signedAt.Add(24 * time.Hour), false},
		{"unstamped cache yields to any known write", time.Time{}, signedAt, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cacheIsFresh(tc.cachedAt, tc.latestSnapshotChange); got != tc.want {
				t.Fatalf("cacheIsFresh(%s, %s) = %v, want %v",
					tc.cachedAt, tc.latestSnapshotChange, got, tc.want)
			}
		})
	}
}

// The comparison must survive a stamp read back in another location: pgx
// hands back whatever timezone the session carries, and a naive field-by-field
// comparison would call a rebuilt history fresh.
func TestCacheIsFreshAcrossLocations(t *testing.T) {
	signedAt := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)
	rebuiltAt := signedAt.Add(time.Hour).In(time.FixedZone("UTC+9", 9*3600))

	if cacheIsFresh(signedAt, rebuiltAt) {
		t.Fatal("a rebuild an hour after signing reads as fresh when the stamp is not UTC")
	}
	if !cacheIsFresh(signedAt, signedAt.Add(-time.Hour).In(time.FixedZone("UTC-5", -5*3600))) {
		t.Fatal("a write an hour before signing reads as stale when the stamp is not UTC")
	}
}
