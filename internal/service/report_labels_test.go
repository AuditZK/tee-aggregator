package service

import (
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/signing"
)

// SEC-09: an unparseable date must fall back to the MOST restrictive
// verifiability class so a strict verifier filters the day, not to "live".
func TestToSigningDailyReturns_UnparseableDateIsRestrictive(t *testing.T) {
	daily := []dailyReturn{{date: "not-a-date", netReturn: 0.01}}

	out := toSigningDailyReturns(daily, nil, map[time.Time]struct{}{})
	if len(out) != 1 {
		t.Fatalf("want 1 row, got %d", len(out))
	}
	if out[0].VerifiabilityClass != signing.VerifiabilityClassRebuilderService {
		t.Fatalf("unparseable date class = %q, want %q (most restrictive)",
			out[0].VerifiabilityClass, signing.VerifiabilityClassRebuilderService)
	}
}

// Provenance labelling: tainted day → rebuilder-service; in-enclave
// reconstruction → in-enclave; otherwise live.
func TestToSigningDailyReturns_ProvenanceLabels(t *testing.T) {
	tainted := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	daily := []dailyReturn{
		{date: "2026-03-01"}, // tainted (external rebuilder)
		{date: "2026-03-02"}, // in-enclave reconstruction
		{date: "2026-03-03"}, // live
	}
	snaps := []*repository.Snapshot{
		{Timestamp: time.Date(2026, 3, 2, 9, 0, 0, 0, time.UTC), IsHistorical: true, FromExternalRebuilder: false},
	}
	taint := map[time.Time]struct{}{tainted: {}}

	out := toSigningDailyReturns(daily, snaps, taint)
	want := []string{
		signing.VerifiabilityClassRebuilderService,
		signing.VerifiabilityClassInEnclave,
		signing.VerifiabilityClassLive,
	}
	for i, w := range want {
		if out[i].VerifiabilityClass != w {
			t.Errorf("day %s class = %q, want %q", daily[i].date, out[i].VerifiabilityClass, w)
		}
	}
}
