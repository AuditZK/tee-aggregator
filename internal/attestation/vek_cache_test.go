package attestation

import (
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
)

func newCacheService(t *testing.T) *Service {
	t.Helper()
	return &Service{
		vcekCacheDir: t.TempDir(),
		logger:       zap.NewNop(),
	}
}

func writeCacheFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("seed %s: %v", name, err)
	}
	return path
}

// ATT-001: a cached VEK that no longer verifies must be moved out of the way,
// or the 7-day TTL keeps serving it and production refuses to start for as long
// as it lasts. This is the whole point of the fix — it must not regress.
func TestInvalidateVEKCache_MovesVEKAsideAndKeepsASK(t *testing.T) {
	s := newCacheService(t)
	vcek := writeCacheFile(t, s.vcekCacheDir, vcekPEMFile, "stale-vcek")
	vlek := writeCacheFile(t, s.vcekCacheDir, vlekPEMFile, "stale-vlek")
	ask := writeCacheFile(t, s.vcekCacheDir, askPEMFile, "amd-intermediate")

	s.invalidateVEKCache("test")

	for _, p := range []string{vcek, vlek} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s still present after invalidation (err=%v)", filepath.Base(p), err)
		}
		stale := p + ".stale"
		if _, err := os.Stat(stale); err != nil {
			t.Errorf("%s.stale missing — the evidence needed to confirm a host migration was discarded", filepath.Base(p))
		}
	}

	// The ASK is the AMD intermediate and stays valid across chips; dropping it
	// would force a needless KDS round-trip on every recovery.
	if got, err := os.ReadFile(ask); err != nil || string(got) != "amd-intermediate" {
		t.Errorf("ASK was touched: content=%q err=%v", string(got), err)
	}
}

// Invalidation runs on a failure path, so it must tolerate a cache that is
// missing, partially populated, or already invalidated once.
func TestInvalidateVEKCache_IsIdempotentAndTolerant(t *testing.T) {
	s := newCacheService(t)

	// Nothing cached at all.
	s.invalidateVEKCache("empty cache")

	// Only a VCEK, invalidated twice. The second pass must overwrite the first
	// .stale slot rather than fail or accumulate files.
	vcek := writeCacheFile(t, s.vcekCacheDir, vcekPEMFile, "first")
	s.invalidateVEKCache("first pass")
	writeCacheFile(t, s.vcekCacheDir, vcekPEMFile, "second")
	s.invalidateVEKCache("second pass")

	got, err := os.ReadFile(vcek + ".stale")
	if err != nil {
		t.Fatalf("read stale slot: %v", err)
	}
	if string(got) != "second" {
		t.Errorf("stale slot = %q, want the most recent bad cert %q", string(got), "second")
	}

	entries, err := os.ReadDir(s.vcekCacheDir)
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("cache dir holds %d entries, want 1 — the .stale slot must not accumulate", len(entries))
	}
}

// A missing cache directory must not panic: invalidation is called from the
// attestation failure path, which also runs on hosts that never cached.
func TestInvalidateVEKCache_MissingDirIsSafe(t *testing.T) {
	s := &Service{
		vcekCacheDir: filepath.Join(t.TempDir(), "does-not-exist"),
		logger:       zap.NewNop(),
	}
	s.invalidateVEKCache("missing dir")
}
