package server

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

func TestExecutableSHA256_IsTheFileOnDisk(t *testing.T) {
	got := executableSHA256()
	if len(got) != 64 {
		t.Fatalf("want a 64-char hex digest, got %q", got)
	}

	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if want := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("hash %s does not match the executable on disk %s", got, want)
	}

	if again := executableSHA256(); again != got {
		t.Fatalf("second call returned %s, first %s", again, got)
	}
}
