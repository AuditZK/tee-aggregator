package encryption

import (
	"bytes"
	"strings"
	"testing"
)

func boundTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := New(bytes.Repeat([]byte{0x24}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestTSBoundRoundTrip(t *testing.T) {
	svc := boundTestService(t)
	aad := ConnectionFieldAAD("user-a", "cuid_row1", "api_key")

	stored, err := svc.EncryptTSBound("synthetic-secret", aad)
	if err != nil {
		t.Fatal(err)
	}
	if !IsBoundTS(stored) || strings.Contains(stored, "synthetic-secret") {
		t.Fatalf("stored value %q is not a bound ciphertext", stored)
	}
	got, err := svc.DecryptTSBound(stored, aad)
	if err != nil || got != "synthetic-secret" {
		t.Fatalf("round trip = %q, %v", got, err)
	}
}

func TestTSBoundRefusesAnotherRowOrField(t *testing.T) {
	svc := boundTestService(t)
	stored, err := svc.EncryptTSBound("synthetic-secret", ConnectionFieldAAD("user-a", "cuid_row1", "api_key"))
	if err != nil {
		t.Fatal(err)
	}
	for name, aad := range map[string][]byte{
		"other row":   ConnectionFieldAAD("user-a", "cuid_row2", "api_key"),
		"other user":  ConnectionFieldAAD("user-b", "cuid_row1", "api_key"),
		"other field": ConnectionFieldAAD("user-a", "cuid_row1", "api_secret"),
		"no aad":      nil,
	} {
		if _, err := svc.DecryptTSBound(stored, aad); err == nil {
			t.Errorf("%s: bound ciphertext opened", name)
		}
	}
}

func TestTSBoundAndLegacyFormatsDoNotCross(t *testing.T) {
	svc := boundTestService(t)
	aad := ConnectionFieldAAD("user-a", "cuid_row1", "api_key")

	legacy, err := svc.EncryptTSString("synthetic-secret")
	if err != nil {
		t.Fatal(err)
	}
	if IsBoundTS(legacy) {
		t.Fatal("legacy ciphertext reads as bound")
	}
	if _, err := svc.DecryptTSBound(legacy, aad); err == nil {
		t.Error("a legacy ciphertext opened as a bound one")
	}

	bound, err := svc.EncryptTSBound("synthetic-secret", aad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DecryptTSString(bound); err == nil {
		t.Error("a bound ciphertext opened without its aad")
	}
	if _, err := svc.EncryptTSBound("x", nil); err == nil {
		t.Error("sealed with an empty aad")
	}
}
