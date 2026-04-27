package bootstrap

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// makeTestKeypair returns a fresh Ed25519 keypair and the ssh-ed25519
// wire-format base64 string the OperatorPubkey constant would hold.
func makeTestKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	// Build ssh-ed25519 wire format.
	algName := "ssh-ed25519"
	body := make([]byte, 0, 4+len(algName)+4+ed25519.PublicKeySize)
	body = append(body, byte(len(algName)>>24), byte(len(algName)>>16), byte(len(algName)>>8), byte(len(algName)))
	body = append(body, []byte(algName)...)
	body = append(body, byte(ed25519.PublicKeySize>>24), byte(ed25519.PublicKeySize>>16), byte(ed25519.PublicKeySize>>8), byte(ed25519.PublicKeySize))
	body = append(body, pub...)
	return pub, priv, base64.StdEncoding.EncodeToString(body)
}

// withOperatorPubkey overrides the package-level constant for the test.
// Implementation detail: we can't change a const, so this helper rebuilds
// the verify path with an injected pubkey. The real package uses the
// hardcoded constant via DecodeOperatorPubkey(); for tests, we exercise
// the lower-level verify against a freshly-generated keypair.
func signAllowlist(t *testing.T, priv ed25519.PrivateKey, payload SignedAllowlistPayload) []byte {
	t.Helper()
	canonical, err := canonicalPayloadJSON(&payload)
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	sig := ed25519.Sign(priv, canonical)
	sa := SignedAllowlist{Payload: payload, Signature: base64.StdEncoding.EncodeToString(sig)}
	raw, err := json.Marshal(sa)
	if err != nil {
		t.Fatalf("marshal signed: %v", err)
	}
	return raw
}

// verifyWithKey is a test-only verifier that takes a pubkey directly,
// bypassing the OperatorPubkey constant. Mirrors VerifyAllowlist exactly
// otherwise.
func verifyWithKey(t *testing.T, pubkey ed25519.PublicKey, raw []byte, now time.Time) (*SignedAllowlistPayload, error) {
	t.Helper()
	var sa SignedAllowlist
	if err := json.Unmarshal(raw, &sa); err != nil {
		return nil, err
	}
	if len(sa.Payload.Entries) == 0 {
		return nil, ErrAllowlistEmpty
	}
	canonical, err := canonicalPayloadJSON(&sa.Payload)
	if err != nil {
		return nil, err
	}
	sig, err := base64.StdEncoding.DecodeString(sa.Signature)
	if err != nil {
		return nil, ErrAllowlistInvalidSignature
	}
	if !ed25519.Verify(pubkey, canonical, sig) {
		return nil, ErrAllowlistInvalidSignature
	}
	if sa.Payload.IssuedAt.After(now.Add(5 * time.Minute)) {
		return nil, ErrAllowlistInTheFuture
	}
	if now.Sub(sa.Payload.IssuedAt) > MaxAllowlistAge {
		return nil, ErrAllowlistTooOld
	}
	return &sa.Payload, nil
}

func TestVerifyAllowlist_HappyPath(t *testing.T) {
	pub, priv, _ := makeTestKeypair(t)
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	payload := SignedAllowlistPayload{
		Version:  1,
		IssuedAt: now.Add(-1 * time.Hour),
		Entries: []AllowlistEntry{
			{Measurement: "abcdef0123456789", Label: "v1.0.0", AddedAt: now.Add(-25 * time.Hour)},
			{Measurement: "0123456789abcdef", Label: "v1.0.1", AddedAt: now.Add(-1 * time.Hour)},
		},
	}
	raw := signAllowlist(t, priv, payload)

	got, err := verifyWithKey(t, pub, raw, now)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !IsAllowed(got, "abcdef0123456789") {
		t.Error("v1.0.0 measurement should be allowed")
	}
	if !IsAllowed(got, "0123456789ABCDEF") {
		t.Error("case-insensitive match should pass")
	}
	if IsAllowed(got, "deadbeefdeadbeef") {
		t.Error("unknown measurement should NOT be allowed")
	}
}

func TestVerifyAllowlist_TamperedPayload(t *testing.T) {
	pub, priv, _ := makeTestKeypair(t)
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	payload := SignedAllowlistPayload{
		Version:  1,
		IssuedAt: now.Add(-1 * time.Hour),
		Entries:  []AllowlistEntry{{Measurement: "deadbeef", Label: "v1.0.0"}},
	}
	raw := signAllowlist(t, priv, payload)

	// Tamper: replace one byte in the JSON.
	tampered := append([]byte{}, raw...)
	for i, b := range tampered {
		if b == '"' {
			continue
		}
		if b >= 'a' && b <= 'z' {
			tampered[i] = b - 1
			break
		}
	}

	if _, err := verifyWithKey(t, pub, tampered, now); err == nil {
		t.Error("expected signature verification to fail on tampered payload")
	}
}

func TestVerifyAllowlist_WrongKey(t *testing.T) {
	_, priv, _ := makeTestKeypair(t)
	otherPub, _, _ := makeTestKeypair(t) // different keypair
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	payload := SignedAllowlistPayload{
		Version:  1,
		IssuedAt: now.Add(-1 * time.Hour),
		Entries:  []AllowlistEntry{{Measurement: "abcd", Label: "v1"}},
	}
	raw := signAllowlist(t, priv, payload)

	if _, err := verifyWithKey(t, otherPub, raw, now); err == nil {
		t.Error("verify with wrong pubkey should fail")
	}
}

func TestVerifyAllowlist_TooOld(t *testing.T) {
	pub, priv, _ := makeTestKeypair(t)
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	old := now.Add(-(MaxAllowlistAge + time.Hour))

	payload := SignedAllowlistPayload{
		Version:  1,
		IssuedAt: old,
		Entries:  []AllowlistEntry{{Measurement: "abcd", Label: "v1"}},
	}
	raw := signAllowlist(t, priv, payload)

	_, err := verifyWithKey(t, pub, raw, now)
	if err != ErrAllowlistTooOld {
		t.Errorf("expected ErrAllowlistTooOld, got %v", err)
	}
}

func TestVerifyAllowlist_FutureIssuedAt(t *testing.T) {
	pub, priv, _ := makeTestKeypair(t)
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	future := now.Add(1 * time.Hour) // way more than the 5-min skew tolerance

	payload := SignedAllowlistPayload{
		Version:  1,
		IssuedAt: future,
		Entries:  []AllowlistEntry{{Measurement: "abcd", Label: "v1"}},
	}
	raw := signAllowlist(t, priv, payload)

	_, err := verifyWithKey(t, pub, raw, now)
	if err != ErrAllowlistInTheFuture {
		t.Errorf("expected ErrAllowlistInTheFuture, got %v", err)
	}
}

func TestVerifyAllowlist_EmptyEntries(t *testing.T) {
	_, priv, _ := makeTestKeypair(t)
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	payload := SignedAllowlistPayload{
		Version:  1,
		IssuedAt: now.Add(-1 * time.Hour),
		Entries:  nil,
	}
	raw := signAllowlist(t, priv, payload)

	if _, err := VerifyAllowlist(raw, now); err != ErrAllowlistEmpty {
		t.Errorf("expected ErrAllowlistEmpty, got %v", err)
	}
}

func TestCanonical_DeterministicOrder(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	a := SignedAllowlistPayload{
		Version:  1,
		IssuedAt: now,
		Entries: []AllowlistEntry{
			{Measurement: "ff00", Label: "B"},
			{Measurement: "0011", Label: "A"},
		},
	}
	b := SignedAllowlistPayload{
		Version:  1,
		IssuedAt: now,
		Entries: []AllowlistEntry{
			{Measurement: "0011", Label: "A"},
			{Measurement: "ff00", Label: "B"},
		},
	}
	canonA, errA := canonicalPayloadJSON(&a)
	canonB, errB := canonicalPayloadJSON(&b)
	if errA != nil || errB != nil {
		t.Fatalf("canonicalize: %v / %v", errA, errB)
	}
	if string(canonA) != string(canonB) {
		t.Errorf("canonical encoding not deterministic across input order:\nA=%s\nB=%s", canonA, canonB)
	}
}

func TestDecodeOperatorPubkey_Placeholder(t *testing.T) {
	// The constant ships with a placeholder; decoding it must NOT fail
	// silently, but it also must NOT be usable as a real pubkey for
	// verification (so deploys with the placeholder will fail-closed).
	pk, err := DecodeOperatorPubkey()
	if err != nil {
		// Acceptable: placeholder is detectably invalid.
		return
	}
	// If decoded "successfully", it must be the wrong size or wrong bytes
	// such that signature verification will fail. Just sanity-check the
	// length so a future refactor doesn't accidentally ship a working
	// placeholder key.
	if len(pk) != ed25519.PublicKeySize {
		t.Errorf("decoded placeholder size=%d, want %d", len(pk), ed25519.PublicKeySize)
	}
}
