package encryption

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

// TestVerifyECDSAReportSignature covers the ECDSA verifier used in
// tryMeasurementRecovery to filter corrupt or tampered signed_reports rows.
func TestVerifyECDSAReportSignature(t *testing.T) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	reportHash := hex.EncodeToString([]byte("synthetic-report-hash-32bytes!!!"))
	digest := sha256.Sum256([]byte(reportHash))
	sigDER, err := ecdsa.SignASN1(rand.Reader, privKey, digest[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	sigB64 := base64.StdEncoding.EncodeToString(sigDER)

	pubDER, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if err != nil {
		t.Fatalf("marshal pubkey: %v", err)
	}
	pubB64 := base64.StdEncoding.EncodeToString(pubDER)

	t.Run("valid signature", func(t *testing.T) {
		ok, err := verifyECDSAReportSignature(reportHash, sigB64, pubB64, "ECDSA-P256-SHA256")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("expected valid signature")
		}
	})

	t.Run("empty algorithm defaults to ECDSA", func(t *testing.T) {
		ok, err := verifyECDSAReportSignature(reportHash, sigB64, pubB64, "")
		if err != nil || !ok {
			t.Fatalf("empty algorithm should default to ECDSA: ok=%v err=%v", ok, err)
		}
	})

	t.Run("tampered report hash", func(t *testing.T) {
		ok, err := verifyECDSAReportSignature("different-hash", sigB64, pubB64, "ECDSA-P256-SHA256")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected invalid signature for tampered hash")
		}
	})

	t.Run("tampered signature", func(t *testing.T) {
		badSig := base64.StdEncoding.EncodeToString([]byte("not-a-valid-der-signature"))
		ok, err := verifyECDSAReportSignature(reportHash, badSig, pubB64, "ECDSA-P256-SHA256")
		if ok {
			t.Fatal("expected false for malformed signature")
		}
		_ = err // malformed DER may or may not return an error depending on parse depth
	})

	t.Run("unsupported algorithm", func(t *testing.T) {
		_, err := verifyECDSAReportSignature(reportHash, sigB64, pubB64, "Ed25519")
		if err == nil {
			t.Fatal("expected error for unsupported algorithm")
		}
	})

	t.Run("malformed public key", func(t *testing.T) {
		_, err := verifyECDSAReportSignature(reportHash, sigB64, "bm90YmFzZTY0", "ECDSA-P256-SHA256")
		if err == nil {
			t.Fatal("expected error for malformed public key")
		}
	})
}

// TestNewKeyDerivationServiceFromMeasurementBytes verifies that deriving from
// the same measurement bytes produces the same master key, and that different
// measurement bytes produce different keys.
func TestNewKeyDerivationServiceFromMeasurementBytes(t *testing.T) {
	measurement := make([]byte, 48)
	if _, err := rand.Read(measurement); err != nil {
		t.Fatalf("rand: %v", err)
	}

	svc1, err := newKeyDerivationServiceFromMeasurementBytes(measurement, nil)
	if err != nil {
		t.Fatalf("derive 1: %v", err)
	}
	svc2, err := newKeyDerivationServiceFromMeasurementBytes(measurement, nil)
	if err != nil {
		t.Fatalf("derive 2: %v", err)
	}
	if svc1.GetMasterKeyID() != svc2.GetMasterKeyID() {
		t.Fatal("same measurement must produce same master_key_id")
	}

	other := make([]byte, 48)
	if _, err := rand.Read(other); err != nil {
		t.Fatalf("rand: %v", err)
	}
	svc3, err := newKeyDerivationServiceFromMeasurementBytes(other, nil)
	if err != nil {
		t.Fatalf("derive 3: %v", err)
	}
	if svc1.GetMasterKeyID() == svc3.GetMasterKeyID() {
		t.Fatal("different measurements must produce different master_key_ids")
	}

	t.Run("empty measurement returns error", func(t *testing.T) {
		_, err := newKeyDerivationServiceFromMeasurementBytes(nil, nil)
		if err == nil {
			t.Fatal("expected error for nil measurement")
		}
	})
}

// TestMeasurementRecoveryWrapUnwrap verifies the round-trip: DEK wrapped with
// key derived from measurement A can be unwrapped by a service derived from the
// same measurement A, and not from measurement B.
func TestMeasurementRecoveryWrapUnwrap(t *testing.T) {
	measurementA := make([]byte, 48)
	measurementB := make([]byte, 48)
	rand.Read(measurementA)
	rand.Read(measurementB)

	svcA, err := newKeyDerivationServiceFromMeasurementBytes(measurementA, nil)
	if err != nil {
		t.Fatalf("derive A: %v", err)
	}
	svcB, err := newKeyDerivationServiceFromMeasurementBytes(measurementB, nil)
	if err != nil {
		t.Fatalf("derive B: %v", err)
	}

	dek := make([]byte, 32)
	rand.Read(dek)

	wrapped, err := svcA.WrapKey(dek)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}

	// Same measurement → must unwrap successfully.
	got, err := svcA.UnwrapKey(wrapped)
	if err != nil {
		t.Fatalf("unwrap with correct key: %v", err)
	}
	if string(got) != string(dek) {
		t.Fatal("unwrapped DEK does not match original")
	}

	// Different measurement → must fail.
	_, err = svcB.UnwrapKey(wrapped)
	if err == nil {
		t.Fatal("expected unwrap failure for wrong measurement key")
	}
}
