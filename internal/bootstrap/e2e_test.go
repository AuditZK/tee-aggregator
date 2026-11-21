package bootstrap

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/attestation"
)

// TestHandoffE2E_ProductionServer wires the *real* HandoffServer (not a
// mock) to the *real* FetchMasterKey client and verifies the full
// round-trip works. The OperatorPubkey is swapped for a test keypair
// via SetOperatorPubkeyForTest to bypass the placeholder.
//
// This is the single test that proves: client → handoff endpoint →
// server-side allowlist check → server-side attestation chain check →
// server-side ECIES encrypt → client decrypts → master key bytes match.
func TestHandoffE2E_ProductionServer(t *testing.T) {
	// 1. Operator keypair (Ed25519) — swap into the package var.
	opPub, opPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("op keygen: %v", err)
	}
	restore := SetOperatorPubkeyForTest(encodeOperatorPubkeyForTest(opPub))
	defer restore()

	// 2. Fixed master key the predecessor will hand off.
	masterKey := bytes.Repeat([]byte{0xCA}, 32)
	exporter := &fakeExporter{key: masterKey}

	// 3. Successor's ECIES keypair — privkey lives "in the TEE", only
	// the test holds it because we are simulating both sides.
	successorPriv, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ecdh: %v", err)
	}
	successorPubBytes := successorPriv.PublicKey().Bytes()
	successorE2EPEM := "-----BEGIN PUBLIC KEY-----\n" +
		base64.StdEncoding.EncodeToString(successorPubBytes) +
		"\n-----END PUBLIC KEY-----\n"

	// 4. Frozen clock so the SignedAllowlist freshness check is stable.
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	measurement := "1206abcdef1234567890"

	signedAllowlist := buildSignedAllowlist(t, opPriv, []string{measurement}, now.Add(-1*time.Hour))

	// 5. Spin up the *real* HandoffServer.
	srv, err := NewHandoffServer(HandoffServerOptions{
		KeyExporter: exporter,
		NowFn:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	httpSrv := httptest.NewTLSServer(srv)
	defer httpSrv.Close()

	// 6. Run the production client against the production server.
	got, err := FetchMasterKey(context.Background(), HandoffClientOptions{
		PeerURL:         httpSrv.URL,
		SignedAllowlist: signedAllowlist,
		AttestationSvc: &fakeAttestationSvc{
			measurement:  measurement,
			tlsFP:        "FAKE_TLS_FP_FOR_E2E",
			e2ePEM:       successorE2EPEM,
			signingPK:    "FAKE_SIGNING_PK_FOR_E2E",
			platform:     attestation.PlatformSevSnp,
			verified:     true,
			vcekVerified: true,
			rdBound:      true,
		},
		ECIESPriv:     successorPriv,
		ClientVersion: "go-enclave-vN+1-e2e",
		HTTPClient:    httpSrv.Client(),
	})
	if err != nil {
		t.Fatalf("FetchMasterKey: %v", err)
	}
	if !bytes.Equal(got, masterKey) {
		t.Fatalf("master key mismatch:\nwant=%x\ngot =%x", masterKey, got)
	}
}

// TestHandoffE2E_RejectsUnsignedAllowlist proves the predecessor REFUSES
// to release the master key when the allowlist signature does not match
// the OperatorPubkey — even when every other check (attestation,
// measurement match, nonce) would pass. This is the property that makes
// the operator's signing key the cryptographic root of trust.
func TestHandoffE2E_RejectsUnsignedAllowlist(t *testing.T) {
	// Real operator pubkey — successor will sign with this.
	opPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("real op keygen: %v", err)
	}
	restore := SetOperatorPubkeyForTest(encodeOperatorPubkeyForTest(opPub))
	defer restore()

	// ATTACKER's signing key — this will sign the allowlist the test
	// will submit. The HandoffServer must reject it because it does
	// not match the OperatorPubkey.
	_, attackerPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("attacker keygen: %v", err)
	}

	masterKey := bytes.Repeat([]byte{0xDE}, 32)
	successorPriv, _ := ecdh.P256().GenerateKey(rand.Reader)
	successorE2EPEM := "-----BEGIN PUBLIC KEY-----\n" +
		base64.StdEncoding.EncodeToString(successorPriv.PublicKey().Bytes()) +
		"\n-----END PUBLIC KEY-----\n"
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	measurement := "1206abcdef1234567890"
	// Allowlist signed by the ATTACKER, not the operator.
	signedAllowlist := buildSignedAllowlist(t, attackerPriv, []string{measurement}, now.Add(-1*time.Hour))

	srv, _ := NewHandoffServer(HandoffServerOptions{
		KeyExporter: &fakeExporter{key: masterKey},
		NowFn:       func() time.Time { return now },
	})
	httpSrv := httptest.NewTLSServer(srv)
	defer httpSrv.Close()

	_, err = FetchMasterKey(context.Background(), HandoffClientOptions{
		PeerURL:         httpSrv.URL,
		SignedAllowlist: signedAllowlist,
		AttestationSvc: &fakeAttestationSvc{
			measurement:  measurement,
			tlsFP:        "FP",
			e2ePEM:       successorE2EPEM,
			signingPK:    "SPK",
			platform:     attestation.PlatformSevSnp,
			verified:     true,
			vcekVerified: true,
			rdBound:      true,
		},
		ECIESPriv:     successorPriv,
		ClientVersion: "attacker-build",
		HTTPClient:    httpSrv.Client(),
	})
	if err == nil {
		t.Fatal("expected handoff to fail because allowlist is signed by an attacker, but it succeeded")
	}
}

// TestHandoffE2E_RejectsMeasurementOutsideAllowlist verifies the
// predecessor refuses to release the master key when the successor's
// measurement is not on the allowlist — even if the allowlist is
// otherwise correctly signed.
func TestHandoffE2E_RejectsMeasurementOutsideAllowlist(t *testing.T) {
	opPub, opPriv, _ := ed25519.GenerateKey(rand.Reader)
	restore := SetOperatorPubkeyForTest(encodeOperatorPubkeyForTest(opPub))
	defer restore()

	masterKey := bytes.Repeat([]byte{0xAD}, 32)
	successorPriv, _ := ecdh.P256().GenerateKey(rand.Reader)
	successorE2EPEM := "-----BEGIN PUBLIC KEY-----\n" +
		base64.StdEncoding.EncodeToString(successorPriv.PublicKey().Bytes()) +
		"\n-----END PUBLIC KEY-----\n"
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	// Allowlist contains version "good_measurement", but successor
	// presents a different one. Both must be valid hex.
	signedAllowlist := buildSignedAllowlist(t, opPriv, []string{"abcd1234abcd1234"}, now.Add(-1*time.Hour))

	srv, _ := NewHandoffServer(HandoffServerOptions{
		KeyExporter: &fakeExporter{key: masterKey},
		NowFn:       func() time.Time { return now },
	})
	httpSrv := httptest.NewTLSServer(srv)
	defer httpSrv.Close()

	_, err := FetchMasterKey(context.Background(), HandoffClientOptions{
		PeerURL:         httpSrv.URL,
		SignedAllowlist: signedAllowlist,
		AttestationSvc: &fakeAttestationSvc{
			measurement:  "rogue_measurement_999", // NOT on the list
			tlsFP:        "FP",
			e2ePEM:       successorE2EPEM,
			signingPK:    "SPK",
			platform:     attestation.PlatformSevSnp,
			verified:     true,
			vcekVerified: true,
			rdBound:      true,
		},
		ECIESPriv:     successorPriv,
		ClientVersion: "rogue-build",
		HTTPClient:    httpSrv.Client(),
	})
	if err == nil {
		t.Fatal("expected handoff to fail for off-allowlist measurement, got nil")
	}
}

// TestHandoffE2E_RejectsUnattestedSuccessor proves a successor without
// a verified SEV-SNP attestation cannot fetch the master key — even
// when the operator-signed allowlist is correct and the measurement is
// valid. This is the silicon binding the trust model requires.
func TestHandoffE2E_RejectsUnattestedSuccessor(t *testing.T) {
	opPub, opPriv, _ := ed25519.GenerateKey(rand.Reader)
	restore := SetOperatorPubkeyForTest(encodeOperatorPubkeyForTest(opPub))
	defer restore()

	masterKey := bytes.Repeat([]byte{0xBE}, 32)
	successorPriv, _ := ecdh.P256().GenerateKey(rand.Reader)
	successorE2EPEM := "-----BEGIN PUBLIC KEY-----\n" +
		base64.StdEncoding.EncodeToString(successorPriv.PublicKey().Bytes()) +
		"\n-----END PUBLIC KEY-----\n"
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	measurement := "1206abcdef1234567890"
	signedAllowlist := buildSignedAllowlist(t, opPriv, []string{measurement}, now.Add(-1*time.Hour))

	srv, _ := NewHandoffServer(HandoffServerOptions{
		KeyExporter: &fakeExporter{key: masterKey},
		NowFn:       func() time.Time { return now },
	})
	httpSrv := httptest.NewTLSServer(srv)
	defer httpSrv.Close()

	_, err := FetchMasterKey(context.Background(), HandoffClientOptions{
		PeerURL:         httpSrv.URL,
		SignedAllowlist: signedAllowlist,
		AttestationSvc: &fakeAttestationSvc{
			measurement:  measurement,
			tlsFP:        "FP",
			e2ePEM:       successorE2EPEM,
			signingPK:    "SPK",
			platform:     attestation.PlatformSevSnp,
			verified:     false, // ← attestation NOT verified
			vcekVerified: true,
			rdBound:      true,
		},
		ECIESPriv:     successorPriv,
		ClientVersion: "unattested-build",
		HTTPClient:    httpSrv.Client(),
	})
	if err == nil {
		t.Fatal("expected handoff to fail when attestation is unverified, got nil")
	}
}

// encodeOperatorPubkeyForTest returns the ssh-ed25519 wire-format
// base64 string for an Ed25519 public key — the same shape the
// production code expects in the OperatorPubkey constant.
func encodeOperatorPubkeyForTest(pub ed25519.PublicKey) string {
	algName := "ssh-ed25519"
	body := make([]byte, 0, 4+len(algName)+4+ed25519.PublicKeySize)
	body = append(body, byte(len(algName)>>24), byte(len(algName)>>16), byte(len(algName)>>8), byte(len(algName)))
	body = append(body, []byte(algName)...)
	body = append(body, byte(ed25519.PublicKeySize>>24), byte(ed25519.PublicKeySize>>16),
		byte(ed25519.PublicKeySize>>8), byte(ed25519.PublicKeySize))
	body = append(body, pub...)
	return base64.StdEncoding.EncodeToString(body)
}
