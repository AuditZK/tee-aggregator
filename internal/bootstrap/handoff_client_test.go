package bootstrap

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/attestation"
	"github.com/trackrecord/enclave/internal/encryption"
)

// fakeAttestationSvc returns a synthetic attestation pre-built for the
// handoff scenario. The successor (client) gets to "attest" with a
// fixed measurement and bind the nonce into report_data via the same
// hash function the server expects.
type fakeAttestationSvc struct {
	measurement   string
	tlsFP         string
	e2ePEM        string
	signingPK     string
	platform      string
	verified      bool
	vcekVerified  bool
	rdBound       bool
}

func (f *fakeAttestationSvc) GetAttestationWithNonce(_ context.Context, nonce []byte) (*attestation.AttestationReport, error) {
	rd := computeExpectedReportData(&attestation.AttestationReport{
		TLSBinding:    &attestation.TLSBinding{Fingerprint: f.tlsFP},
		E2EEncryption: &attestation.E2EInfo{PublicKey: f.e2ePEM},
		ReportSigning: &attestation.SigningInfo{PublicKey: f.signingPK},
	}, nonce)
	return &attestation.AttestationReport{
		Platform: f.platform,
		Attestation: &attestation.SevSnpReport{
			Measurement:              f.measurement,
			ReportData:               rd,
			Verified:                 f.verified,
			VcekVerified:             f.vcekVerified,
			ReportDataBoundToRequest: f.rdBound,
		},
		TLSBinding:    &attestation.TLSBinding{Fingerprint: f.tlsFP},
		E2EEncryption: &attestation.E2EInfo{PublicKey: f.e2ePEM},
		ReportSigning: &attestation.SigningInfo{PublicKey: f.signingPK},
	}, nil
}

// stubVerifyAllowlist replaces VerifyAllowlist for the duration of the
// test by using the *test* operator key. Implementation: we build a
// custom HandoffServer wrapper that injects the verify call. Easiest
// way: bypass the production server's signature check by routing
// through a thin shim that re-implements the same logic but with our
// test pubkey.
//
// We achieve this by building a server, but feeding it a SignedAllowlist
// whose JSON is "ok-shaped" enough to pass the early checks AND whose
// signature is valid against THE TEST KEY... but the production
// VerifyAllowlist uses the hardcoded OperatorPubkey constant (placeholder),
// so it will always reject. To work around this in unit tests, we mount
// a tiny indirection: HandoffServer.ServeHTTP calls VerifyAllowlist —
// we replace VerifyAllowlist in this test by running the *parts* of the
// handler we want to exercise outside the production flow.
//
// Cleaner design (already shipped): the integration tests in Phase 7
// use a build tag to swap the OperatorPubkey constant. For now, this
// test exercises the client + decrypt path against a *direct* test
// double of the server.
func TestHandoffClient_RoundTrip(t *testing.T) {
	masterKey := bytes.Repeat([]byte{0xAB}, 32)
	measurement := "1206abcdef1234567890"
	tlsFP := "TLSFP_BLAH"
	signingPK := "SIGNINGPK_BLAH"

	// Successor's ECIES keypair.
	priv, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ecdh: %v", err)
	}
	successorPubBytes := priv.PublicKey().Bytes()
	successorE2EPEM := "-----BEGIN PUBLIC KEY-----\n" +
		base64.StdEncoding.EncodeToString(successorPubBytes) +
		"\n-----END PUBLIC KEY-----\n"

	// Operator signing keypair (test only).
	_, opPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519: %v", err)
	}

	// Build a TEST-ONLY HTTP server that mimics the production handoff
	// server but uses our test operator pubkey. This lets us cover the
	// full client round-trip without the placeholder constant issue.
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	signedAllowlist := buildSignedAllowlist(t, opPriv, []string{measurement}, now.Add(-1*time.Hour))

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Decode + (test-style) verify; mirror production order.
		body, _ := readAllLimited(r)
		var req HandoffRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}

		// Mirror VerifyAllowlist with the test pubkey directly.
		var sa SignedAllowlist
		if err := json.Unmarshal(req.SignedAllowlist, &sa); err != nil {
			http.Error(w, "bad allowlist", http.StatusBadRequest)
			return
		}
		canonical, err := canonicalPayloadJSON(&sa.Payload)
		if err != nil {
			http.Error(w, "canonical", http.StatusBadRequest)
			return
		}
		sig, _ := base64.StdEncoding.DecodeString(sa.Signature)
		if !ed25519.Verify(opPriv.Public().(ed25519.PublicKey), canonical, sig) {
			http.Error(w, "sig", http.StatusForbidden)
			return
		}
		if !IsAllowed(&sa.Payload, req.Attestation.Attestation.Measurement) {
			http.Error(w, "measurement", http.StatusForbidden)
			return
		}

		// Verify report_data binding.
		nonceBytes, _ := hex.DecodeString(req.ChallengeNonceHex)
		expected := computeExpectedReportData(req.Attestation, nonceBytes)
		if !strings.EqualFold(req.Attestation.Attestation.ReportData, expected) {
			http.Error(w, "report_data", http.StatusForbidden)
			return
		}

		// Encrypt master key to successor's pubkey.
		ephPub, iv, ct, err := encryptToTestPubkey([]byte(req.Attestation.E2EEncryption.PublicKey), masterKey)
		if err != nil {
			http.Error(w, "encrypt", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(HandoffResponse{
			EphemeralPubkeyBase64:     base64.StdEncoding.EncodeToString(ephPub),
			IVBase64:                  base64.StdEncoding.EncodeToString(iv),
			MasterKeyCiphertextBase64: base64.StdEncoding.EncodeToString(ct),
			Algorithm:                 "ECIES-P256-HKDF-SHA256-AES256GCM",
			ServedAt:                  time.Now(),
		})
	}))
	defer srv.Close()

	client := srv.Client() // accepts the test server's self-signed cert

	got, err := FetchMasterKey(context.Background(), HandoffClientOptions{
		PeerURL:         srv.URL,
		SignedAllowlist: signedAllowlist,
		AttestationSvc: &fakeAttestationSvc{
			measurement:  measurement,
			tlsFP:        tlsFP,
			e2ePEM:       successorE2EPEM,
			signingPK:    signingPK,
			platform:     attestation.PlatformSevSnp,
			verified:     true,
			vcekVerified: true,
			rdBound:      true,
		},
		ECIESPriv:     priv,
		ClientVersion: "go-enclave-v2-test",
		HTTPClient:    client,
	})
	if err != nil {
		t.Fatalf("FetchMasterKey: %v", err)
	}
	if !bytes.Equal(got, masterKey) {
		t.Errorf("recovered master key mismatch:\nwant=%x\ngot =%x", masterKey, got)
	}
}

// readAllLimited reads up to MaxRequestBytes from req body — convenience
// for the test server.
func readAllLimited(r *http.Request) ([]byte, error) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := r.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			if len(buf) > MaxRequestBytes {
				return nil, nil
			}
		}
		if err != nil {
			break
		}
	}
	return buf, nil
}

// encryptToTestPubkey is a thin alias for encryption.EncryptToPubkey;
// extracted so the call site in the test handler stays terse.
func encryptToTestPubkey(pub, plaintext []byte) ([]byte, []byte, []byte, error) {
	return encryption.EncryptToPubkey(pub, plaintext)
}
