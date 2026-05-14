package bootstrap

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/attestation"
)

// TestNewPinnedHTTPClient_RejectsMissingFingerprint asserts the
// AUDIT-HIGH-002 invariant: no fingerprint, no client. The boot path
// must surface ErrMissingPeerTLSFingerprint instead of silently falling
// back to InsecureSkipVerify.
func TestNewPinnedHTTPClient_RejectsMissingFingerprint(t *testing.T) {
	for _, fp := range []string{"", "   ", "\t\n"} {
		client, err := NewPinnedHTTPClient(fp, 30*time.Second)
		if client != nil {
			t.Errorf("fp=%q: expected nil client, got non-nil", fp)
		}
		if !errors.Is(err, ErrMissingPeerTLSFingerprint) {
			t.Errorf("fp=%q: expected ErrMissingPeerTLSFingerprint, got %v", fp, err)
		}
	}
}

// TestNewPinnedHTTPClient_RejectsMalformedFingerprint covers the
// hex-decode and length checks. We expect a clear, non-fatal error
// rather than a runtime TLS panic later.
func TestNewPinnedHTTPClient_RejectsMalformedFingerprint(t *testing.T) {
	cases := []struct {
		name string
		fp   string
	}{
		{"too-short", "abcd"},
		{"too-long", strings.Repeat("a", 65)},
		{"non-hex", strings.Repeat("z", 64)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client, err := NewPinnedHTTPClient(c.fp, 30*time.Second)
			if client != nil {
				t.Errorf("expected nil client, got non-nil")
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			// Must NOT be the missing-fingerprint sentinel — those are
			// distinct failure modes the operator should be able to
			// distinguish from logs.
			if errors.Is(err, ErrMissingPeerTLSFingerprint) {
				t.Errorf("malformed fp should not surface as ErrMissingPeerTLSFingerprint, got %v", err)
			}
		})
	}
}

// TestNewPinnedHTTPClient_AcceptsBothFingerprintFormats verifies the
// helper consumes the colon-separated upper-case form (the shape
// internal/tls.KeyGenerator emits) AND the bare lower-case form (a raw
// hex.EncodeToString of sha256.Sum256). The two MUST yield the same
// trust decision.
func TestNewPinnedHTTPClient_AcceptsBothFingerprintFormats(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	leaf := srv.Certificate()
	sum := sha256.Sum256(leaf.Raw)

	bareLower := hex.EncodeToString(sum[:])
	colonUpper := colonize(strings.ToUpper(bareLower))

	for _, fp := range []string{bareLower, colonUpper} {
		client, err := NewPinnedHTTPClient(fp, 5*time.Second)
		if err != nil {
			t.Fatalf("fp=%q: %v", fp, err)
		}
		// Override RootCAs with the server's CA so chain validation
		// would accept the cert; we want to prove our pinning
		// callback specifically accepts it.
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatalf("fp=%q: GET: %v", fp, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("fp=%q: status=%d", fp, resp.StatusCode)
		}
	}
}

// TestNewPinnedHTTPClient_RejectsMismatchedCert is the heart of
// AUDIT-HIGH-002: a TLS server whose cert hashes to anything OTHER than
// the pinned fingerprint must be refused at the TLS handshake — we
// never see the application response.
func TestNewPinnedHTTPClient_RejectsMismatchedCert(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("handler should not have been invoked — TLS pinning failed open")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Fingerprint of "anything but the server's cert".
	wrong := sha256.Sum256([]byte("definitely-not-the-right-cert"))
	client, err := NewPinnedHTTPClient(hex.EncodeToString(wrong[:]), 5*time.Second)
	if err != nil {
		t.Fatalf("NewPinnedHTTPClient: %v", err)
	}

	_, err = client.Get(srv.URL)
	if err == nil {
		t.Fatal("expected TLS handshake error from fingerprint mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "tls fingerprint mismatch") {
		t.Errorf("expected fingerprint-mismatch error, got %v", err)
	}
}

// TestFetchMasterKey_RejectsMissingFingerprint exercises the
// FetchMasterKey-level integration: when neither HTTPClient nor
// PeerTLSFingerprint is provided, the function must refuse to attempt
// the upgrade rather than fall through to an insecure default.
func TestFetchMasterKey_RejectsMissingFingerprint(t *testing.T) {
	masterKey := bytes.Repeat([]byte{0xCA}, 32)
	successorPriv, _ := ecdh.P256().GenerateKey(rand.Reader)
	successorE2EPEM := "-----BEGIN PUBLIC KEY-----\n" +
		base64.StdEncoding.EncodeToString(successorPriv.PublicKey().Bytes()) +
		"\n-----END PUBLIC KEY-----\n"
	opPub, opPriv, _ := ed25519.GenerateKey(rand.Reader)
	restore := SetOperatorPubkeyForTest(encodeOperatorPubkeyForTest(opPub))
	defer restore()

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
			verified:     true,
			vcekVerified: true,
			rdBound:      true,
		},
		ECIESPriv:     successorPriv,
		ClientVersion: "no-fp-test",
		// HTTPClient and PeerTLSFingerprint deliberately omitted —
		// the boot path must reject this configuration.
	})
	if err == nil {
		t.Fatal("expected refusal when no transport policy is configured, got nil")
	}
	if !errors.Is(err, ErrMissingPeerTLSFingerprint) {
		t.Errorf("expected ErrMissingPeerTLSFingerprint in chain, got %v", err)
	}
}

// TestFetchMasterKey_HandoffWithPinnedClient_HappyPath is the positive
// counterpart to the rejection tests: when the operator provides the
// correct fingerprint, the handoff completes end-to-end. Proves the
// pinning helper is wired correctly into FetchMasterKey.
func TestFetchMasterKey_HandoffWithPinnedClient_HappyPath(t *testing.T) {
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

	leaf := httpSrv.Certificate()
	sum := sha256.Sum256(leaf.Raw)
	fp := hex.EncodeToString(sum[:])

	got, err := FetchMasterKey(context.Background(), HandoffClientOptions{
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
		ECIESPriv:          successorPriv,
		ClientVersion:      "pin-happy-path",
		PeerTLSFingerprint: fp,
	})
	if err != nil {
		t.Fatalf("FetchMasterKey with correct pin: %v", err)
	}
	if !bytes.Equal(got, masterKey) {
		t.Fatalf("master key mismatch:\nwant=%x\ngot =%x", masterKey, got)
	}
}

// colonize re-formats a 64-char hex string into AB:CD:... shape, matching
// the output of internal/tls.KeyGenerator.Fingerprint.
func colonize(hex string) string {
	out := make([]byte, 0, len(hex)+len(hex)/2)
	for i := 0; i < len(hex); i += 2 {
		if i > 0 {
			out = append(out, ':')
		}
		out = append(out, hex[i], hex[i+1])
	}
	return string(out)
}
