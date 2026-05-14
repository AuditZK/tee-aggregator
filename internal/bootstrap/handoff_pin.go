package bootstrap

// AUDIT-HIGH-002: TLS fingerprint pinning for the B2 handoff client.
//
// The predecessor enclave serves its own self-signed cert (its identity
// is anchored in the SEV-SNP attestation chain, not in any public PKI),
// so OS root validation cannot be used. The operator must therefore
// commit to the predecessor's leaf-cert SHA-256 *out of band* — fetched
// from the predecessor's /api/v1/tls/fingerprint endpoint before the
// upgrade window opens — and supply it to the handoff client.
//
// The previous default (InsecureSkipVerify: true) was correct for the
// confidentiality of the master key (ECIES protects it), but
// (a) leaked upgrade timing to a passive observer, and (b) removed the
// only remaining defense-in-depth layer should the attestation chain
// ever regress. NewPinnedHTTPClient inverts that default: no
// fingerprint, no client.

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ErrMissingPeerTLSFingerprint is returned by NewPinnedHTTPClient when
// the operator did not provide a fingerprint and did not supply a
// custom HTTPClient. Surfaced as-is so a misconfigured boot fails loud.
var ErrMissingPeerTLSFingerprint = errors.New(
	"PeerTLSFingerprint is required when HTTPClient is not provided " +
		"(fetch it from <peer>/api/v1/tls/fingerprint and pass via HandoffClientOptions.PeerTLSFingerprint)")

// ErrTLSFingerprintMismatch is returned by the pinned VerifyConnection
// callback when the leaf cert SHA-256 does not match the expected
// fingerprint. Surfaced verbatim by *http.Client.Do as part of the
// underlying *net.OpError chain.
var ErrTLSFingerprintMismatch = errors.New("tls fingerprint mismatch")

// NewPinnedHTTPClient builds an *http.Client whose TLS layer rejects
// any peer whose leaf-cert SHA-256 differs from expectedFingerprint.
//
// expectedFingerprint may be either of the two formats this codebase
// produces:
//   - colon-separated upper-case hex ("AB:CD:EF:..."), the form
//     internal/tls.KeyGenerator.Fingerprint() returns
//   - bare lower-case hex ("abcdef..."), the form a hex.EncodeToString
//     of a sha256.Sum256 yields
//
// timeout caps the total request duration; pass 0 to use Go's default
// (no timeout — not recommended for the handoff path).
func NewPinnedHTTPClient(expectedFingerprint string, timeout time.Duration) (*http.Client, error) {
	want, err := normalizeFingerprint(expectedFingerprint)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		// We do our own validation in VerifyConnection because the
		// predecessor's cert is self-signed (no chain to a public CA).
		// Standard chain validation would reject the cert before we
		// got a chance to compare its fingerprint.
		InsecureSkipVerify: true, //nolint:gosec
		MinVersion:         tls.VersionTLS12,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("%w: peer presented no certificates", ErrTLSFingerprintMismatch)
			}
			sum := sha256.Sum256(cs.PeerCertificates[0].Raw)
			got := sum[:]
			if subtle.ConstantTimeCompare(got, want) != 1 {
				return fmt.Errorf("%w: got=%s want=%s",
					ErrTLSFingerprintMismatch,
					hex.EncodeToString(got),
					hex.EncodeToString(want),
				)
			}
			return nil
		},
	}

	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}, nil
}

// normalizeFingerprint accepts the two fingerprint shapes used in this
// repo and returns the raw 32-byte SHA-256.
func normalizeFingerprint(s string) ([]byte, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil, ErrMissingPeerTLSFingerprint
	}
	t = strings.ReplaceAll(t, ":", "")
	t = strings.ToLower(t)
	if len(t) != sha256.Size*2 {
		return nil, fmt.Errorf("PeerTLSFingerprint: expected %d hex chars (SHA-256), got %d",
			sha256.Size*2, len(t))
	}
	raw, err := hex.DecodeString(t)
	if err != nil {
		return nil, fmt.Errorf("PeerTLSFingerprint: not valid hex: %w", err)
	}
	return raw, nil
}
