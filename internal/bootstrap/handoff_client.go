package bootstrap

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/attestation"
	"github.com/trackrecord/enclave/internal/encryption"
	"go.uber.org/zap"
	"golang.org/x/crypto/hkdf"
)

// AttestationProvider is what the client needs from the running enclave
// to assemble the handoff request: an attestation report with a fresh
// nonce binding.
type AttestationProvider interface {
	GetAttestationWithNonce(ctx context.Context, nonce []byte) (*attestation.AttestationReport, error)
}

// HandoffClientOptions configures FetchMasterKey.
type HandoffClientOptions struct {
	// PeerURL is the predecessor enclave's handoff endpoint, e.g.
	// "https://enclave-v1.internal:3050/api/v1/admin/handoff".
	PeerURL string

	// SignedAllowlist is the operator-signed JSON document listing the
	// successor's measurement among the trusted releases. Shipped with
	// the binary or via GCP metadata.
	SignedAllowlist []byte

	// AttestationSvc generates the attestation. Required.
	AttestationSvc AttestationProvider

	// ECIESPriv is the local ECIES private key — the *only* place that
	// can decrypt the handoff response. NEVER leaves the TEE process.
	ECIESPriv *ecdh.PrivateKey

	// ClientVersion is informational, sent to the server for its log
	// (no security significance).
	ClientVersion string

	// HTTPClient is optional; falls back to a default with reasonable
	// timeouts. The transport should pin the predecessor's TLS cert
	// (caller's responsibility) so MITM cannot proxy the handoff.
	HTTPClient *http.Client

	// Logger is optional.
	Logger *zap.Logger
}

// FetchMasterKey performs the full handoff handshake from the successor
// (this process) to the predecessor (PeerURL). On success returns the
// 32-byte master key, ready to be plugged into
// KeyManagementOptions.ExternalMasterKey.
//
// Error paths are intentionally verbose so an operator inspecting boot
// logs can pinpoint why an upgrade refused. Returns the predecessor's
// vague error string verbatim when the response was a 4xx — the precise
// reason lives in the predecessor's logs (search by request_id).
func FetchMasterKey(ctx context.Context, opts HandoffClientOptions) ([]byte, error) {
	if opts.PeerURL == "" {
		return nil, fmt.Errorf("PeerURL is required")
	}
	if opts.AttestationSvc == nil {
		return nil, fmt.Errorf("AttestationSvc is required")
	}
	if opts.ECIESPriv == nil {
		return nil, fmt.Errorf("ECIESPriv is required")
	}
	if len(opts.SignedAllowlist) == 0 {
		return nil, fmt.Errorf("SignedAllowlist is required")
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
			// We DELIBERATELY accept self-signed certs by default —
			// the cryptographic check is at the application layer
			// (attestation + ECIES), not at the TLS layer. Operators
			// who want stricter pinning supply a custom HTTPClient.
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
			},
		}
	}

	// 1. Fresh challenge nonce — 32 bytes from crypto/rand. Embedded in
	// the attestation's report_data so the predecessor can verify the
	// quote is fresh AND bound to this specific request.
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	// 2. Get attestation with nonce.
	att, err := opts.AttestationSvc.GetAttestationWithNonce(ctx, nonce)
	if err != nil {
		return nil, fmt.Errorf("fetch attestation: %w", err)
	}
	if att == nil || att.Attestation == nil {
		return nil, fmt.Errorf("attestation service returned no attestation")
	}

	// 3. Build + send the request.
	reqBody, err := json.Marshal(HandoffRequest{
		Attestation:       att,
		SignedAllowlist:   opts.SignedAllowlist,
		ChallengeNonceHex: hex.EncodeToString(nonce),
		ClientVersion:     opts.ClientVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, opts.PeerURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("build http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	if opts.Logger != nil {
		opts.Logger.Info("handoff client: requesting master key",
			zap.String("peer_url", opts.PeerURL),
			zap.String("client_version", opts.ClientVersion),
		)
	}

	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("post handoff: %w", err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(httpResp.Body, MaxRequestBytes))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if httpResp.StatusCode != http.StatusOK {
		var errResp HandoffErrorResponse
		_ = json.Unmarshal(body, &errResp)
		return nil, fmt.Errorf("handoff peer rejected request: status=%d error=%q request_id=%q",
			httpResp.StatusCode, errResp.Error, errResp.RequestID)
	}

	// 4. Decode + decrypt the response with the local ECIES private key.
	var resp HandoffResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	masterKey, err := decryptHandoffPayload(opts.ECIESPriv, &resp)
	if err != nil {
		return nil, fmt.Errorf("decrypt master key: %w", err)
	}
	if len(masterKey) != 32 {
		// Defensive: zero-out any partial buffer before returning.
		wipe(masterKey)
		return nil, fmt.Errorf("decrypted master key length=%d, want 32", len(masterKey))
	}

	if opts.Logger != nil {
		opts.Logger.Info("handoff client: master key received and decrypted",
			zap.String("algorithm", resp.Algorithm),
			zap.Time("served_at", resp.ServedAt),
		)
	}

	return masterKey, nil
}

// decryptHandoffPayload reverses what HandoffServer encrypts: ECDH(local
// priv, server's ephemeral pub) → HKDF → AES-GCM open. Mirror of
// internal/encryption.ECIESService.Decrypt but with a passed-in private
// key (the bootstrap client doesn't hold an ECIESService — it operates
// on the freshly-generated boot keypair directly).
func decryptHandoffPayload(priv *ecdh.PrivateKey, resp *HandoffResponse) ([]byte, error) {
	if !strings.EqualFold(resp.Algorithm, "ECIES-P256-HKDF-SHA256-AES256GCM") {
		return nil, fmt.Errorf("unsupported algorithm %q", resp.Algorithm)
	}

	ephBytes, err := base64.StdEncoding.DecodeString(resp.EphemeralPubkeyBase64)
	if err != nil {
		return nil, fmt.Errorf("decode ephemeral_pub: %w", err)
	}
	iv, err := base64.StdEncoding.DecodeString(resp.IVBase64)
	if err != nil {
		return nil, fmt.Errorf("decode iv: %w", err)
	}
	ct, err := base64.StdEncoding.DecodeString(resp.MasterKeyCiphertextBase64)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}

	curve := ecdh.P256()
	ephPub, err := curve.NewPublicKey(ephBytes)
	if err != nil {
		return nil, fmt.Errorf("parse ephemeral_pub: %w", err)
	}
	shared, err := priv.ECDH(ephPub)
	if err != nil {
		return nil, fmt.Errorf("ecdh: %w", err)
	}
	hkdfReader := hkdf.New(sha256.New, shared, nil, []byte("enclave-e2e-encryption"))
	aesKey := make([]byte, 32)
	if _, err := hkdfReader.Read(aesKey); err != nil {
		return nil, fmt.Errorf("hkdf: %w", err)
	}
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, fmt.Errorf("aes: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}
	pt, err := gcm.Open(nil, iv, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("gcm open: %w", err)
	}
	return pt, nil
}

// Compile-time guard: ensure the production *encryption.ECIESService is
// reachable here so callers integrating the client see the canonical
// way to expose its private key. (We don't import private fields, but
// we keep the reference so future refactors that move ECIES around
// surface this dependency early.)
var _ = encryption.NewECIES
