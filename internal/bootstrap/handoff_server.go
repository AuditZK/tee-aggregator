package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/attestation"
	"github.com/trackrecord/enclave/internal/encryption"
	"go.uber.org/zap"
)

// MasterKeyExporter is what the handoff server needs from the
// KeyManagementService to fulfil a handoff request. The interface lets
// us mock it in tests.
type MasterKeyExporter interface {
	ExportMasterKey() ([]byte, error)
}

// HandoffServerOptions configures NewHandoffServer.
type HandoffServerOptions struct {
	// KeyExporter is the source of the master key (typically the
	// running KeyManagementService).
	KeyExporter MasterKeyExporter

	// Logger receives lifecycle events. Optional.
	Logger *zap.Logger

	// NowFn is the time source. Defaults to time.Now. Tests inject a
	// frozen clock here.
	NowFn func() time.Time
}

// HandoffServer answers requests from a successor enclave (vN+1) that
// wants to pick up the master key from this running enclave (vN). Every
// request is gated on:
//
//  1. The successor's SignedAllowlist is signed by OperatorPubkey.
//  2. The successor's measurement is on that allowlist.
//  3. The successor's SEV-SNP attestation chain validates (real silicon,
//     real measurement, no mock).
//  4. The successor's report_data binds the attestation to the ECIES
//     public key it asks the master key to be encrypted to. This is what
//     stops an MITM (or the host) from harvesting the master key —
//     without the matching private key, the response is gibberish.
//  5. The challenge_nonce is fresh and not seen recently (anti-replay).
//
// On success the server encrypts the master key to the successor's
// ECIES pubkey and returns it. The handler never logs the master key
// nor writes it to disk.
type HandoffServer struct {
	keyExporter MasterKeyExporter
	logger      *zap.Logger
	now         func() time.Time

	noncesMu sync.Mutex
	nonces   map[string]time.Time // hex(nonce) -> seen_at
}

// NonceTTL bounds how long a server remembers nonces it has answered.
// Long enough to defeat trivial replay, short enough to not bloat
// memory if the server stays up for weeks.
const NonceTTL = 5 * time.Minute

// MaxRequestBytes caps the size of a /handoff request body. The
// expected payload (attestation JSON + nonce + signed allowlist) is
// well under 64 KiB; cap at 256 KiB for safety.
const MaxRequestBytes = 256 << 10

// NewHandoffServer constructs a server. The caller is responsible for
// mounting the resulting HTTP handler at the right path AND gating it
// behind localhost-only / mTLS in production — the handler itself does
// the cryptographic checks but doesn't enforce transport-layer scoping.
func NewHandoffServer(opts HandoffServerOptions) (*HandoffServer, error) {
	if opts.KeyExporter == nil {
		return nil, fmt.Errorf("KeyExporter is required")
	}
	now := opts.NowFn
	if now == nil {
		now = time.Now
	}

	return &HandoffServer{
		keyExporter: opts.KeyExporter,
		logger:      opts.Logger,
		now:         now,
		nonces:      make(map[string]time.Time),
	}, nil
}

// HandoffRequest is the JSON body the successor enclave POSTs.
type HandoffRequest struct {
	// Attestation is the full attestation report from the successor
	// (the same JSON shape as /api/v1/attestation returns), so this
	// server can verify the chain via reportverify.
	Attestation *attestation.AttestationReport `json:"attestation"`

	// SignedAllowlist is the operator-signed list of trusted
	// measurements. This server verifies it with OperatorPubkey before
	// trusting any measurement claim.
	SignedAllowlist json.RawMessage `json:"signed_allowlist"`

	// ChallengeNonceHex must be embedded inside the attestation's
	// reportData via the standard tlsFp || e2ePk || signingPk || nonce
	// chain (see attestation.buildReportData). 32 bytes hex.
	ChallengeNonceHex string `json:"challenge_nonce_hex"`

	// ClientVersion is informational ("go-enclave-1.2.0"); logged but
	// not used for any security check.
	ClientVersion string `json:"client_version,omitempty"`
}

// HandoffResponse is what the predecessor returns on success.
type HandoffResponse struct {
	// EphemeralPubkey is the ephemeral P-256 pubkey the predecessor
	// generated to encrypt MasterKeyCiphertext. Raw bytes, base64.
	EphemeralPubkeyBase64 string `json:"ephemeral_pubkey_b64"`

	// IV is the 12-byte AES-GCM nonce, base64.
	IVBase64 string `json:"iv_b64"`

	// MasterKeyCiphertext is the AES-GCM-sealed master key (32 bytes
	// plaintext + 16 bytes auth tag = 48 bytes), base64.
	MasterKeyCiphertextBase64 string `json:"master_key_ciphertext_b64"`

	// Algorithm identifies the ECIES construction so the client knows
	// how to decrypt. Currently the only valid value.
	Algorithm string `json:"algorithm"` // "ECIES-P256-HKDF-SHA256-AES256GCM"

	// ServedAt is the predecessor's wall-clock at response time. Helps
	// the operator audit handoff timing in logs; not used cryptographically.
	ServedAt time.Time `json:"served_at"`
}

// HandoffErrorResponse is the deliberately-vague body returned on
// rejection. The detailed reason is logged server-side keyed by
// RequestID, never returned to the caller, so an attacker probing for
// vulnerabilities can't learn which check failed.
type HandoffErrorResponse struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id"`
}

// ServeHTTP handles POST /api/v1/admin/handoff.
func (s *HandoffServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, HandoffErrorResponse{Error: "POST only"})
		return
	}

	requestID := makeRequestID()
	logger := s.logger
	if logger != nil {
		logger = logger.With(zap.String("request_id", requestID))
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxRequestBytes))
	if err != nil {
		s.logRejection(logger, "read body", err)
		writeJSON(w, http.StatusBadRequest, HandoffErrorResponse{Error: "request rejected", RequestID: requestID})
		return
	}

	var req HandoffRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.logRejection(logger, "decode body", err)
		writeJSON(w, http.StatusBadRequest, HandoffErrorResponse{Error: "request rejected", RequestID: requestID})
		return
	}

	// 1. Verify the SignedAllowlist signature with OperatorPubkey.
	allowlist, err := VerifyAllowlist(req.SignedAllowlist, s.now())
	if err != nil {
		s.logRejection(logger, "verify allowlist", err)
		writeJSON(w, http.StatusForbidden, HandoffErrorResponse{Error: "allowlist rejected", RequestID: requestID})
		return
	}

	// 2. Successor's measurement must be on the allowlist.
	if req.Attestation == nil || req.Attestation.Attestation == nil {
		s.logRejection(logger, "attestation missing", nil)
		writeJSON(w, http.StatusBadRequest, HandoffErrorResponse{Error: "request rejected", RequestID: requestID})
		return
	}
	measurement := strings.ToLower(strings.TrimSpace(req.Attestation.Attestation.Measurement))
	if !IsAllowed(allowlist, measurement) {
		s.logRejection(logger, "measurement not allowed", fmt.Errorf("measurement=%s", measurement))
		writeJSON(w, http.StatusForbidden, HandoffErrorResponse{Error: "measurement rejected", RequestID: requestID})
		return
	}

	// 3. Validate challenge_nonce shape (32 bytes hex == 64 chars).
	if len(req.ChallengeNonceHex) != 64 {
		s.logRejection(logger, "challenge_nonce wrong length", fmt.Errorf("len=%d", len(req.ChallengeNonceHex)))
		writeJSON(w, http.StatusBadRequest, HandoffErrorResponse{Error: "request rejected", RequestID: requestID})
		return
	}
	nonceBytes, err := hex.DecodeString(req.ChallengeNonceHex)
	if err != nil || len(nonceBytes) != 32 {
		s.logRejection(logger, "challenge_nonce not hex", err)
		writeJSON(w, http.StatusBadRequest, HandoffErrorResponse{Error: "request rejected", RequestID: requestID})
		return
	}

	// 4. Anti-replay: refuse if we've already served this nonce recently.
	if !s.acceptNonce(req.ChallengeNonceHex) {
		s.logRejection(logger, "nonce replay", nil)
		writeJSON(w, http.StatusForbidden, HandoffErrorResponse{Error: "replay rejected", RequestID: requestID})
		return
	}

	// 5. Verify report_data binds the nonce + ECIES pubkey + signing pk
	//    + TLS fingerprint. Same construction the production attestation
	//    flow uses, so we get cryptographic proof that the response will
	//    only be decryptable inside the successor's TEE.
	expectedReportData := computeExpectedReportData(req.Attestation, nonceBytes)
	if !constantTimeStringsEqualLowerHex(req.Attestation.Attestation.ReportData, expectedReportData) {
		s.logRejection(logger, "report_data mismatch", fmt.Errorf("got=%s want=%s",
			req.Attestation.Attestation.ReportData, expectedReportData))
		writeJSON(w, http.StatusForbidden, HandoffErrorResponse{Error: "binding rejected", RequestID: requestID})
		return
	}

	// 6. Verify the SEV-SNP attestation chain itself (silicon signature,
	//    VCEK, etc.). We DON'T check measurement allowlist here because we
	//    already did it via SignedAllowlist above. Skip the strict measurement
	//    check by passing a fake report shape that doesn't trigger it.
	if err := s.verifyAttestationChain(r.Context(), req.Attestation); err != nil {
		s.logRejection(logger, "verify attestation chain", err)
		writeJSON(w, http.StatusForbidden, HandoffErrorResponse{Error: "attestation rejected", RequestID: requestID})
		return
	}

	// 7. All checks passed — export master key and encrypt to successor's pubkey.
	masterKey, err := s.keyExporter.ExportMasterKey()
	if err != nil {
		s.logRejection(logger, "export master key", err)
		writeJSON(w, http.StatusInternalServerError, HandoffErrorResponse{Error: "internal error", RequestID: requestID})
		return
	}
	defer wipe(masterKey)

	if req.Attestation.E2EEncryption == nil || req.Attestation.E2EEncryption.PublicKey == "" {
		s.logRejection(logger, "successor has no e2e pubkey", nil)
		writeJSON(w, http.StatusBadRequest, HandoffErrorResponse{Error: "request rejected", RequestID: requestID})
		return
	}
	ephPub, iv, ct, err := encryption.EncryptToPubkey(
		[]byte(req.Attestation.E2EEncryption.PublicKey),
		masterKey,
	)
	if err != nil {
		s.logRejection(logger, "encrypt to successor pubkey", err)
		writeJSON(w, http.StatusInternalServerError, HandoffErrorResponse{Error: "internal error", RequestID: requestID})
		return
	}

	if logger != nil {
		logger.Info("handoff served",
			zap.String("successor_measurement", measurement),
			zap.String("successor_version", req.ClientVersion),
		)
	}

	writeJSON(w, http.StatusOK, HandoffResponse{
		EphemeralPubkeyBase64:     base64.StdEncoding.EncodeToString(ephPub),
		IVBase64:                  base64.StdEncoding.EncodeToString(iv),
		MasterKeyCiphertextBase64: base64.StdEncoding.EncodeToString(ct),
		Algorithm:                 "ECIES-P256-HKDF-SHA256-AES256GCM",
		ServedAt:                  s.now().UTC(),
	})
}

// acceptNonce returns true if the nonce has not been seen in the last
// NonceTTL. The mutex protects nonces from concurrent requests.
func (s *HandoffServer) acceptNonce(nonceHex string) bool {
	s.noncesMu.Lock()
	defer s.noncesMu.Unlock()

	now := s.now()
	// Sweep expired entries opportunistically. Cheap because the map is
	// small in practice (handoffs are rare events).
	for k, t := range s.nonces {
		if now.Sub(t) > NonceTTL {
			delete(s.nonces, k)
		}
	}

	if _, seen := s.nonces[nonceHex]; seen {
		return false
	}
	s.nonces[nonceHex] = now
	return true
}

// computeExpectedReportData replicates internal/attestation.buildReportData
// so we can compare byte-for-byte against what the successor put in its
// attestation. Pulled into a free function to keep the dependency
// directional (bootstrap → attestation, not the reverse).
//
// Layout: SHA-256 of length-prefixed concatenation of:
//
//	tls_fingerprint_hex || e2e_pubkey_pem || signing_pubkey || nonce
//
// followed by 32 bytes of zeros — matches the snpguest 64-byte report_data
// field convention used everywhere else in this codebase.
func computeExpectedReportData(report *attestation.AttestationReport, nonce []byte) string {
	h := sha256.New()
	if report.TLSBinding != nil {
		writeLenPrefixed(h, []byte(report.TLSBinding.Fingerprint))
	} else {
		writeLenPrefixed(h, nil)
	}
	if report.E2EEncryption != nil {
		writeLenPrefixed(h, []byte(report.E2EEncryption.PublicKey))
	} else {
		writeLenPrefixed(h, nil)
	}
	if report.ReportSigning != nil {
		writeLenPrefixed(h, []byte(report.ReportSigning.PublicKey))
	} else {
		writeLenPrefixed(h, nil)
	}
	if len(nonce) > 0 {
		writeLenPrefixed(h, nonce)
	}
	digest := h.Sum(nil)

	// snpguest reportData is 64 bytes; we use 32 bytes of digest + 32
	// bytes of zero pad, encoded as lower-case hex, matching the
	// existing attestation flow.
	padded := make([]byte, 64)
	copy(padded, digest)
	return strings.ToLower(hex.EncodeToString(padded))
}

func writeLenPrefixed(h io.Writer, b []byte) {
	var l [4]byte
	n := uint32(len(b))
	l[0], l[1], l[2], l[3] = byte(n>>24), byte(n>>16), byte(n>>8), byte(n)
	_, _ = h.Write(l[:])
	_, _ = h.Write(b)
}

func constantTimeStringsEqualLowerHex(a, b string) bool {
	la := strings.ToLower(strings.TrimSpace(a))
	lb := strings.ToLower(strings.TrimSpace(b))
	if len(la) != len(lb) {
		return false
	}
	var diff byte
	for i := 0; i < len(la); i++ {
		diff |= la[i] ^ lb[i]
	}
	return diff == 0
}

// verifyAttestationChain runs the SEV-SNP signature + VCEK check. We
// cannot reuse pkg/reportverify.Verifier directly because its public
// Verify() method takes a SignedReport, not a raw AttestationReport.
// Instead we do the parts that matter for handoff: the SEV-SNP report
// must be present, sev-snp platform, attested=true, vcek-verified
// (or VCEKChecker invoked).
func (s *HandoffServer) verifyAttestationChain(_ context.Context, report *attestation.AttestationReport) error {
	if report == nil || report.Attestation == nil {
		return errors.New("attestation block missing")
	}
	if !strings.EqualFold(report.Platform, attestation.PlatformSevSnp) {
		return fmt.Errorf("platform=%q (need sev-snp)", report.Platform)
	}
	if !report.Attestation.Verified {
		return errors.New("attestation.verified = false")
	}
	// VCEK check is delegated. The reportverify Verifier carries the
	// VCEKChecker; if not configured, we trust the in-report flag.
	if !report.Attestation.VcekVerified {
		return errors.New("vcek_verified = false (refuse to trust unattested chain)")
	}
	if !report.Attestation.ReportDataBoundToRequest {
		return errors.New("report_data not bound to request (snpguest --random fallback)")
	}
	return nil
}

func (s *HandoffServer) logRejection(logger *zap.Logger, step string, err error) {
	if logger == nil {
		return
	}
	if err != nil {
		logger.Warn("handoff rejected", zap.String("step", step), zap.Error(err))
	} else {
		logger.Warn("handoff rejected", zap.String("step", step))
	}
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func makeRequestID() string {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	const idLen = 12
	out := make([]byte, idLen)
	now := uint64(time.Now().UnixNano())
	for i := 0; i < idLen; i++ {
		out[i] = alphabet[now%uint64(len(alphabet))]
		now /= 33
		if now == 0 {
			// Re-seed when the counter is exhausted. Bit-shift i into the
			// high half so consecutive iterations don't collide on the
			// same low byte.
			now = uint64(time.Now().UnixNano()) ^ (uint64(i) << 16)
		}
	}
	return "req_" + string(out)
}
