// Package bootstrap implements the trustless binary upgrade mechanism (B2)
// described in doc/audit/RFC_KEY_BROKER.md.
//
// Big picture: each release of the enclave is shipped with a SignedAllowlist
// — a JSON document listing the SEV-SNP measurements of every binary the
// operator has approved, signed by the operator's Ed25519 long-term key.
// The Ed25519 public key is hardcoded in this package (OperatorPubkey
// constant) so that it cannot be substituted by an attacker who controls
// the runtime configuration.
//
// At upgrade time, an old enclave (vN, holding the master key) and a new
// enclave (vN+1, freshly booted) talk to each other:
//
//  1. vN+1 boots, reads HANDOFF_PEER_URL, fetches its own attestation.
//  2. vN+1 → vN: "give me the master key, here is my attestation +
//     measurement + ECIES pubkey + the signed allowlist for this release".
//  3. vN verifies (a) its own copy of OperatorPubkey signed the allowlist,
//     (b) vN+1's measurement is in the allowlist, (c) vN+1's attestation
//     chain is valid, (d) the ECIES pubkey is bound to the attestation.
//  4. vN encrypts the master key with vN+1's ECIES pubkey, returns it.
//  5. vN+1 decrypts inside its TEE, takes over.
//
// Trust root: OperatorPubkey constant in this file. To rotate it, ship a
// new release that includes BOTH the old and new pubkeys (transition
// release), then a release that drops the old one.
package bootstrap

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// OperatorPubkey is the Ed25519 public key that signs every SignedAllowlist
// shipped with the enclave. Hardcoded so an attacker who controls the
// runtime cannot substitute it.
//
// Format: ssh-ed25519 wire format, base64-encoded (stdin from
// `ssh-keygen -t ed25519` minus the `ssh-ed25519 ` prefix and the comment).
//
// PLACEHOLDER: replace with the operator's real Ed25519 pubkey before
// the first production deploy. See cmd/release-sign for how to generate it.
const OperatorPubkey = "AAAAC3NzaC1lZDI1NTE5AAAAIPLACEHOLDERPLACEHOLDERPLACEHOLDERPLACEHO"

// AllowlistEntry is one approved binary release.
type AllowlistEntry struct {
	// Measurement is the SEV-SNP launch measurement (hex, lowercase) of
	// the audited binary. Must match the value the silicon emits at boot.
	Measurement string `json:"measurement"`

	// Label is a human-readable tag (e.g. "go-enclave-v1.2.0"). Not used
	// for any cryptographic check; purely for ops legibility.
	Label string `json:"label"`

	// AddedAt records when this measurement was approved by the operator.
	// Used by the verifier to surface oddly-fresh entries (potential
	// late-stage compromise) but not enforced.
	AddedAt time.Time `json:"added_at"`
}

// SignedAllowlistPayload is the unsigned body of an allowlist file. The
// signature field below covers the canonical JSON encoding of this struct.
type SignedAllowlistPayload struct {
	// Version of the allowlist schema. Bumped if the JSON layout changes.
	Version int `json:"version"`

	// IssuedAt is when the operator generated this allowlist. The verifier
	// rejects allowlists older than MaxAllowlistAge to limit replay of
	// stale (potentially compromised) allowlists.
	IssuedAt time.Time `json:"issued_at"`

	// Entries is the list of approved measurements. Order is canonical
	// (sorted by Measurement) so the signature is deterministic.
	Entries []AllowlistEntry `json:"entries"`
}

// SignedAllowlist is the on-disk / on-wire representation of the allowlist:
// the payload plus an Ed25519 signature over its canonical JSON encoding.
type SignedAllowlist struct {
	Payload   SignedAllowlistPayload `json:"payload"`
	Signature string                 `json:"signature"` // base64
}

// MaxAllowlistAge bounds how stale a SignedAllowlist can be at verification
// time. 90 days lets an operator iterate at a normal cadence without
// re-issuing the allowlist every release, while making compromise + late
// replay attacks bounded.
const MaxAllowlistAge = 90 * 24 * time.Hour

// Errors returned by VerifyAllowlist. Sentinel errors so callers can log
// the precise failure mode without leaking details to remote callers.
var (
	ErrAllowlistInvalidJSON      = errors.New("signed allowlist: invalid JSON")
	ErrAllowlistInvalidSignature = errors.New("signed allowlist: invalid signature")
	ErrAllowlistTooOld           = errors.New("signed allowlist: issued more than MaxAllowlistAge ago")
	ErrAllowlistInTheFuture      = errors.New("signed allowlist: issued_at is in the future")
	ErrAllowlistEmpty            = errors.New("signed allowlist: no entries")
	ErrAllowlistOperatorPubkey   = errors.New("signed allowlist: OperatorPubkey constant is invalid")
	ErrMeasurementNotAllowed     = errors.New("measurement is not on the signed allowlist")
)

// VerifyAllowlist parses raw JSON, checks the Ed25519 signature against
// OperatorPubkey, and validates the freshness window. Returns the parsed
// payload on success.
//
// Caller still has to check that a specific measurement is in the
// allowlist via IsAllowed.
func VerifyAllowlist(raw []byte, now time.Time) (*SignedAllowlistPayload, error) {
	var sa SignedAllowlist
	if err := json.Unmarshal(raw, &sa); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAllowlistInvalidJSON, err)
	}
	if len(sa.Payload.Entries) == 0 {
		return nil, ErrAllowlistEmpty
	}

	pubkey, err := DecodeOperatorPubkey()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAllowlistOperatorPubkey, err)
	}

	canonical, err := canonicalPayloadJSON(&sa.Payload)
	if err != nil {
		return nil, fmt.Errorf("canonicalize allowlist payload: %w", err)
	}

	sig, err := base64.StdEncoding.DecodeString(sa.Signature)
	if err != nil {
		return nil, fmt.Errorf("%w: signature not base64: %v", ErrAllowlistInvalidSignature, err)
	}
	if !ed25519.Verify(pubkey, canonical, sig) {
		return nil, ErrAllowlistInvalidSignature
	}

	// Freshness checks. Allow a 5-min skew on issued_at-in-future to
	// tolerate clock drift between the operator's signing host and the
	// enclave (NTP usually keeps drift << 5 min on healthy infra).
	if sa.Payload.IssuedAt.After(now.Add(5 * time.Minute)) {
		return nil, ErrAllowlistInTheFuture
	}
	if now.Sub(sa.Payload.IssuedAt) > MaxAllowlistAge {
		return nil, ErrAllowlistTooOld
	}

	return &sa.Payload, nil
}

// IsAllowed reports whether measurement (lowercase hex) is listed in the
// payload. Constant-time comparison would be overkill here — the
// measurement is not a secret, and the leak of "matched/didn't match" is
// exactly what we want to surface in audit logs.
func IsAllowed(payload *SignedAllowlistPayload, measurement string) bool {
	target := strings.ToLower(strings.TrimSpace(measurement))
	if target == "" {
		return false
	}
	for _, e := range payload.Entries {
		if strings.ToLower(strings.TrimSpace(e.Measurement)) == target {
			return true
		}
	}
	return false
}

// canonicalPayloadJSON re-encodes the payload with sorted JSON keys so the
// signature is deterministic regardless of how the operator's tooling
// serialised the original. We also trim each entry to a canonical shape
// (sorted by Measurement, lowercase hex) before hashing.
func canonicalPayloadJSON(p *SignedAllowlistPayload) ([]byte, error) {
	// Sort entries by measurement.
	entries := make([]AllowlistEntry, len(p.Entries))
	copy(entries, p.Entries)
	for i := range entries {
		entries[i].Measurement = strings.ToLower(strings.TrimSpace(entries[i].Measurement))
		// Strict: measurement must be valid hex.
		if _, err := hex.DecodeString(entries[i].Measurement); err != nil {
			return nil, fmt.Errorf("entry %d: measurement not hex: %w", i, err)
		}
	}
	sortAllowlistEntries(entries)

	canonical := SignedAllowlistPayload{
		Version:  p.Version,
		IssuedAt: p.IssuedAt.UTC(),
		Entries:  entries,
	}

	// json.Marshal emits keys in struct-field order, which is stable for
	// a given struct definition — that's our canonical encoding.
	return json.Marshal(canonical)
}

// sortAllowlistEntries sorts in place by Measurement ascending. Pulled into
// its own function so the test can exercise it directly.
func sortAllowlistEntries(entries []AllowlistEntry) {
	// Insertion sort: the allowlist is tiny (one entry per release, dozens
	// at most), so we avoid the import of sort for clarity.
	for i := 1; i < len(entries); i++ {
		j := i
		for j > 0 && entries[j-1].Measurement > entries[j].Measurement {
			entries[j-1], entries[j] = entries[j], entries[j-1]
			j--
		}
	}
}

// DecodeOperatorPubkey decodes the OperatorPubkey constant into raw
// Ed25519 public key bytes. Format: base64-encoded ssh-ed25519 wire format.
//
// Wire format (RFC 4253):
//
//	uint32  len("ssh-ed25519") = 11
//	string  "ssh-ed25519"
//	uint32  len(pubkey) = 32
//	string  pubkey (32 bytes)
func DecodeOperatorPubkey() (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(OperatorPubkey)
	if err != nil {
		return nil, fmt.Errorf("operator pubkey not base64: %w", err)
	}
	const algName = "ssh-ed25519"
	const algLenPrefix = 4

	// Read length-prefixed alg name.
	if len(raw) < algLenPrefix+len(algName) {
		return nil, fmt.Errorf("operator pubkey too short")
	}
	algLen := readBigEndianU32(raw[:algLenPrefix])
	if int(algLen) != len(algName) {
		return nil, fmt.Errorf("operator pubkey alg name length=%d, want %d", algLen, len(algName))
	}
	if string(raw[algLenPrefix:algLenPrefix+int(algLen)]) != algName {
		return nil, fmt.Errorf("operator pubkey alg name != %q", algName)
	}
	rest := raw[algLenPrefix+int(algLen):]

	// Read length-prefixed public key bytes.
	if len(rest) < algLenPrefix+ed25519.PublicKeySize {
		return nil, fmt.Errorf("operator pubkey body too short")
	}
	keyLen := readBigEndianU32(rest[:algLenPrefix])
	if int(keyLen) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("operator pubkey body length=%d, want %d", keyLen, ed25519.PublicKeySize)
	}
	pk := make(ed25519.PublicKey, ed25519.PublicKeySize)
	copy(pk, rest[algLenPrefix:algLenPrefix+int(keyLen)])
	return pk, nil
}

func readBigEndianU32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
