package main

// B2 handoff wiring helpers.
//
// Pulled into a dedicated file to keep main.go focused on the standard
// boot path. The functions here are used only when HANDOFF_PEER_URL is
// set, i.e. during the upgrade window where v_N+1 fetches the master
// key from v_N over an attested ECIES channel. See
// doc/audit/RFC_KEY_BROKER.md (B2 section) for the protocol design.

import (
	"context"
	"time"

	"github.com/trackrecord/enclave/internal/attestation"
	"github.com/trackrecord/enclave/internal/bootstrap"
	"github.com/trackrecord/enclave/internal/config"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/signing"
	tlspkg "github.com/trackrecord/enclave/internal/tls"
	"go.uber.org/zap"
)

// fetchMasterKeyFromPredecessor performs the full B2 client handshake.
// On success returns the unwrapped master key (32 bytes). On failure
// the boot caller is expected to fatal — we never silently fall back
// to a measurement-derived key when the operator explicitly asked for
// a handoff (HANDOFF_PEER_URL was non-empty).
//
// The function builds an *ephemeral* signing keypair purely for the
// attestation's report_data binding. The "real" report signer used to
// sign performance reports is built later in main.go's normal sequence;
// reusing it here would create an init-order pretzel without adding
// security (the predecessor only needs the attestation chain to be
// internally consistent, not to verify the successor's signing key).
func fetchMasterKeyFromPredecessor(
	ctx context.Context,
	cfg *config.Config,
	eciesSvc *encryption.ECIESService,
	tlsKeygen *tlspkg.KeyGenerator,
	logger *zap.Logger,
) ([]byte, error) {
	logger.Info("B2 handoff: requesting master key from predecessor",
		zap.String("peer_url", cfg.HandoffPeerURL))

	// Ephemeral signer used only to fill the attestation's
	// signing_pk slot. Its private half is dropped at function exit.
	tempSigner, err := signing.NewReportSignerGenerate()
	if err != nil {
		return nil, err
	}

	attestOpts := attestation.Options{
		DevMode: cfg.IsDevelopment(),
		Logger:  logger,
	}
	if tlsKeygen != nil {
		attestOpts.TLSFingerprint = tlsKeygen.Fingerprint()
	}
	if eciesSvc != nil {
		attestOpts.E2EPublicKey = eciesSvc.PublicKeyPEM()
	}
	attestOpts.SigningPubKey = tempSigner.PublicKey()
	attestSvc := attestation.NewService(attestOpts)

	// Use the configured signed-allowlist override if present, else
	// rely on the binary's default (caller passes "" → handoff client
	// will reject with a clear error so the operator notices the
	// missing config; the alternative is a hardcoded empty allowlist
	// that would fail-closed silently).
	signedAllowlist := []byte(cfg.HandoffSignedAllowlist)
	if len(signedAllowlist) == 0 {
		return nil, errMissingSignedAllowlist
	}

	// Cap the handoff fetch at 60s — long enough to tolerate slow
	// snpguest startups, short enough that a misconfigured deploy
	// doesn't hang the enclave forever.
	fetchCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	masterKey, err := bootstrap.FetchMasterKey(fetchCtx, bootstrap.HandoffClientOptions{
		PeerURL:         cfg.HandoffPeerURL,
		SignedAllowlist: signedAllowlist,
		AttestationSvc:  attestSvc,
		ECIESPriv:       eciesSvc.PrivateKey(),
		ClientVersion:   "go-enclave-1.0.0", // TODO: source from build-time ldflags
		Logger:          logger,
	})
	if err != nil {
		return nil, err
	}

	logger.Info("B2 handoff: master key fetched successfully")
	return masterKey, nil
}

// errMissingSignedAllowlist is returned when HANDOFF_PEER_URL is set
// but HANDOFF_SIGNED_ALLOWLIST isn't. The two MUST be set together.
var errMissingSignedAllowlist = handoffError("HANDOFF_PEER_URL is set but HANDOFF_SIGNED_ALLOWLIST is empty — handoff requires both")

type handoffError string

func (e handoffError) Error() string { return string(e) }

// wipeBytes zeroes a sensitive buffer. Best-effort — Go's GC may keep
// copies around but we do what we can.
func wipeBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
