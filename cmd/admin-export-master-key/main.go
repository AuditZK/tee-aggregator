// One-shot admin tool to export the master key from a running enclave
// container, so it can be passed to the next-version enclave as
// LEGACY_MASTER_KEY_HEX during the v0→v1 migration.
//
// Why this exists: the standard B2 handoff requires the predecessor
// to expose the /api/v1/admin/handoff endpoint. The first deploy of
// an enclave that includes the B2 code (v1) has a *predecessor* (v0)
// that does NOT yet ship that endpoint — it has no way to hand the
// master key to v1. This tool bridges the gap.
//
// Usage:
//
//	# 1. Cross-compile this tool for linux/amd64 from your laptop:
//	GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/admin-export-master-key \
//	    ./cmd/admin-export-master-key
//
//	# 2. Drop it inside the running v0 container so it inherits the
//	#    same SEV-SNP measurement as v0:
//	docker cp /tmp/admin-export-master-key enclave_go_test:/tmp/
//
//	# 3. Run it. Output is 32 bytes hex on stdout, NOTHING ELSE there.
//	#    All diagnostics are written to stderr so a calling shell can:
//	#       docker exec enclave_go_test /tmp/admin-export-master-key > master.key.hex
//	docker exec enclave_go_test /tmp/admin-export-master-key
//
//	# 4. Push the value into GCP instance metadata so v1 can pick it up:
//	gcloud compute instances add-metadata $INSTANCE \
//	    --metadata-from-file=legacy-master-key-hex=master.key.hex
//	shred -u master.key.hex
//
//	# 5. Boot v1. Its handoff_wire.go honours LEGACY_MASTER_KEY_HEX,
//	#    unwraps the existing DEK with that key, then re-wraps with
//	#    the v1-measurement-derived master key and writes it back. At
//	#    the next reboot, v1 boots normally without needing the env
//	#    var.
//
//	# 6. Once v1 is healthy, REMOVE legacy-master-key-hex from metadata:
//	gcloud compute instances remove-metadata $INSTANCE --keys=legacy-master-key-hex
//
// Threat model: the master key is briefly visible (a) on stdout
// inside the container, (b) in GCP metadata for the duration of step
// 4–6. Both are accessible to admin host root. This is a *one-shot*
// operation gated on operator action — the alternative (asking users
// to re-submit) is what we're trying to avoid. Treat the operation
// as a maintenance window: minimise the time legacy-master-key-hex
// is set in metadata, and remove it the moment v1 boots cleanly.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/trackrecord/enclave/internal/encryption"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set — run this inside the enclave container so it inherits the env from start-enclave.sh")
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer pool.Close()

	// Re-derive the master key from the same SEV-SNP measurement the
	// running enclave used. Logs to stderr ("master key derived from
	// SEV-SNP measurement") are explicitly suppressed by passing nil
	// — we don't want to leak the master_key_id to stderr next to the
	// hex output.
	derivation, err := encryption.NewKeyDerivationService(nil)
	if err != nil {
		return fmt.Errorf("derive master key: %w", err)
	}

	// Ensure we are picking up the active DEK row, not a deactivated
	// one. The DEK shape mirrors KeyManagementService.loadActiveDEK
	// (kept inline to avoid a circular dep on the production loader).
	var (
		dekID, encryptedDEK, iv, authTag, masterKeyID string
	)
	err = pool.QueryRow(ctx, `
		SELECT id, encrypted_dek, iv, auth_tag, master_key_id
		FROM data_encryption_keys
		WHERE is_active = true
		ORDER BY created_at DESC
		LIMIT 1`).Scan(&dekID, &encryptedDEK, &iv, &authTag, &masterKeyID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("no active DEK found in data_encryption_keys")
		}
		return fmt.Errorf("query active DEK: %w", err)
	}

	// Unwrap the DEK to confirm the master key actually matches the
	// stored row before we export. Without this check the operator
	// might paste a master key that the v1 enclave then fails to use,
	// wasting a maintenance window.
	dek, err := derivation.UnwrapKey(&encryption.EncryptedData{
		Ciphertext: encryptedDEK,
		IV:         iv,
		AuthTag:    authTag,
	})
	if err != nil {
		return fmt.Errorf("unwrap DEK to verify master key — refuse to export an invalid key: %w", err)
	}
	wipe(dek)

	// Diagnostics on stderr; ONLY the master key on stdout.
	fmt.Fprintf(os.Stderr, "active DEK id:        %s\n", dekID)
	fmt.Fprintf(os.Stderr, "stored master_key_id: %s\n", masterKeyID)
	fmt.Fprintf(os.Stderr, "derived master_key_id: %s\n", derivation.GetMasterKeyID())
	if masterKeyID != derivation.GetMasterKeyID() {
		fmt.Fprintln(os.Stderr, "WARNING: stored vs derived master_key_id mismatch — DEK unwrap still succeeded so proceeding, but verify the running enclave's measurement matches expectations.")
	}
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Master key (32 bytes hex) follows on stdout. Capture with:")
	fmt.Fprintln(os.Stderr, "  docker exec ... > master.key.hex")
	fmt.Fprintln(os.Stderr, "")

	// Print the master key. We don't have a public accessor on
	// KeyDerivationService for the raw master bytes (deliberately —
	// the production code never needs them in clear). The trick:
	// we know that wrapping a known-zero plaintext with the master
	// key, and then xor'ing with that same wrapped output, doesn't
	// recover the key. Instead we expose the key by wrapping THE
	// MASTER KEY ITSELF as plaintext: the production WrapKey method
	// uses AES-GCM, so wrapping a 32-byte plaintext with the master
	// key produces 32 bytes of ciphertext + 16 bytes auth tag — not
	// what we want here.
	//
	// Cleanest path: add a test-mode export. We do that by writing
	// a 32-byte zero buffer, wrapping it, and... no, this doesn't
	// expose the key either.
	//
	// Right answer: KeyDerivationService stores the master key as a
	// private []byte field. We need a controlled accessor. The
	// production package doesn't expose one because production code
	// shouldn't need it. This admin tool is the legitimate exception.
	//
	// We rely on a brand-new, narrowly-scoped accessor:
	// encryption.ExportRawMasterKeyForLegacyMigration — added in the
	// same commit as this tool with a clear comment that production
	// code MUST NOT use it.
	rawMaster, err := encryption.ExportRawMasterKeyForLegacyMigration(derivation)
	if err != nil {
		return fmt.Errorf("export raw master key: %w", err)
	}
	defer wipe(rawMaster)

	fmt.Println(hex.EncodeToString(rawMaster))
	return nil
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
