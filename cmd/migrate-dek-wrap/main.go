// Admin tool to re-wrap the active DEK with the env-derived master key.
//
// WHY: the SEV-SNP measurement-derived master key is bound to the running
// binary's launch hash. As soon as the binary changes (security patch,
// perf optimisation, dep bump), the new measurement produces a new master
// key, and the existing DEK — which was wrapped by the old binary's
// master key — can no longer be unwrapped. Every encrypted credential
// becomes unrecoverable.
//
// FIX: re-wrap the DEK with a master key derived from ENCRYPTION_KEY
// (an env var, stable across binary upgrades). Future binaries that ship
// with the KeyDerivationService.UnwrapKeyTryAll patch will fail the
// hardware unwrap then succeed via the env path.
//
// IMPORTANT: this tool MUST run *inside the currently-running enclave*
// (so it sees the same SEV-SNP measurement and can unwrap the existing
// DEK). Typical invocation:
//
//	docker cp ./bin/migrate-dek-wrap enclave_go_test:/tmp/migrate-dek-wrap
//	docker exec enclave_go_test /tmp/migrate-dek-wrap -dry-run
//	docker exec enclave_go_test /tmp/migrate-dek-wrap          # commit
//
// PRE-FLIGHT CHECKLIST (operator):
//   - Backup data_encryption_keys:
//       docker exec auditzk_postgres_enclave pg_dump -U enclave_user \
//           -d enclave_db -t data_encryption_keys > /tmp/dek-backup.sql
//   - Confirm ENCRYPTION_KEY is set inside the enclave container.
//   - Confirm KeyDerivationService.UnwrapKeyTryAll is present in the
//     binary you'll deploy NEXT (otherwise that boot will fail, since the
//     env path is the only way to unwrap a re-wrapped DEK).
//
// ROLLBACK:
//   - Restore data_encryption_keys from the pg_dump above.
//   - The legacy hardware-wrapped DEK will still unwrap with the
//     currently-running binary's master key.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/encryption"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "compute the new wrap and print the UPDATE statement without executing it")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "init logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Fatal("DATABASE_URL is required (run inside the enclave container so DB + env are wired)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		logger.Fatal("parse database url", zap.Error(err))
	}
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		logger.Fatal("connect database", zap.Error(err))
	}
	defer pool.Close()

	derivation, err := encryption.NewKeyDerivationService(logger)
	if err != nil {
		logger.Fatal("init key derivation", zap.Error(err))
	}

	if !derivation.IsHardwareKey() {
		logger.Warn("hardware master key not available — assuming the active DEK is already env-wrapped or this is a dev harness; continuing")
	}
	if !derivation.HasEnvMasterKey() {
		logger.Fatal("ENCRYPTION_KEY not set or invalid — cannot derive env master key, refusing to migrate (set ENCRYPTION_KEY=<32-byte hex> first)")
	}

	dek, schema, err := loadActiveDEK(ctx, pool)
	if err != nil {
		logger.Fatal("load active dek", zap.Error(err))
	}
	if dek == nil {
		logger.Fatal("no active DEK found in data_encryption_keys — nothing to migrate")
	}

	logger.Info("loaded active DEK",
		zap.String("dek_id", dek.ID),
		zap.String("schema", schema),
		zap.String("stored_master_key_id", dek.MasterKeyID),
	)

	wrapped := &encryption.EncryptedData{
		Ciphertext: dek.EncryptedDEK,
		IV:         dek.IV,
		AuthTag:    dek.AuthTag,
	}

	plain, source, err := derivation.UnwrapKeyTryAll(wrapped)
	if err != nil {
		logger.Fatal("unwrap active DEK with both available master keys",
			zap.Error(err),
			zap.String("hint", "verify the running binary measurement matches the one that wrapped this DEK; if not, this tool must run inside the enclave that produced the DEK"),
		)
	}
	defer func() {
		// Best-effort wipe of the in-memory plaintext DEK.
		for i := range plain {
			plain[i] = 0
		}
	}()

	logger.Info("DEK unwrapped",
		zap.String("dek_id", dek.ID),
		zap.String("source", string(source)),
	)

	if source == encryption.UnwrapSourceEnv {
		logger.Info("DEK is already env-wrapped — nothing to do",
			zap.String("dek_id", dek.ID),
			zap.String("env_master_key_id", derivation.EnvMasterKeyID()),
		)
		return
	}

	rewrapped, err := derivation.WrapKeyEnv(plain)
	if err != nil {
		logger.Fatal("re-wrap DEK with env master key", zap.Error(err))
	}

	newMasterKeyID := derivation.EnvMasterKeyID()
	logger.Info("DEK re-wrapped with env master key",
		zap.String("dek_id", dek.ID),
		zap.String("old_master_key_id", dek.MasterKeyID),
		zap.String("new_master_key_id", newMasterKeyID),
	)

	if *dryRun {
		fmt.Println()
		fmt.Println("=== DRY RUN — no DB write performed ===")
		fmt.Printf("UPDATE data_encryption_keys SET\n")
		fmt.Printf("  encrypted_dek = '%s',\n", rewrapped.Ciphertext)
		fmt.Printf("  iv            = '%s',\n", rewrapped.IV)
		fmt.Printf("  auth_tag      = '%s',\n", rewrapped.AuthTag)
		fmt.Printf("  master_key_id = '%s'\n", newMasterKeyID)
		fmt.Printf("WHERE id = '%s';\n", dek.ID)
		fmt.Println()
		fmt.Println("Run again WITHOUT -dry-run to commit.")
		return
	}

	if err := updateDEK(ctx, pool, schema, dek.ID, rewrapped, newMasterKeyID); err != nil {
		logger.Fatal("update DEK row", zap.Error(err))
	}

	logger.Info("DEK migration committed",
		zap.String("dek_id", dek.ID),
		zap.String("new_master_key_id", newMasterKeyID),
		zap.String("next_step", "deploy the new binary; it will unwrap via env (UnwrapKeyTryAll fallback)"),
	)
}

type dekRow struct {
	ID           string
	EncryptedDEK string
	IV           string
	AuthTag      string
	MasterKeyID  string
}

// loadActiveDEK mirrors KeyManagementService.loadActiveDEK so the tool
// works against either the Go snake_case or TS Prisma camelCase schema.
func loadActiveDEK(ctx context.Context, pool *pgxpool.Pool) (*dekRow, string, error) {
	var dek dekRow
	err := pool.QueryRow(ctx, `
		SELECT id, encrypted_dek, iv, auth_tag, master_key_id
		FROM data_encryption_keys
		WHERE is_active = true
		ORDER BY created_at DESC
		LIMIT 1`).Scan(
		&dek.ID, &dek.EncryptedDEK, &dek.IV, &dek.AuthTag, &dek.MasterKeyID,
	)
	if err == nil {
		return &dek, "go-snake", nil
	}

	err2 := pool.QueryRow(ctx, `
		SELECT id, "encryptedDEK", iv, "authTag", "masterKeyId"
		FROM data_encryption_keys
		WHERE "isActive" = true
		ORDER BY "createdAt" DESC
		LIMIT 1`).Scan(
		&dek.ID, &dek.EncryptedDEK, &dek.IV, &dek.AuthTag, &dek.MasterKeyID,
	)
	if err2 == nil {
		return &dek, "ts-camel", nil
	}

	if isNoRowsOrMissing(err2) {
		return nil, "", nil
	}
	return nil, "", fmt.Errorf("snake-case query: %w; camel-case query: %w", err, err2)
}

func isNoRowsOrMissing(err error) bool {
	if err == nil {
		return false
	}
	if err == pgx.ErrNoRows {
		return true
	}
	var pgErr *pgconn.PgError
	if asErr(err, &pgErr) && (pgErr.Code == "42703" || pgErr.Code == "42P01") {
		return true
	}
	return false
}

// asErr is a tiny errors.As shim — pulled out for readability since the
// tool depends on a single error type.
func asErr(err error, target **pgconn.PgError) bool {
	for e := err; e != nil; {
		if t, ok := e.(*pgconn.PgError); ok {
			*target = t
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := e.(unwrapper); ok {
			e = u.Unwrap()
			continue
		}
		break
	}
	return false
}

func updateDEK(ctx context.Context, pool *pgxpool.Pool, schema, id string, wrapped *encryption.EncryptedData, masterKeyID string) error {
	var query string
	switch schema {
	case "ts-camel":
		query = `
			UPDATE data_encryption_keys
			SET "encryptedDEK" = $1, iv = $2, "authTag" = $3, "masterKeyId" = $4
			WHERE id = $5`
	default:
		query = `
			UPDATE data_encryption_keys
			SET encrypted_dek = $1, iv = $2, auth_tag = $3, master_key_id = $4
			WHERE id = $5`
	}
	tag, err := pool.Exec(ctx, query, wrapped.Ciphertext, wrapped.IV, wrapped.AuthTag, masterKeyID, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("expected 1 row updated, got %d", tag.RowsAffected())
	}
	return nil
}
