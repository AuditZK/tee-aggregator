// Admin tool to update encrypted credentials for an existing exchange_connections row.
//
// Usage (inside the enclave container, which has DATABASE_URL + ENCRYPTION_KEY):
//
//	docker exec -it enclave_go_test /app/admin-update-creds \
//	    -user-uid="1efeef34-5f45-523d-851e-e0b4b643a3d0" \
//	    -exchange="lighter" \
//	    -label="lighter account" \
//	    -api-key="ro:713194:single:1777217715:ea4843..." \
//	    -api-secret=""
//
// Auto-detects TS (Prisma camelCase, hex-packed) vs Go (snake_case, 3-column) schema.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/encryption"
)

func main() {
	var (
		userUID    = flag.String("user-uid", "", "user UID (required)")
		exchange   = flag.String("exchange", "", "exchange name, e.g. lighter (required)")
		label      = flag.String("label", "", "connection label, e.g. 'lighter account' (required)")
		apiKey     = flag.String("api-key", "", "plaintext API key (required)")
		apiSecret  = flag.String("api-secret", "", "plaintext API secret (optional — empty string to clear)")
		passphrase = flag.String("passphrase", "", "plaintext passphrase (optional)")
		dryRun     = flag.Bool("dry-run", false, "encrypt + print the UPDATE query but do not execute it")
	)
	flag.Parse()

	if *userUID == "" || *exchange == "" || *label == "" || *apiKey == "" {
		fmt.Fprintln(os.Stderr, "missing required flag(s): -user-uid, -exchange, -label, -api-key")
		flag.Usage()
		os.Exit(2)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL env var is required")
		os.Exit(2)
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		fatalf("connect db: %v", err)
	}
	defer pool.Close()

	// Bring up the exact same encryption service the enclave uses at runtime
	// (wrapped DEK loaded from data_encryption_keys, unwrapped with master key).
	derivation, err := encryption.NewKeyDerivationService(logger)
	if err != nil {
		fatalf("init key derivation: %v", err)
	}
	kms, err := encryption.NewKeyManagementService(pool, encryption.KeyManagementOptions{
		Derivation:    derivation,
		Logger:        logger,
		AllowAutoInit: false,
	})
	if err != nil {
		fatalf("init key management: %v", err)
	}
	enc, err := kms.GetEncryptionService()
	if err != nil {
		fatalf("build encryption service: %v", err)
	}

	isTS, err := detectTSSchema(ctx, pool)
	if err != nil {
		fatalf("detect schema: %v", err)
	}

	if isTS {
		if err := updateTS(ctx, pool, enc, *userUID, *exchange, *label, *apiKey, *apiSecret, *passphrase, *dryRun); err != nil {
			fatalf("update TS row: %v", err)
		}
	} else {
		if err := updateGo(ctx, pool, enc, *userUID, *exchange, *label, *apiKey, *apiSecret, *passphrase, *dryRun); err != nil {
			fatalf("update Go row: %v", err)
		}
	}

	if *dryRun {
		fmt.Println("dry-run: nothing written")
	} else {
		fmt.Println("credentials updated successfully")
	}
}

func detectTSSchema(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var exists bool
	q := `SELECT EXISTS (
	    SELECT 1 FROM information_schema.columns
	    WHERE table_name = 'exchange_connections' AND column_name = 'encryptedApiKey'
	)`
	if err := pool.QueryRow(ctx, q).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func updateTS(
	ctx context.Context,
	pool *pgxpool.Pool,
	enc *encryption.Service,
	userUID, exchange, label, apiKey, apiSecret, passphrase string,
	dryRun bool,
) error {
	encKey, err := enc.EncryptTSString(apiKey)
	if err != nil {
		return fmt.Errorf("encrypt api_key: %w", err)
	}
	encSecret, err := enc.EncryptTSString(apiSecret)
	if err != nil {
		return fmt.Errorf("encrypt api_secret: %w", err)
	}
	var encPass *string
	if passphrase != "" {
		v, err := enc.EncryptTSString(passphrase)
		if err != nil {
			return fmt.Errorf("encrypt passphrase: %w", err)
		}
		encPass = &v
	}

	query := `
	    UPDATE exchange_connections
	    SET "encryptedApiKey" = $1,
	        "encryptedApiSecret" = $2,
	        "encryptedPassphrase" = $3,
	        "isActive" = true,
	        "updatedAt" = NOW()
	    WHERE "userUid" = $4
	      AND exchange = $5
	      AND TRIM(label) = TRIM($6)
	    RETURNING id, "userUid", exchange, label, "isActive"`

	if dryRun {
		fmt.Println("[dry-run] TS schema UPDATE:")
		fmt.Println(query)
		fmt.Printf("params: userUid=%s exchange=%s label=%s\n", userUID, exchange, label)
		fmt.Printf("encryptedApiKey(hex)=%s\n", encKey)
		return nil
	}

	rows, err := pool.Query(ctx, query, encKey, encSecret, encPass, userUID, exchange, label)
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, uu, ex, lb string
		var active bool
		if err := rows.Scan(&id, &uu, &ex, &lb, &active); err != nil {
			return err
		}
		fmt.Printf("updated: id=%s user=%s exchange=%s label=%q active=%v\n", id, uu, ex, lb, active)
		count++
	}
	if count == 0 {
		return fmt.Errorf("no row matched (user_uid=%s, exchange=%s, label=%s)", userUID, exchange, label)
	}
	return nil
}

func updateGo(
	ctx context.Context,
	pool *pgxpool.Pool,
	enc *encryption.Service,
	userUID, exchange, label, apiKey, apiSecret, passphrase string,
	dryRun bool,
) error {
	encKey, err := enc.EncryptString(apiKey)
	if err != nil {
		return fmt.Errorf("encrypt api_key: %w", err)
	}
	encSecret, err := enc.EncryptString(apiSecret)
	if err != nil {
		return fmt.Errorf("encrypt api_secret: %w", err)
	}
	var (
		encPassCT, encPassIV, encPassTag string
		hasPass                          bool
	)
	if passphrase != "" {
		v, err := enc.EncryptString(passphrase)
		if err != nil {
			return fmt.Errorf("encrypt passphrase: %w", err)
		}
		encPassCT, encPassIV, encPassTag = v.Ciphertext, v.IV, v.AuthTag
		hasPass = true
	}

	query := `
	    UPDATE exchange_connections
	    SET encrypted_api_key = $1, api_key_iv = $2, api_key_auth_tag = $3,
	        encrypted_api_secret = $4, api_secret_iv = $5, api_secret_auth_tag = $6,
	        encrypted_passphrase = CASE WHEN $7 THEN $8 ELSE encrypted_passphrase END,
	        passphrase_iv        = CASE WHEN $7 THEN $9 ELSE passphrase_iv END,
	        passphrase_auth_tag  = CASE WHEN $7 THEN $10 ELSE passphrase_auth_tag END,
	        is_active = true,
	        updated_at = NOW()
	    WHERE user_uid = $11
	      AND exchange = $12
	      AND TRIM(label) = TRIM($13)
	    RETURNING id, user_uid, exchange, label, is_active`

	if dryRun {
		fmt.Println("[dry-run] Go schema UPDATE:")
		fmt.Println(query)
		fmt.Printf("params: user_uid=%s exchange=%s label=%s\n", userUID, exchange, label)
		return nil
	}

	rows, err := pool.Query(ctx, query,
		encKey.Ciphertext, encKey.IV, encKey.AuthTag,
		encSecret.Ciphertext, encSecret.IV, encSecret.AuthTag,
		hasPass, encPassCT, encPassIV, encPassTag,
		userUID, exchange, label,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id, uu, ex, lb string
		var active bool
		if err := rows.Scan(&id, &uu, &ex, &lb, &active); err != nil {
			return err
		}
		fmt.Printf("updated: id=%s user=%s exchange=%s label=%q active=%v\n", id, uu, ex, lb, active)
		count++
	}
	if count == 0 {
		return fmt.Errorf("no row matched (user_uid=%s, exchange=%s, label=%s)", userUID, exchange, label)
	}
	return nil
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n", a...)
	os.Exit(1)
}
