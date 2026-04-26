// Admin tool to update encrypted credentials for an existing exchange_connections row.
//
// Usage (inside the enclave container, which has DATABASE_URL + ENCRYPTION_KEY):
//
//	# Preferred: pass secrets via env vars so they do not appear in `ps -ef`.
//	ADMIN_API_KEY="ro:713194:single:1777217715:ea4843..." \
//	ADMIN_API_SECRET="" \
//	docker exec -it enclave_go_test /app/admin-update-creds \
//	    -user-uid="1efeef34-5f45-523d-851e-e0b4b643a3d0" \
//	    -exchange="lighter" \
//	    -label="lighter account"
//
//	# Legacy (SEC-011): -api-key / -api-secret still accepted but require the
//	# explicit -allow-insecure-flags switch. Avoid on shared hosts.
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
		userUID            = flag.String("user-uid", "", "user UID (required)")
		exchange           = flag.String("exchange", "", "exchange name, e.g. lighter (required)")
		label              = flag.String("label", "", "connection label, e.g. 'lighter account' (required)")
		apiKeyFlag         = flag.String("api-key", "", "[deprecated, SEC-011] plaintext API key — prefer ADMIN_API_KEY env var")
		apiSecretFlag      = flag.String("api-secret", "", "[deprecated, SEC-011] plaintext API secret — prefer ADMIN_API_SECRET env var")
		passphraseFlag     = flag.String("passphrase", "", "[deprecated, SEC-011] plaintext passphrase — prefer ADMIN_PASSPHRASE env var")
		allowInsecureFlags = flag.Bool("allow-insecure-flags", false, "opt in to passing secrets via -api-key / -api-secret / -passphrase (visible in `ps`)")
		dryRun             = flag.Bool("dry-run", false, "encrypt + print the UPDATE query but do not execute it")
	)
	flag.Parse()

	// SEC-011: prefer env vars (not visible in `ps -ef`). CLI flags are still
	// accepted but require -allow-insecure-flags and wipe themselves from
	// os.Args so they are not echoed to any subsequent log.
	apiKey, err := resolveSecret("ADMIN_API_KEY", apiKeyFlag, *allowInsecureFlags, "api-key")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	apiSecret, err := resolveSecret("ADMIN_API_SECRET", apiSecretFlag, *allowInsecureFlags, "api-secret")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	passphrase, err := resolveSecret("ADMIN_PASSPHRASE", passphraseFlag, *allowInsecureFlags, "passphrase")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if *userUID == "" || *exchange == "" || *label == "" || apiKey == "" {
		fmt.Fprintln(os.Stderr, "missing required input: -user-uid, -exchange, -label, and ADMIN_API_KEY (or -api-key with -allow-insecure-flags)")
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

	args := updateCredsArgs{
		UserUID:    *userUID,
		Exchange:   *exchange,
		Label:      *label,
		APIKey:     apiKey,
		APISecret:  apiSecret,
		Passphrase: passphrase,
		DryRun:     *dryRun,
	}
	if isTS {
		if err := updateTS(ctx, pool, enc, args); err != nil {
			fatalf("update TS row: %v", err)
		}
	} else {
		if err := updateGo(ctx, pool, enc, args); err != nil {
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

// updateCredsArgs bundles the per-row inputs for the credential UPDATE
// helpers. QUAL-001: extracted to keep updateTS / updateGo under
// SonarQube's S107 (>7 params) threshold.
type updateCredsArgs struct {
	UserUID    string
	Exchange   string
	Label      string
	APIKey     string
	APISecret  string
	Passphrase string
	DryRun     bool
}

func updateTS(ctx context.Context, pool *pgxpool.Pool, enc *encryption.Service, args updateCredsArgs) error {
	userUID, exchange, label := args.UserUID, args.Exchange, args.Label
	apiKey, apiSecret, passphrase, dryRun := args.APIKey, args.APISecret, args.Passphrase, args.DryRun
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

func updateGo(ctx context.Context, pool *pgxpool.Pool, enc *encryption.Service, args updateCredsArgs) error {
	userUID, exchange, label := args.UserUID, args.Exchange, args.Label
	apiKey, apiSecret, passphrase, dryRun := args.APIKey, args.APISecret, args.Passphrase, args.DryRun
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

// resolveSecret returns the secret value for the named input. Priority:
//  1. Env var envName (never appears in `ps -ef`).
//  2. CLI flag *flagPtr, but only when allowInsecure is true — otherwise an
//     error is returned so an accidental `-api-key=xxx` is refused.
//
// Returns (value, nil) when the env var is set (regardless of flag state).
// Returns ("", nil) when both env var and flag are empty — callers decide
// whether empty is acceptable for that field.
// SEC-011.
func resolveSecret(envName string, flagPtr *string, allowInsecure bool, label string) (string, error) {
	if v := os.Getenv(envName); v != "" {
		return v, nil
	}
	if flagPtr == nil || *flagPtr == "" {
		return "", nil
	}
	if !allowInsecure {
		return "", fmt.Errorf("refusing to read %s from -%s CLI flag: re-run with -allow-insecure-flags or set %s env var (SEC-011)", label, label, envName)
	}
	return *flagPtr, nil
}
